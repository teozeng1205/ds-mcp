"""Simple table helpers for DS-MCP.

Every table gets five core tools for free:
- describe_table() – introduction, key/partition columns, default limits
- get_table_partitions() – partition metadata via svv_table_info
- get_table_schema() – column metadata from svv_columns
- read_table_head(limit) – quick peek at the latest rows
- query_table(sql, max_rows) – SELECT/WITH only, auto LIMIT and macro expansion

Custom SQL helpers stay declarative using SQLToolSpec/ParameterSpec.
"""

from __future__ import annotations

from collections.abc import Sequence as SequenceABC
from dataclasses import dataclass, field
import json
import logging
import re
from typing import Any, Callable, Dict, Mapping, Optional, Sequence

from ds_mcp.core.connectors import get_connector
from ds_mcp.core.registry import TableConfig, TableRegistry

log = logging.getLogger(__name__)

MacroValue = str | Callable[[Optional[str]], str]

_FORBIDDEN = {"DELETE", "UPDATE", "INSERT", "DROP", "TRUNCATE", "ALTER", "COPY", "UNLOAD"}
_PARAM_PATTERN = re.compile(r"(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)")
_MACRO_PATTERN = re.compile(r"\{\{([A-Z0-9_]+)(?::([A-Za-z_][A-Za-z0-9_]*))?\}\}")


@dataclass(slots=True)
class ParameterSpec:
    name: str
    description: str = ""
    default: Any | None = None
    coerce: Callable[[Any], Any] | None = None
    kind: str = "scalar"
    choices: Sequence[Any] | None = None
    min_value: float | None = None
    max_value: float | None = None
    include_in_sql: bool = True
    as_literal: bool = False

    def apply(self, value: Any | None) -> Any:
        if value is None:
            value = self.default
        if value is None:
            raise ValueError(f"Missing required parameter: {self.name}")
        if self.kind == "list":
            if isinstance(value, str):
                value = [part.strip() for part in value.split(",") if part.strip()]
            elif not isinstance(value, SequenceABC):
                value = [value]
        if self.coerce:
            if self.kind == "list":
                value = [self.coerce(item) for item in value]
            else:
                value = self.coerce(value)
        self._validate(value)
        return value

    def _validate(self, value: Any) -> None:
        values = value if self.kind == "list" else [value]
        for item in values:
            if isinstance(item, (int, float)):
                if self.min_value is not None and item < self.min_value:
                    raise ValueError(f"{self.name} must be >= {self.min_value}")
                if self.max_value is not None and item > self.max_value:
                    raise ValueError(f"{self.name} must be <= {self.max_value}")
            if self.choices is not None and str(item) not in {str(choice) for choice in self.choices}:
                raise ValueError(f"{self.name} must be one of {', '.join(map(str, self.choices))}")


@dataclass(slots=True)
class SQLToolSpec:
    name: str
    sql: str
    doc: str
    params: Sequence[ParameterSpec] = field(default_factory=tuple)
    enforce_limit: bool = True
    default_limit: int = 200
    prepare: Callable[[Dict[str, Any]], str] | None = None


@dataclass
class Table:
    slug: str
    schema_name: str
    table_name: str
    description: str
    database_name: str | None = None
    key_columns: Sequence[str] = field(default_factory=tuple)
    partition_columns: Sequence[str] = field(default_factory=tuple)
    connector_type: str = "analytics"
    default_limit: int = 200
    head_limit: int = 50
    head_order_by: Sequence[str] = field(default_factory=tuple)
    macros: Mapping[str, MacroValue] = field(default_factory=dict)
    custom_tools: Sequence[SQLToolSpec] = field(default_factory=tuple)
    query_aliases: Sequence[str] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not self.slug or not self.schema_name or not self.table_name:
            raise ValueError("slug, schema_name, and table_name are required")
        builtin_macros = {
            "TABLE": self.full_name,
            "FULL_TABLE": self.full_name,
            "SCHEMA": self.schema_name,
            "TABLE_ONLY": self.table_name,
            "TODAY": "CAST(TO_CHAR(CURRENT_DATE, 'YYYYMMDD') AS INT)",
        }
        merged = dict(builtin_macros)
        merged.update(self.macros)
        self.macros = merged

    @property
    def full_name(self) -> str:
        if self.database_name:
            return f"{self.database_name}.{self.schema_name}.{self.table_name}"
        return f"{self.schema_name}.{self.table_name}"

    def register(self, registry: TableRegistry) -> None:
        tools = {
            "describe_table": self.make_describe_tool(),
            "get_table_schema": self.make_schema_tool(),
            "get_table_partitions": self.make_partitions_tool(),
            "read_table_head": self.make_head_tool(),
            "query_table": self.make_query_tool(),
        }
        for alias in self.query_aliases:
            tools[alias] = self._make_alias(tools["query_table"], alias)
        for spec in self.custom_tools:
            tools[spec.name] = self.make_sql_tool(spec)
        registry.register_table(
            TableConfig(
                name=self.full_name,
                display_name=self.table_name.replace("_", " ").title(),
                description=self.description,
                schema_name=self.schema_name,
                table_name=self.table_name,
                database_name=self.database_name,
                connector_type=self.connector_type,
                tools=list(tools.values()),
                metadata=self._metadata(),
            )
        )

    def _metadata(self) -> Dict[str, Any]:
        return {
            "introduction": self.description,
            "key_columns": list(self.key_columns),
            "partition_columns": list(self.partition_columns),
            "default_limit": self.default_limit,
            "head_limit": self.head_limit,
        }

    # ------------------------------------------------------------------ tools
    def make_describe_tool(self) -> Callable[[], str]:
        payload = self._metadata()
        payload["table"] = self.full_name

        def describe() -> str:
            return json.dumps(payload, indent=2)

        describe.__name__ = "describe_table"
        describe.__doc__ = f"Describe {self.full_name} with key columns and partitions."
        return describe

    def make_schema_tool(self) -> Callable[[], str]:
        schema = self.schema_name
        table = self.table_name
        database = self.database_name

        def schema_tool() -> str:
            query = [
                "SELECT column_name, data_type, character_maximum_length, is_nullable",
                "FROM svv_columns",
                "WHERE table_schema = %s",
                "  AND table_name = %s",
            ]
            params: list[Any] = [schema, table]
            if database:
                query.append("  AND table_catalog = %s")
                params.append(database)
            query.append("ORDER BY ordinal_position")
            result = self._run_query("\n".join(query), params)
            return json.dumps(result, indent=2)

        schema_tool.__name__ = "get_table_schema"
        schema_tool.__doc__ = f"Return Redshift schema for {self.full_name}."
        return schema_tool

    def make_partitions_tool(self) -> Callable[[], str]:
        schema = self.schema_name
        table = self.table_name
        database = self.database_name

        def get_partitions() -> str:
            query = [
                "SELECT DISTINCT partitionkey AS column_name",
                "FROM svv_table_info",
                "WHERE schemaname = %s",
                "  AND tablename = %s",
            ]
            params: list[Any] = [schema, table]
            if database:
                query.append("  AND table_catalog = %s")
                params.append(database)
            query.append("ORDER BY column_name")
            result = self._run_query("\n".join(query), params)
            return json.dumps(result, indent=2)

        get_partitions.__name__ = "get_table_partitions"
        get_partitions.__doc__ = f"Return partition columns (if available) for {self.full_name}."
        return get_partitions

    def make_head_tool(self) -> Callable[[int], str]:
        order_clause = ""
        if self.head_order_by:
            order_clause = " ORDER BY " + ", ".join(self.head_order_by)

        def head(limit: int = self.head_limit) -> str:
            limit_int = max(1, int(limit))
            sql = f"SELECT * FROM {self.full_name}{order_clause} LIMIT {limit_int}"
            return json.dumps(self._run_query(sql), indent=2)

        head.__name__ = "read_table_head"
        head.__doc__ = f"Return the first few rows from {self.full_name}."
        return head

    def make_query_tool(self) -> Callable[[str, Optional[int]], str]:
        default_limit = self.default_limit

        def query(sql: str, max_rows: Optional[int] = None) -> str:
            limit = default_limit if max_rows is None else max(1, int(max_rows))
            sql_text = self._ensure_safe_select(sql, limit)
            if max_rows is not None:
                sql_text = re.sub(r"(?i)limit\s+\d+", f"LIMIT {limit}", sql_text)
            return json.dumps(self._run_query(sql_text), indent=2)

        query.__name__ = "query_table"
        query.__doc__ = f"Execute a read-only query against {self.full_name}. Default LIMIT {default_limit}."
        return query

    def make_sql_tool(self, spec: SQLToolSpec) -> Callable[..., str]:
        params = list(spec.params)

        def tool(**kwargs):
            values: Dict[str, Any] = {}
            for param in params:
                values[param.name] = param.apply(kwargs.get(param.name))
            sql_template = spec.sql
            if spec.prepare:
                sql_template = spec.prepare(values)
            sql_rendered, sql_params = self._render_sql(sql_template, values, spec.params)
            sql_rendered = self._expand_macros(sql_rendered)
            if spec.enforce_limit and "limit" not in sql_rendered.lower():
                sql_rendered = f"{sql_rendered.rstrip(';')} LIMIT {spec.default_limit}"
            return json.dumps(self._run_query(sql_rendered, sql_params), indent=2)

        tool.__name__ = spec.name
        tool.__doc__ = spec.doc
        return tool

    # ----------------------------------------------------------------- helpers
    def _expand_macros(self, sql: str) -> str:
        def repl(match: re.Match[str]) -> str:
            name = match.group(1)
            alias = match.group(2)
            value = self.macros.get(name)
            if value is None:
                return match.group(0)
            if callable(value):
                return value(alias)
            if alias:
                try:
                    return value.format(alias=alias)
                except Exception:
                    pass
            return value

        return _MACRO_PATTERN.sub(repl, sql)

    def _ensure_safe_select(self, sql: str, limit: int) -> str:
        stripped = sql.strip()
        if not stripped:
            raise ValueError("SQL cannot be empty")
        upper = stripped.upper()
        if not (upper.startswith("SELECT") or upper.startswith("WITH")):
            raise ValueError("Only SELECT or WITH statements are allowed")
        for keyword in _FORBIDDEN:
            if keyword in upper:
                raise ValueError(f"Forbidden keyword detected: {keyword}")
        if "{{" in stripped:
            stripped = self._expand_macros(stripped)
        if "limit" not in stripped.lower():
            stripped = f"{stripped.rstrip(';')} LIMIT {limit}"
        return stripped

    def _run_query(self, sql: str, params: Sequence[Any] | None = None) -> Dict[str, Any]:
        connector = get_connector(self.connector_type)
        conn = connector.get_connection()
        with conn.cursor() as cursor:
            log.info("SQL[%s]: %s | params=%s", self.slug, sql, params)
            cursor.execute(sql, params)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
        data = [self._row_to_dict(columns, row) for row in rows]
        return {
            "columns": columns,
            "rows": data,
            "row_count": len(data),
            "sql": sql,
            "truncated": False,
        }

    @staticmethod
    def _row_to_dict(columns: Sequence[str], row: Sequence[Any]) -> Dict[str, Any]:
        return {col: value for col, value in zip(columns, row)}

    def _render_sql(
        self,
        sql: str,
        values: Mapping[str, Any],
        params: Sequence[ParameterSpec],
    ) -> tuple[str, Sequence[Any]]:
        collected: list[Any] = []
        param_map = {spec.name: spec for spec in params if spec.include_in_sql}

        def replacer(match: re.Match[str]) -> str:
            name = match.group(1)
            spec = param_map.get(name)
            if spec is None:
                return match.group(0)
            value = values[name]
            if spec.as_literal:
                return str(value)
            collected.append(value)
            return "%s"

        rendered = _PARAM_PATTERN.sub(replacer, sql)
        return rendered, collected

    @staticmethod
    def _make_alias(func: Callable[..., Any], name: str) -> Callable[..., Any]:
        def alias(*args, **kwargs):
            return func(*args, **kwargs)

        alias.__name__ = name
        alias.__doc__ = func.__doc__
        return alias

def build_table(**kwargs: Any) -> Table:
    return Table(**kwargs)


def export_tools(table: Table, namespace: Dict[str, Any]) -> None:
    registry = TableRegistry()
    table.register(registry)
    config = registry.get_all_tables()[0]
    for tool in config.tools:
        namespace[tool.__name__] = tool
