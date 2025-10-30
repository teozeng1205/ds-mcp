"""
Shared helpers for defining DS-MCP tables.

This module provides a small abstraction (`SimpleTableDefinition`) that
wraps the common patterns for exposing a database table as a set of MCP
tools. It keeps query execution, macro expansion, and safety checks in one
place so individual table modules can stay concise.
"""

from __future__ import annotations

import functools
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Mapping, Optional, Sequence

from ds_mcp.core.connectors import get_connector
from ds_mcp.core.registry import TableConfig, TableRegistry

log = logging.getLogger(__name__)

MacroValue = str | Callable[[Optional[str]], str]

_MACRO_PATTERN = re.compile(r"\{\{([A-Z0-9_]+)(?::([A-Za-z_][A-Za-z0-9_]*))?\}\}")
_LIMIT_PATTERN = re.compile(r"\blimit\b\s+(\d+)", flags=re.IGNORECASE)
_FORBIDDEN_KEYWORDS = {
    "DELETE",
    "UPDATE",
    "INSERT",
    "DROP",
    "TRUNCATE",
    "ALTER",
    "CREATE",
    "COPY",
    "UNLOAD",
    "GRANT",
    "REVOKE",
}
_PARAM_PATTERN = re.compile(r":([a-zA-Z_][a-zA-Z0-9_]*)")
_MISSING = object()


@dataclass(slots=True)
class ParameterSpec:
    """
    Declarative description of a tool parameter.

    Attributes:
        name: Parameter name.
        description: Human-readable description.
        default: Default value (if any).
        coerce: Optional callable to coerce incoming values.
        transform: Optional callable applied after coercion.
        min_value/max_value: Numeric bounds when applicable.
        choices: Optional allowed values (for scalar parameters).
        kind: Either ``"scalar"`` or ``"list"`` (comma-separated strings accepted).
        include_in_sql: Whether the parameter should be substituted as ``:name``.
        as_literal: If True the value is embedded directly into the SQL instead of
            using a database parameter (use for LIMIT / column names, etc.).
    """

    name: str
    description: str = ""
    default: Any = _MISSING
    coerce: Optional[Callable[[Any], Any]] = None
    transform: Optional[Callable[[Any], Any]] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    choices: Optional[Sequence[str]] = None
    kind: str = "scalar"  # "scalar" | "list"
    include_in_sql: bool = True
    as_literal: bool = False

    def has_default(self) -> bool:
        return self.default is not _MISSING

    def apply(self, value: Any) -> Any:
        if value is _MISSING:
            raise ValueError(f"Missing required value for parameter '{self.name}'")

        if self.kind == "list":
            if isinstance(value, str):
                value = [v for v in (part.strip() for part in value.split(",")) if v]
            elif isinstance(value, Sequence):
                value = list(value)
            else:
                raise ValueError(f"Parameter '{self.name}' expects a list")

        if self.coerce:
            if self.kind == "list":
                value = [self.coerce(item) for item in value]
            else:
                value = self.coerce(value)

        if self.transform:
            value = self.transform(value)

        if self.kind == "scalar":
            self._validate_scalar(value)
        else:
            for item in value:
                self._validate_scalar(item)

        return value

    def _validate_scalar(self, value: Any) -> None:
        if self.min_value is not None and isinstance(value, (int, float)):
            if value < self.min_value:
                raise ValueError(f"{self.name} must be >= {self.min_value}")
        if self.max_value is not None and isinstance(value, (int, float)):
            if value > self.max_value:
                raise ValueError(f"{self.name} must be <= {self.max_value}")
        if self.choices is not None:
            if str(value) not in set(map(str, self.choices)):
                raise ValueError(f"{self.name} must be one of {', '.join(map(str, self.choices))}")


@dataclass(slots=True)
class SQLToolSpec:
    """
    Declarative SQL tool definition.

    Attributes:
        name: Tool function name.
        sql: SQL template using ``:param`` placeholders and table macros.
        doc: Docstring for the generated tool.
        params: Parameter specifications.
        enforce_limit: Whether to enforce automatic LIMIT injection.
        max_rows: Hard cap on rows returned (overrides parameter-provided values).
        max_rows_param: Name of parameter whose value will drive result truncation.
        prepare: Optional callable that receives parameter values (after coercion)
                 and returns a customised SQL string (used before macro expansion).
    """

    name: str
    sql: str
    doc: str
    params: Sequence[ParameterSpec] = ()
    enforce_limit: bool = True
    max_rows: Optional[int] = None
    max_rows_param: Optional[str] = None
    prepare: Optional[Callable[[Dict[str, Any]], str]] = None


@dataclass(slots=True)
class SimpleTableDefinition:
    """
    Declarative description of a table exposed through the MCP server.

    Parameters capture the metadata needed for the registry plus a couple of
    user-facing configuration options (tool names, default row limits, etc.).
    """

    slug: str
    schema_name: str
    table_name: str
    display_name: str
    description: str
    database_name: Optional[str] = None
    connector_type: str = "analytics"
    query_tool_name: str = "query_table"
    schema_tool_name: str = "get_table_schema"
    query_aliases: Sequence[str] = field(default_factory=tuple)
    schema_aliases: Sequence[str] = field(default_factory=tuple)
    default_limit: int = 200
    max_limit: int = 1000
    macros: Mapping[str, MacroValue] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    sql_tools: Sequence[SQLToolSpec] = field(default_factory=tuple)

    def full_table_name(self) -> str:
        if self.database_name:
            return f"{self.database_name}.{self.schema_name}.{self.table_name}"
        return f"{self.schema_name}.{self.table_name}"

    def build_config(self) -> TableConfig:
        executor = SimpleTableExecutor(self)

        tools: list[Callable[..., str]] = []

        query_tool = executor.make_query_tool("query_table")
        tools.append(query_tool)
        for alias in self._normalise_aliases(
            preferred=self.query_tool_name,
            aliases=self.query_aliases,
            default="query_table",
        ):
            tools.append(executor.make_alias(query_tool, alias))

        schema_tool = executor.make_schema_tool("get_table_schema")
        tools.append(schema_tool)
        for alias in self._normalise_aliases(
            preferred=self.schema_tool_name,
            aliases=self.schema_aliases,
            default="get_table_schema",
        ):
            tools.append(executor.make_alias(schema_tool, alias))

        for sql_tool in self.sql_tools:
            tools.append(executor.make_sql_tool(sql_tool))

        metadata = {
            "slug": self.slug,
            "default_limit": self.default_limit,
        }
        if self.macros:
            metadata["macros"] = sorted(self.macros.keys())
        metadata.update(self.metadata)

        return TableConfig(
            name=self.full_table_name(),
            display_name=self.display_name,
            description=self.description,
            schema_name=self.schema_name,
            table_name=self.table_name,
            database_name=self.database_name,
            connector_type=self.connector_type,
            tools=tools,
            metadata=metadata,
        )

    def register(self, registry: TableRegistry) -> None:
        """Register this table definition with the given registry."""
        registry.register_table(self.build_config())

    @staticmethod
    def _normalise_aliases(
        *,
        preferred: str,
        aliases: Sequence[str],
        default: str,
    ) -> Sequence[str]:
        names: list[str] = []
        if preferred and preferred != default:
            names.append(preferred)
        for alias in aliases:
            if alias and alias not in names and alias != default:
                names.append(alias)
        return tuple(names)


@dataclass(slots=True)
class TableBundle:
    """
    Convenience wrapper bundling a table definition, registry config, and tools.

    Table modules can return a ``TableBundle`` to expose helpers like
    ``register`` or ``get_tool`` without re-implementing boilerplate. This keeps
    the public API tiny while still enabling direct access to the underlying
    :class:`SimpleTableDefinition`.
    """

    definition: SimpleTableDefinition
    config: TableConfig
    tools: Dict[str, Callable[..., str]]

    @property
    def slug(self) -> str:
        return self.definition.slug

    @property
    def display_name(self) -> str:
        return self.definition.display_name

    def register(self, registry: TableRegistry) -> None:
        self.definition.register(registry)

    def get_tool(self, name: str) -> Callable[..., str]:
        try:
            return self.tools[name]
        except KeyError as exc:
            raise KeyError(f"Tool '{name}' not found for table '{self.definition.slug}'") from exc

    def iter_tools(self) -> Sequence[Callable[..., str]]:
        return list(self.tools.values())

    def create_executor(self) -> "SimpleTableExecutor":
        return SimpleTableExecutor(self.definition)


def build_table(
    *,
    slug: str,
    schema_name: str,
    table_name: str,
    display_name: str,
    description: str,
    database_name: Optional[str] = None,
    connector_type: str = "analytics",
    query_tool_name: str = "query_table",
    schema_tool_name: str = "get_table_schema",
    query_aliases: Sequence[str] = (),
    schema_aliases: Sequence[str] = (),
    default_limit: int = 200,
    max_limit: int = 1000,
    macros: Mapping[str, MacroValue] | None = None,
    metadata: Dict[str, Any] | None = None,
    sql_tools: Sequence[SQLToolSpec] = (),
) -> TableBundle:
    """
    Build a :class:`TableBundle` from declarative pieces.

    Table modules should call this helper and export the returned bundle. The
    bundle exposes helpers for registration and direct access to tool callables,
    keeping new table additions down to a handful of lines.
    """

    definition = SimpleTableDefinition(
        slug=slug,
        schema_name=schema_name,
        table_name=table_name,
        database_name=database_name,
        display_name=display_name,
        description=description,
        connector_type=connector_type,
        query_tool_name=query_tool_name,
        schema_tool_name=schema_tool_name,
        query_aliases=tuple(query_aliases),
        schema_aliases=tuple(schema_aliases),
        default_limit=default_limit,
        max_limit=max_limit,
        macros=dict(macros or {}),
        metadata=dict(metadata or {}),
        sql_tools=tuple(sql_tools),
    )

    config = definition.build_config()
    tools = {tool.__name__: tool for tool in config.tools}
    return TableBundle(definition=definition, config=config, tools=tools)


def export_tools(bundle: TableBundle, namespace: Dict[str, Any]) -> None:
    """
    Export all tool callables from *bundle* into *namespace*.

    Typical usage inside a table package::

        TABLE = build_table(...)
        export_tools(TABLE, globals())

    This keeps the public module surface identical to the previous
    ``BaseTableModule``-driven approach while staying explicit.
    """

    namespace.update(bundle.tools)

class SimpleTableExecutor:
    """
    Helper that handles macro expansion, SQL safety checks, and execution.
    """

    def __init__(self, definition: SimpleTableDefinition):
        self.definition = definition
        self.log = logging.getLogger(f"ds_mcp.tables.{definition.slug}")
        self.macros = {
            "TABLE": self.definition.full_table_name(),
            "FULL_TABLE": self.definition.full_table_name(),
            "SCHEMA": self.definition.schema_name,
            "TABLE_ONLY": self.definition.table_name,
            "TODAY": "CAST(TO_CHAR(CURRENT_DATE, 'YYYYMMDD') AS INT)",
        }
        self.macros.update(definition.macros)

    # ------------------------------------------------------------------ macros

    def expand_macros(self, sql: str) -> str:
        """Replace ``{{MACRO}}`` placeholders using the configured mapping."""

        def replacer(match: re.Match[str]) -> str:
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
                except (KeyError, IndexError, ValueError):
                    # Fall back to the raw string if formatting fails
                    pass
            return value

        return _MACRO_PATTERN.sub(replacer, sql)

    # ---------------------------------------------------------------- execution

    def run_query(
        self,
        sql: str,
        *,
        params: Optional[Sequence[Any]] = None,
        max_rows: Optional[int] = None,
        enforce_limit: bool = True,
    ) -> str:
        """Execute the given SQL and return a JSON payload."""
        sql_with_macros = self.expand_macros(sql)
        result = self.fetch(
            sql_with_macros,
            params=params,
            max_rows=max_rows,
            enforce_limit=enforce_limit,
        )
        return json.dumps(result, indent=2)

    def fetch(
        self,
        sql: str,
        *,
        params: Optional[Sequence[Any]] = None,
        max_rows: Optional[int] = None,
        enforce_limit: bool = True,
    ) -> Dict[str, Any]:
        """
        Execute the query and return a Python dict with columns/rows metadata.
        """
        try:
            prepared_sql, limit = self._prepare_query(sql, max_rows, enforce_limit)
        except ValueError as exc:
            return {"error": str(exc)}

        try:
            payload = self._run_select(
                prepared_sql,
                params=None if params is None else tuple(params),
                limit=limit,
            )
        except Exception as exc:  # pragma: no cover - defensive
            self.log.error("Query execution failed", exc_info=exc)
            return {"error": str(exc)}
        return payload

    def _prepare_query(
        self,
        sql: str,
        max_rows: Optional[int],
        enforce_limit: bool,
    ) -> tuple[str, Optional[int]]:
        stripped = sql.strip()
        if not stripped:
            raise ValueError("SQL query cannot be empty")

        expanded = self.expand_macros(stripped)
        upper = expanded.lstrip().upper()
        if not (upper.startswith("SELECT") or upper.startswith("WITH")):
            raise ValueError("Only SELECT or WITH queries are allowed")

        for keyword in _FORBIDDEN_KEYWORDS:
            if keyword in upper:
                raise ValueError(f"Forbidden keyword detected: {keyword}")

        if not enforce_limit:
            return expanded, max_rows

        limit = max_rows or self.definition.default_limit
        limit = max(1, min(limit, self.definition.max_limit))
        match = _LIMIT_PATTERN.search(expanded)
        if match:
            try:
                existing = int(match.group(1))
            except ValueError:
                existing = limit
            adjusted = min(existing, limit)
            if adjusted != existing:
                expanded = _LIMIT_PATTERN.sub(f"LIMIT {adjusted}", expanded, count=1)
            limit = adjusted
        else:
            expanded = expanded.rstrip(";") + f" LIMIT {limit}"

        return expanded, limit

    def _run_select(
        self,
        sql: str,
        *,
        params: Optional[Sequence[Any]],
        limit: Optional[int],
    ) -> Dict[str, Any]:
        connector = get_connector(self.definition.connector_type)
        conn = connector.get_connection()

        try:
            if getattr(conn, "autocommit", None) is False:
                conn.autocommit = True
        except Exception:
            pass

        attempts = 0
        while attempts < 2:
            try:
                with conn.cursor() as cursor:
                    try:
                        cursor.execute("ROLLBACK;")
                    except Exception:
                        pass

                    self.log.info("SQL[%s]: %s | params=%s", self.definition.slug, sql.strip(), params)
                    cursor.execute(sql, params)

                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchmany(limit) if limit else cursor.fetchall()
                    data = [self._coerce_row(columns, row) for row in rows]

                    truncated = bool(limit) and len(rows) == limit
                    return {
                        "columns": columns,
                        "rows": data,
                        "row_count": len(data),
                        "truncated": truncated,
                        "sql": sql,
                    }
            except Exception as exc:  # pragma: no cover - defensive
                msg = str(exc)
                if attempts == 0 and ("25P02" in msg or "aborted" in msg):
                    attempts += 1
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    continue
                raise
        raise RuntimeError("Failed to execute query after retry")  # pragma: no cover

    @staticmethod
    def _coerce_row(columns: Sequence[str], values: Sequence[Any]) -> Dict[str, Any]:
        row: Dict[str, Any] = {}
        for idx, column in enumerate(columns):
            value = values[idx]
            if isinstance(value, bytes):
                try:
                    value = value.decode("utf-8", "ignore")
                except Exception:  # pragma: no cover - extremely rare
                    value = value.hex()
            elif hasattr(value, "isoformat"):
                value = value.isoformat()
            elif value is not None and not isinstance(value, (str, int, float, bool)):
                value = str(value)
            row[column] = value
        return row

    # ------------------------------------------------------------------ tools

    def make_alias(self, tool: Callable[..., str], name: str) -> Callable[..., str]:
        """Return a thin alias around *tool* with a different exported name."""

        @functools.wraps(tool)
        def alias(*args, **kwargs):
            return tool(*args, **kwargs)

        alias.__name__ = name
        alias.__doc__ = tool.__doc__
        return alias

    def make_query_tool(self, name: str) -> Callable[[str, Optional[int]], str]:
        macros_list = ", ".join(sorted(self.macros.keys()))
        default_limit = self.definition.default_limit
        max_limit = self.definition.max_limit

        def query_tool(sql_query: str, max_rows: Optional[int] = None) -> str:
            """
            Execute a read-only query against the configured table.

            The query must start with ``SELECT`` or ``WITH``. Mutating statements
            (``DELETE``, ``UPDATE``, etc.) are rejected automatically. A ``LIMIT``
            clause is applied when missing to keep responses bounded.
            """

            return self.run_query(sql_query, max_rows=max_rows)

        query_tool.__name__ = name
        query_tool.__doc__ = (
            query_tool.__doc__.strip()
            + f"\n\nMacros available: {macros_list or '(none)'}."
            + f" Default limit: {default_limit} rows (cap {max_limit})."
        )
        return query_tool

    def make_schema_tool(self, name: str) -> Callable[[], str]:
        schema = self.definition.schema_name
        table = self.definition.table_name
        database = self.definition.database_name

        query = [
            "SELECT",
            "  column_name,",
            "  data_type,",
            "  character_maximum_length,",
            "  is_nullable",
            "FROM svv_columns",
            "WHERE table_schema = %s",
            "  AND table_name = %s",
        ]
        params: list[Any] = [schema, table]
        if database:
            query.append("  AND table_catalog = %s")
            params.append(database)
        query.append("ORDER BY ordinal_position")
        schema_sql = "\n".join(query)

        def get_schema() -> str:
            """Return column metadata from ``svv_columns``."""

            result = self.fetch(schema_sql, params=params, enforce_limit=False)
            return json.dumps(result, indent=2)

        get_schema.__name__ = name
        return get_schema

    def make_sql_tool(self, spec: SQLToolSpec) -> Callable[..., str]:
        """Create a tool function from a SQLToolSpec."""
        param_specs = list(spec.params)
        spec_map = {p.name: p for p in param_specs if p.include_in_sql}

        def tool(*args, **kwargs):
            if len(args) > len(param_specs):
                raise TypeError(f"{spec.name}() takes at most {len(param_specs)} positional arguments ({len(args)} given)")

            values: Dict[str, Any] = {}

            for positional, param in zip(args, param_specs):
                values[param.name] = param.apply(positional)

            for param in param_specs[len(args):]:
                if param.name in kwargs:
                    values[param.name] = param.apply(kwargs.pop(param.name))
                elif param.has_default():
                    default = param.default
                    if param.kind == "list" and isinstance(default, (tuple, list)):
                        default = list(default)
                    values[param.name] = param.apply(default)
                else:
                    raise TypeError(f"Missing required argument: {param.name}")

            if kwargs:
                raise TypeError(f"Unexpected argument(s): {', '.join(kwargs.keys())}")

            sql_template = spec.sql
            if spec.prepare:
                sql_template = spec.prepare(values)

            rendered_sql = self.expand_macros(sql_template)
            rendered_sql, sql_params = self._render_sql_with_params(rendered_sql, values, spec_map)

            max_rows = spec.max_rows
            if spec.max_rows_param and spec.max_rows_param in values:
                max_rows = int(values[spec.max_rows_param])
            result = self.fetch(
                rendered_sql,
                params=sql_params,
                max_rows=max_rows,
                enforce_limit=spec.enforce_limit,
            )
            return json.dumps(result, indent=2)

        tool.__name__ = spec.name
        tool.__doc__ = spec.doc.strip()
        return tool

    def _render_sql_with_params(
        self,
        sql_template: str,
        values: Dict[str, Any],
        spec_map: Dict[str, ParameterSpec],
    ) -> tuple[str, Sequence[Any]]:
        params: list[Any] = []

        def replacer(match: re.Match[str]) -> str:
            name = match.group(1)
            if name not in spec_map:
                raise ValueError(f"Unknown SQL parameter :{name}")
            spec = spec_map[name]
            value = values[name]
            if spec.as_literal:
                return str(value)
            params.append(value)
            return "%s"

        sql = _PARAM_PATTERN.sub(replacer, sql_template)
        return sql, params

