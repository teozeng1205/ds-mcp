# Adding Tables the Easy Way

The refactored DS-MCP keeps table onboarding intentionally small: describe the
table once, list the tools you want, and export everything with a helper. No
framework surgery, no manual registry edits.

## 1. Create a table package

```
src/ds_mcp/tables/my_table/
├── __init__.py   # builds & exports tools
└── tools.py      # optional helpers / SQL specs
```

`__init__.py` can be the only file if the table is tiny. Use `tools.py` when you
want to keep SQL specs or helper functions separate.

## 2. Describe SQL tools declaratively

`ds_mcp.tables.base` still provides `ParameterSpec` and `SQLToolSpec`. They let
you describe parameters, macros, and limits without writing boilerplate. For
example:

```python
# src/ds_mcp/tables/my_table/tools.py
from ds_mcp.tables.base import ParameterSpec, SQLToolSpec

MACROS = {
    "TABLE": "analytics.my_table",
}

SQL_TOOL_SPECS = (
    SQLToolSpec(
        name="latest_rows",
        doc="Return the most recent records (defaults to 50).",
        sql="SELECT * FROM {{TABLE}} ORDER BY snapshot_ts DESC LIMIT :limit",
        params=(
            ParameterSpec(
                name="limit",
                default=50,
                coerce=int,
                min_value=1,
                max_value=500,
                as_literal=True,
            ),
        ),
        enforce_limit=False,
        max_rows_param="limit",
    ),
)
```

You can skip this step entirely if the default `query_*` and `get_table_schema`
tools are enough.

## 3. Build the table in `__init__.py`

```python
# src/ds_mcp/tables/my_table/__init__.py
from ds_mcp.core.registry import TableRegistry
from ds_mcp.tables.base import build_table, export_tools

from . import tools

TABLE = build_table(
    slug="my_table",
    schema_name="analytics",
    table_name="my_table",
    display_name="My Table",
    description="Short blurb for MCP clients.",
    query_tool_name="query_my_table",  # adds an alias; `query_table` is always present
    default_limit=200,
    macros=tools.MACROS,
    sql_tools=tools.SQL_TOOL_SPECS,
)

export_tools(TABLE, globals())          # adds query_table(), query_my_table(), latest_rows(), …
TABLE_NAME = TABLE.definition.full_table_name()


def register_table(registry: TableRegistry) -> None:
    TABLE.register(registry)


__all__ = ["TABLE", "TABLE_NAME", "register_table", *sorted(TABLE.tools)]
```

The helper returns a `TableBundle` with generated tool callables and
aliases. `query_table` and `get_table_schema` are emitted for every table; any
custom names (via ``query_tool_name`` or ``query_aliases``) become thin aliases.
`export_tools()` pushes everything onto the module namespace so users can import
the tools directly or reach them via MCP.

## 4. Register (nothing to edit!)

Discovery is automatic: once your module lives under `ds_mcp.tables.*` and
exposes a `TABLE` bundle, the registry picks it up. `FastMCP` servers that rely on `register_all_tables()` will see the new
tools automatically.

If you want a single-table server:

```python
from ds_mcp.tables.my_table import register_table
```

That helper is generated in Step 3.

## 5. Add a database connector (only if you need a new one)

The connector module now exposes a tiny registry. Register a connector once at
import time and reference it by name in your table definition:

```python
from ds_mcp.core.connectors import register_connector
from some_lib import PostgresConnector

register_connector("analytics_readonly", lambda: PostgresConnector(...))
```

Set `connector_type="analytics_readonly"` in `build_table` and you're done.

## 6. Test ad-hoc

You can import and run any generated tool directly:

```python
from ds_mcp.tables.my_table import latest_rows

print(latest_rows(limit=10))
```

Everything returns JSON text so it matches the MCP behaviour exactly.

## 7. Optional niceties

- Provide friendly aliases with `query_tool_name` / `query_aliases` (standard
  `query_table` / `get_table_schema` remain available automatically).
- Reuse macros such as `{{TABLE}}`, `{{SCHEMA}}`, and `{{TODAY}}` added by the
  executor automatically.

That’s it. One module, a couple of declarative lines, and the tools are live.
