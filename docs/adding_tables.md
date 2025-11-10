# Adding a Table

Everything lives in `ds_mcp/tables/__init__.py`. Each entry is a call to
`build_table(...)` plus an optional tuple of `SQLToolSpec` definitions. The base
helper automatically exposes:

- `describe_table()` – shows introduction + key/partition columns
- `get_table_schema()` – pulls metadata from `svv_columns`
- `read_table_head(limit)` – lightweight preview (defaults to 50 rows)
- `query_table(sql, max_rows)` – SELECT/WITH-only execution with automatic LIMITs

## 1. Define custom SQL helpers (optional)

```python
from ds_mcp.tables.base import ParameterSpec, SQLToolSpec

MY_SQL_TOOLS = (
    SQLToolSpec(
        name="latest_rows",
        doc="Return the most recent records.",
        sql="SELECT * FROM {{TABLE}} ORDER BY snapshot_ts DESC LIMIT :limit",
        params=(
            ParameterSpec(name="limit", default=50, coerce=int, min_value=1, max_value=500, as_literal=True),
        ),
        enforce_limit=False,
    ),
)
```

Macros like `{{TABLE}}`, `{{SCHEMA}}`, and `{{TODAY}}` are available by default,
so most queries remain short strings.

## 2. Register the table

Append to the `TABLES` dict:

```python
from ds_mcp.tables.base import build_table

TABLES["example"] = build_table(
    slug="example",
    schema_name="analytics",
    table_name="example_table",
    description="Short blurb for prompts.",
    key_columns=("customer", "sales_date"),
    partition_columns=("sales_date",),
    macros={"TABLE": "analytics.example_table"},
    custom_tools=MY_SQL_TOOLS,
    query_aliases=("query_example",),  # optional alias for query_table
)
```

That’s it—`register_all_tables` automatically picks up the new entry, and the
chat client can target it via `--table example` (or the full `schema.table` if
you’d rather not add a slug).

## 3. Generic tables

If you don’t want to touch the file, you can call any `schema.table` (or
`database.schema.table`) directly from `chat.py`/`run_mcp_server.sh`. The helper
will synthesize a generic `Table` on the fly with the five core tools
(describe/schema/partitions/head/query).

## 4. Safety tips

- Keep introductions short but specific. Mention key columns + partition column.
- Custom SQL helpers should stay read-only; the base helper already blocks
  mutating statements.
- Prefer declarative macros so you don’t repeat schema names throughout strings.
