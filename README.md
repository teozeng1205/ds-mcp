ds-mcp

- What: Minimal MCP servers for Provider Combined Audit and Market Anomalies.

- Quick Start (repo root)
  - python -m venv .venv && source .venv/bin/activate
  - pip install -U openai-agents
  - pip install -e ds-threevictors -r ds-mcp/requirements.txt -e ds-mcp
  - Create `env.sh` at repo root with `AWS_PROFILE`, `AWS_DEFAULT_REGION`, `OPENAI_API_KEY`
- Run server: `bash ds-mcp/scripts/run_mcp_server.sh [slug ...]` (use `--list` to see options or provide multiple table identifiers)

- Connect from a client
  - Use any MCP client to connect to the servers started above.

- More
  - Details and table guides: `docs/`

## Authoring tables

Every table is described once via `ds_mcp.tables.base.build_table`. You provide
the schema/table names, a short introduction (key columns + partitions), and any
custom SQL helpers. The base class automatically exposes five core tools:

- `describe_table()` – structured intro plus key/partition columns
- `get_table_partitions()` – partition metadata from `svv_table_info`
- `get_table_schema()` – metadata from `svv_columns`
- `read_table_head(limit=50)` – quick sampling
- `query_table(sql, max_rows=None)` – safe SELECT/WITH execution with auto LIMITs

Custom tools stay declarative through `SQLToolSpec`.

```python
from ds_mcp.tables.base import ParameterSpec, SQLToolSpec, build_table, export_tools

CUSTOM_TOOLS = (
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

TABLE = build_table(
    slug="example",
    schema_name="analytics",
    table_name="example_table",
    description="Short blurb for client prompts.",
    key_columns=("customer", "sales_date"),
    partition_columns=("sales_date",),
    custom_tools=CUSTOM_TOOLS,
)

export_tools(TABLE, globals())
```
