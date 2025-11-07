ds-mcp

- What: Minimal MCP servers for Provider Combined Audit and Market Anomalies.

- Quick Start (repo root)
  - python -m venv .venv && source .venv/bin/activate
  - pip install -U openai-agents
  - pip install -e ds-threevictors -r ds-mcp/requirements.txt -e ds-mcp
  - Create `env.sh` at repo root with `AWS_PROFILE`, `AWS_DEFAULT_REGION`, `OPENAI_API_KEY`
- Run server: `bash ds-mcp/scripts/run_mcp_server.sh [slug]` (use `--list` to see options)

- Connect from a client
  - Use any MCP client to connect to the servers started above.

- More
  - Details and table guides: `docs/`

## Authoring tables

Tables now subclass `ds_mcp.tables.base.TableBlueprint`, define a handful of class
attributes (slug, schema, table name, display name, macros, SQL tool specs), and
call `build()` to obtain a `TableBundle`. The blueprint automatically wires up
shared tools such as `query_table`/`get_table_schema` and exports them via
`export_tools`, keeping new table modules down to a few dozen lines.

```python
from ds_mcp.tables.base import PartitionGuardrail, TableBlueprint, export_tools

class ExampleTable(TableBlueprint):
    slug = "example"
    schema_name = "analytics"
    table_name = "example_table"
    display_name = "Example"
    macros = MY_MACROS
    sql_tools = MY_SQL_TOOLS
    partition_guardrail = PartitionGuardrail(column="sales_date")

TABLE = ExampleTable().build()
export_tools(TABLE, globals())
```

## Partition guardrails

All Redshift tables are partitioned, so blueprints can opt into
`PartitionGuardrail` to assert that ad-hoc queries include a predicate on the
partition column (e.g., `sales_date`). Guardrails can operate in `error` or
`warn` mode and surface a helpful hint when a user forgets to filter; this keeps
table scans inexpensive without sacrificing flexibility.
