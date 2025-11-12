ds-mcp

- What: MCP server for database exploration using AnalyticsReader with Redshift connector.

- Quick Start (repo root)
  - python -m venv .venv && source .venv/bin/activate
  - pip install -U openai-agents
  - pip install threevictors pandas redshift-connector
  - pip install -e ds-mcp
  - Setup AWS credentials: `assume 3VDEV` (or appropriate environment)

- Run server: `python -m ds_mcp.server --name "Analytics Server"`

- Connect from a client
  - Use any MCP client to connect to the server started above
  - Or use the interactive chat: `python chat.py`

## Core Tools

The MCP server exposes 4 core analytics tools via AnalyticsReader:

1. **describe_table(table_name)** - Get table metadata (schema, type)
   - Works for current database tables (e.g., `price_anomalies.anomaly_table`)
   - For cross-database queries, use `read_table_head()` instead

2. **get_table_schema(table_name)** - Get column information (names, types, nullable)
   - Works for current database tables
   - For cross-database queries, use `read_table_head()` instead

3. **read_table_head(table_name, limit=50)** - Preview first N rows
   - Supports cross-database queries (e.g., `prod.monitoring.provider_combined_audit`)
   - Returns pandas DataFrame as JSON

4. **query_table(query, limit=1000)** - Execute custom SELECT queries
   - Full SQL SELECT support with safety limits
   - Supports cross-database queries
   - Returns pandas DataFrame as JSON

## Example Usage

```python
from ds_mcp.core.connectors import AnalyticsReader

reader = AnalyticsReader()

# Preview data from cross-database table
df = reader.read_table_head('prod.monitoring.provider_combined_audit', limit=10)

# Custom SQL query
df = reader.query_table('''
    SELECT * FROM prod.monitoring.provider_combined_audit
    WHERE sales_date = 20251109
    LIMIT 100
''')

# Get schema for current database table
schema = reader.get_table_schema('price_anomalies.anomaly_table')
```

## Notes

- Cross-database queries: `describe_table()` and `get_table_schema()` work only within the current database due to Redshift information_schema limitations
- For cross-database table exploration (e.g., `prod.monitoring.*`), use `read_table_head()` or `query_table()`
- All queries require proper AWS credentials and Redshift access

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
