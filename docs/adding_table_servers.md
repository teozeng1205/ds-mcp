# Launching Table-Specific Servers

Dedicated server files are no longer required. Use the shared entrypoints:

```bash
# Provider only
python -m ds_mcp.server --table provider

# Market anomalies only
python -m ds_mcp.server --table anomalies

# Multiple tables
python -m ds_mcp.server --table provider --table analytics.some_table
```

The helper script mirrors the same behaviour:

```bash
bash ds-mcp/scripts/run_mcp_server.sh provider analytics.some_table
```

Servers automatically register the standard tools (describe, schema, head, query)
plus any custom SQL helpers defined for that table. No additional boilerplate is
needed.
