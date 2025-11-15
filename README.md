# ds-mcp

Model Context Protocol (MCP) server that turns our analytics Redshift data into first-class tools for OpenAI Agents.  
It launches as a subprocess, registers table-aware utilities, and streams JSON results back to any MCP-compliant client (CLI, FastAPI backend, or other agents).

## Features

- **AnalyticsReader wrapper** – reuses the internal `threevictors.dao.redshift_connector` for credentialed access.
- **FastMCP-based server** – lightweight async implementation with stdio transport.
- **Tool catalog** – describe tables, inspect schemas, preview rows, execute bounded SQL, and run provider monitoring helpers (`get_top_site_issues`, `analyze_issue_scope`).
- **Configurable tables** – pass `--table <schema.table>` repeatedly to restrict what the agent can see.
- **Safe defaults** – SELECT-only enforcement plus automatic `LIMIT` behavior for arbitrary SQL.

## Installation

```bash
cd ds-agentic-workflows/ds-mcp
python -m venv .venv && source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt         # mcp[cli], pandas, boto3, redshift-connector …
pip install -e .                        # optional editable install
```

Set your AWS profile / credentials and OpenAI key in the repo-level `env.sh` before running anything. The AnalyticsReader expects VPN + AWS SSO access to the analytics Redshift environment (e.g., `aws sso login --profile 3VDEV`).

## Running the Server

```bash
# From ds-mcp/
python -m ds_mcp.server --name "Analytics Server"

# Limit tools to specific tables
python -m ds_mcp.server --name "Provider Audit" \
  --table prod.monitoring.provider_combined_audit \
  --table local.analytics.market_level_anomalies_v3
```

When invoked via `agent_core.AgentExecutor` or `scripts/run_mcp_server.sh`, the wrapper automatically sources `env.sh` and injects `PYTHONPATH` so the package can be used without installation.

## Tools Exposed

| Tool | Purpose |
| --- | --- |
| `describe_table(table_name)` | Information-schema lookup for table metadata. |
| `get_table_schema(table_name)` | Column definitions with type, nullability, defaults. |
| `read_table_head(table_name, limit=50)` | Preview first N rows (works across databases). |
| `query_table(query, limit=1000)` | Executes SELECT/WITH statements with enforced limits. |
| `get_top_site_issues(target_date?)` | Compares provider issues for today vs. last week/month. |
| `analyze_issue_scope(providercode?, sitecode?, target_date?, lookback_days=7)` | Breaks down provider/site issues by geography, trip type, cabin, LOS, etc. |

Each tool returns JSON (DataFrame `orient='records'`), which upstream agents present as structured answers.

## Using the AnalyticsReader Directly

```python
from ds_mcp.core.connectors import AnalyticsReader

reader = AnalyticsReader()
df = reader.read_table_head('prod.monitoring.provider_combined_audit', limit=10)
issues = reader.get_top_site_issues('20251109')
scope = reader.analyze_issue_scope(providercode='QL2', sitecode='QF', lookback_days=14)
```

## Extending the Server

1. Add new helper methods to `AnalyticsReader` for any custom SQL you need.
2. Register them in `src/ds_mcp/server.py` using `@mcp.tool()` decorators.
3. Bump `EXPOSED_TOOLS` / `allowed_tools` in the consuming agent to make them available.

The MCP binary stays transport-agnostic, so any client that understands Model Context Protocol can use the new capabilities immediately.
