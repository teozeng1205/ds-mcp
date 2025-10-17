# DS-MCP: Scalable Model Context Protocol Server

> **Version 2.0** - A professional, scalable MCP server framework for exposing database tables as AI-accessible tools.

## Overview

DS-MCP is a modular framework that makes it easy to expose database tables via the Model Context Protocol (MCP), allowing AI assistants like Claude to query and analyze your data through natural language.

### Key Features

- **Scalable Architecture**: Add new tables without modifying core code
- **Table Registry System**: Automatic tool registration for multiple tables
- **Modular Design**: Clean separation of concerns (connectors, tools, queries)
- **Production Ready**: Proper Python package with professional structure
- **Easy to Extend**: Well-documented patterns for adding new tables
- **Type Safe**: Full type hints throughout the codebase

## Quick Start

Pick the path that fits your setup. The monorepo path is the easiest.

### Monorepo (recommended, uses env.sh and serialized SSO)

```bash
# From the repo root (../agents)
python -m venv .venv
source .venv/bin/activate

# Install required packages
pip install -U pip
pip install openai-agents ds-threevictors
# Optional: install ds-mcp in editable mode for development
(cd ds-mcp && pip install -e .)

# Create env.sh at repo root if you don't have one yet
cat > env.sh <<'EOF'
export AWS_PROFILE=3VDEV
export AWS_DEFAULT_REGION=us-east-1
# export OPENAI_API_KEY=sk-...
echo "Environment variables loaded from env.sh" >&2
EOF

# Run a server directly (stdio)
bash ds-agents/scripts/run_mcp_provider_audit_stdio.sh
# or
bash ds-agents/scripts/run_mcp_market_anomalies_stdio.sh
```

The ds‑agents wrappers source `env.sh` and reuse a shared AWS SSO login with a filesystem lock so only one browser window is opened even when multiple servers start together (e.g., Claude Desktop).

### Stand‑alone (classic .env.sh)

```bash
cd ds-mcp
python -m venv .venv && source .venv/bin/activate
pip install -e . -r requirements.txt

# Create .env.sh and source it (classic pattern)
cat > .env.sh <<'EOF'
export AWS_PROFILE=3VDEV
export AWS_DEFAULT_REGION=us-east-1
echo "Environment variables loaded from .env.sh" >&2
EOF
source .env.sh

# Preferred: run script (uses repo .venv and serialized SSO)
scripts/run_server.sh

# Dev alternative
# export PYTHONPATH="$PWD/src:$PWD/..:$PYTHONPATH" && python src/ds_mcp/server.py
# Or: mcp dev src/ds_mcp/server.py
```

## Project Structure

```
ds-mcp/
├── src/
│   └── ds_mcp/
│       ├── core/                    # Core framework
│       │   ├── connectors.py        # Database connections
│       │   ├── registry.py          # Table registry
│       │   └── tools.py             # Base tool classes
│       ├── tables/                  # Table-specific modules
│       │   ├── market_anomalies_v3/ # Example table module
│       │   │   ├── config.py        # Table configuration
│       │   │   ├── tools.py         # MCP tools
│       │   │   └── queries.py       # SQL queries (optional)
│       │   └── [your_table]/        # Add new tables here!
│       ├── servers/                 # Server implementations
│       │   └── base_server.py       # Main server logic
│       └── server.py                # Entry point
├── tests/                           # Test suite
│   ├── test_imports.py
│   ├── test_tools.py
│   └── integration/
├── scripts/                         # Utility scripts
│   ├── run_server.sh
│   └── explore_table.py
├── docs/                            # Documentation
│   ├── README.md                    # Detailed docs
│   ├── tutorial.md                  # Tutorial
│   └── adding_tables.md             # Guide for new tables
├── examples/                        # Usage examples
├── setup.py                         # Package configuration
├── pyproject.toml                   # Modern Python packaging
└── requirements.txt                 # Dependencies
```

## Currently Available Tables

### Market Level Anomalies V3
- **Table**: `analytics.market_level_anomalies_v3`
- **Tools**: 3 (query_anomalies, get_table_schema, get_available_customers)
- **Description**: Market-level pricing anomalies with impact scores

### Provider Combined Audit
- **Table**: `monitoring_prod.provider_combined_audit`
- **Tools**: 6
  - `query_audit` (ad‑hoc SQL with macros)
  - `get_table_schema`
  - `top_site_issues` (provider)
  - `list_provider_sites` (provider)
  - `issue_scope_combined` (single SQL; 2–4 dims)
  - `overview_site_issues_today` (single SQL)
- **Description**: Audit trail of provider‑level monitoring events and issues.
- **Key columns**: `sales_date` (YYYYMMDD int), `providercode`, `sitecode`, `issue_reasons`, `issue_sources`.
- **Macros**:
  - `{{PCA}}` → fully‑qualified table name
  - `{{EVENT_TS[:alias]}}` → event timestamp
  - `{{OBS_HOUR}}` → hour bucket from event timestamp
  - `{{OD}}` → `originairportcode || '-' || destinationairportcode`
  - `{{ISSUE_TYPE}}` → `COALESCE(NULLIF(TRIM(issue_reasons), ''), NULLIF(TRIM(issue_sources), ''))`
  - `{{IS_SITE}}` → sitecode present

Examples (in Claude):
- Top site issues (reasons or sources) for a provider over last 7 days:
  - Tool: `top_site_issues`, Args: `{ "provider": "QL2", "lookback_days": 7, "limit": 10 }`

- Scope across dims in one query (hour/POS/triptype/LOS):
  - Tool: `issue_scope_combined`, Args: `{ "provider": "QL2", "site": "QF", "dims": ["obs_hour","pos","triptype","los"], "lookback_days": 7 }`

- Ad‑hoc SQL with macros (filter out “OK” rows):
  - Tool: `query_audit`
  - SQL:
    `SELECT {{ISSUE_TYPE}} AS issue_key, COUNT(*)
     FROM {{PCA}}
     WHERE providercode ILIKE '%QL2%'
       AND COALESCE(NULLIF(TRIM(issue_reasons::VARCHAR), ''), NULLIF(TRIM(issue_sources::VARCHAR), '')) IS NOT NULL
       AND TO_DATE(sales_date::VARCHAR,'YYYYMMDD') >= CURRENT_DATE - 7
     GROUP BY 1 ORDER BY 2 DESC LIMIT 50`

 

## Adding a New Table

Adding a new table is simple! Follow these steps:

### 1. Create Table Module

```bash
mkdir -p src/ds_mcp/tables/my_table
cd src/ds_mcp/tables/my_table
touch __init__.py config.py tools.py
```

### 2. Define Configuration (`config.py`)

```python
from ds_mcp.core.registry import TableConfig

def get_table_config() -> TableConfig:
    from ds_mcp.tables.my_table import tools

    return TableConfig(
        name="schema.my_table",
        display_name="My Table",
        description="Description of my table",
        schema_name="schema",
        table_name="my_table",
        connector_type="analytics",
        tools=[
            tools.query_data,
            tools.get_schema,
        ]
    )
```

### 3. Implement Tools (`tools.py`)

```python
from ds_mcp.core.connectors import DatabaseConnectorFactory

def query_data(sql_query: str) -> str:
    """
    Query my_table data.

    Args:
        sql_query: SQL query to execute

    Returns:
        JSON with results
    """
    connector = DatabaseConnectorFactory.get_connector("analytics")
    # Implementation here...
```

### 4. Register Table (`src/ds_mcp/tables/__init__.py`)

```python
def register_all_tables(registry: TableRegistry) -> None:
    from ds_mcp.tables.market_anomalies_v3 import register_table as register_market
    from ds_mcp.tables.my_table import register_table as register_my_table

    register_market(registry)
    register_my_table(registry)  # Add your table!
```

That's it! The server will automatically pick up your new table and tools.

See [docs/adding_tables.md](docs/adding_tables.md) for a detailed guide.

## Configuration for Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS). Use absolute paths.

### Easiest: env.sh + wrappers (monorepo)

```json
{
  "mcpServers": {
    "provider-combined-audit": {
      "command": "bash",
      "args": ["/ABS/PATH/TO/agents/ds-agents/scripts/run_mcp_provider_audit_stdio.sh"]
    },
    "market-anomalies-v3": {
      "command": "bash",
      "args": ["/ABS/PATH/TO/agents/ds-agents/scripts/run_mcp_market_anomalies_stdio.sh"]
    }
  }
}
```

Notes:
- Wrappers source `env.sh` at the repo root and serialize AWS SSO login; no extra `env` needed in JSON.
- Use absolute paths; relative paths can fail when Claude Desktop changes working directory.

### Stand‑alone: classic run scripts

```json
{
  "mcpServers": {
    "provider-combined-audit": {
      "command": "bash",
      "args": ["/ABS/PATH/TO/agents/ds-mcp/scripts/run_provider_combined_audit.sh"]
    },
    "market-anomalies-v3": {
      "command": "bash",
      "args": ["/ABS/PATH/TO/agents/ds-mcp/scripts/run_market_anomalies.sh"]
    }
  }
}
```

Tip: Pre‑authenticate once in a terminal to avoid any browser prompt on startup: `aws sso login --profile 3VDEV`.

## Development

### Running Tests

```bash
# Test imports
python tests/test_imports.py

# Test tools
python tests/test_tools.py

# Integration tests
python tests/integration/test_mcp_connection.py
```

### Code Quality

```bash
# Install dev dependencies
pip install -r requirements-dev.txt

# Format code
black src/

# Type checking
mypy src/

# Linting
flake8 src/
```

## Architecture Highlights

### Table Registry Pattern
- Centralized management of all tables
- Dynamic tool registration
- Easy to query available tables and tools

### Database Connection Factory
- Singleton pattern for connections
- Support for multiple database types
- Automatic cleanup

### Modular Design
- Each table is self-contained
- No cross-table dependencies
- Easy to test and maintain

## Migration from v1.0

If you're upgrading from the old monolithic `server.py`:

1. Your existing server.py has been preserved as a reference
2. All functionality is maintained in the new structure
3. Tools work identically from the client perspective
4. See `REORGANIZATION_PLAN.md` for migration details

## Documentation

- **[docs/README.md](docs/README.md)**: Comprehensive documentation
- **[docs/tutorial.md](docs/tutorial.md)**: Step-by-step tutorial
- **[docs/adding_tables.md](docs/adding_tables.md)**: Guide for adding tables
- **[REORGANIZATION_PLAN.md](REORGANIZATION_PLAN.md)**: Architecture overview

## Requirements

- Python 3.10+
- AWS credentials configured
- Access to Analytics Redshift database
- ds-threevictors package installed

## License

Internal ATPCO tool - Not for external distribution

## Version History

- **v2.0.0** - Complete reorganization with scalable architecture
- **v1.0.0** - Initial implementation with market_level_anomalies_v3

## Support

For issues or questions, contact the ATPCO Data Science team.
