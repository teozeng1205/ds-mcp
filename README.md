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

### Installation

```bash
# Clone and navigate to repository
cd ds-mcp

# Install in development mode
pip install -e .

# Or install dependencies manually
pip install -r requirements.txt
```

### Running the Server

```bash
# Option 1: Using the run script (recommended, uses .venv python if present)
cd scripts
./run_server.sh

# Option 2: Direct execution
source ../.env.sh
export PYTHONPATH="$PWD/src:$PWD/..:$PYTHONPATH"
python src/ds_mcp/server.py

# Option 3: Development mode with MCP CLI
mcp dev src/ds_mcp/server.py
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
- **Tools**: 4 (query_audit, get_table_schema, top_site_issues, issue_scope_breakdown)
- **Description**: Audit trail of provider-level monitoring events and issues.
- **Macros**: `{{PCA}}`, `{{OD}}`, `{{ISSUE_TYPE}}`, `{{EVENT_TS}}`, `{{OBS_HOUR}}`, `{{IS_SITE}}`, `{{IS_INVALID}}`, `{{LATEST_DATE}}`.

Examples (in Claude with DS-MCP server running):
- Top site issues for a provider over last 7 days:
  - Tool: `top_site_issues`
  - Args: `{ "provider": "QL2|QF", "lookback_days": 7, "limit": 10 }`

- Scope of site issues across dimensions (hour/POS/triptype/LOS/O&D/cabin/depart):
  - Tool: `issue_scope_breakdown`
  - Args: `{ "provider": "QL2|QF", "lookback_days": 7, "per_dim_limit": 10 }`

- Ad-hoc SQL with macros (site-related only, latest date):
  - Tool: `query_audit`
  - SQL: `SELECT {{ISSUE_TYPE:issue_type}}, COUNT(*) FROM {{PCA}} WHERE {"provider"} ILIKE '%%QL2|QF%%' AND {{IS_SITE}} GROUP BY 1 ORDER BY 2 DESC LIMIT 50`

 

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

Add to your Claude Desktop config file:

- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`

Minimal example to run the Market Anomalies server (adjust paths):

```json
{
  "mcpServers": {
    "market-anomalies-v3": {
      "command": "bash",
      "args": ["/Users/you/path/agents/ds-mcp/scripts/run_market_anomalies.sh"],
      "env": {
        "AWS_ACCESS_KEY_ID": "your-key",
        "AWS_SECRET_ACCESS_KEY": "your-secret",
        "AWS_SESSION_TOKEN": "your-token"
      }
    }
  }
}
```

Notes:
- Ensure `.env.sh` at the repo root exports required variables (e.g., Redshift creds, region) and that the Redshift properties file `database-analytics-redshift-serverless-reader.properties` is discoverable by the connector.
- The server logs to stderr and exposes tools to Claude Desktop under the configured name.
- Wrapper scripts prefer Python at `./.venv/bin/python3` (repo-local) or `../.venv/bin/python3` (parent) so the internal `threevictors` package is importable.

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
