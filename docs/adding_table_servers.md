# Quick Guide: Adding a New Table Server

## Overview

This guide shows you how to create a dedicated MCP server for a new table in just **5 minutes**.

## Prerequisites

- You've already created the table module in `src/ds_mcp/tables/your_table/`
- Your table has `config.py`, `tools.py`, and `__init__.py`
- Your table is registered in `src/ds_mcp/tables/__init__.py`

## Step-by-Step Process

### 1. Create Server File (2 minutes)

```bash
cd src/ds_mcp/servers
cp table_server_template.py your_table_server.py
```

**Edit `your_table_server.py`:**

Replace these lines:
```python
# FROM:
# from ds_mcp.tables.[TABLE_NAME] import register_table

# TO:
from ds_mcp.tables.your_table import register_table

# AND

# FROM:
log.info("Starting [TABLE_NAME] MCP Server")
mcp = FastMCP("[TABLE_NAME] Server")

# TO:
log.info("Starting Your Table MCP Server")
mcp = FastMCP("Your Table Server")
```

**Uncomment the register line:**
```python
# FROM:
# register_table(registry)

# TO:
register_table(registry)
```

### 2. Create Launch Script (1 minute)

```bash
cd scripts
cp run_table_template.sh run_your_table.sh
chmod +x run_your_table.sh
```

**Edit `run_your_table.sh`:**

Replace the last line:
```bash
# FROM:
echo "Error: This is a template. Copy and customize for your table." >&2
exit 1

# TO:
exec "$PYTHON" "$ROOT_DIR/src/ds_mcp/servers/your_table_server.py"
```

### 3. Test Your Server (1 minute)

```bash
cd /path/to/ds-mcp
bash scripts/run_your_table.sh
```

**Expected output:**
```
INFO Starting Your Table MCP Server
INFO Registered 1 table
INFO Registering X tools from Your Table
INFO   - Registered tool: tool_name_1
INFO   - Registered tool: tool_name_2
INFO Total tools registered: X
```

Press Ctrl+C to stop.

### 4. Add to Claude Desktop (1 minute)

**Edit:** `~/Library/Application Support/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "ds-mcp-all-tables": {
      "command": "bash",
      "args": ["/full/path/to/scripts/run_with_env.sh"]
    },
    "market-anomalies-v3": {
      "command": "bash",
      "args": ["/full/path/to/scripts/run_market_anomalies.sh"]
    },
    "your-table": {
      "command": "bash",
      "args": ["/full/path/to/scripts/run_your_table.sh"]
    }
  }
}
```

**Get full path:**
```bash
cd /path/to/ds-mcp/scripts
pwd
# Copy the output and use it in the config
```

### 5. Restart Claude Desktop

1. Quit Claude Desktop (Cmd+Q on macOS)
2. Reopen Claude Desktop
3. Look for your table tools in the MCP tools list

## Complete Example

Let's say you're adding a `sales_data` table:

**1. Server file: `sales_data_server.py`**
```python
#!/usr/bin/env python3
import sys
import os
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../..'))

from mcp.server.fastmcp import FastMCP
from ds_mcp.core.registry import TableRegistry
from ds_mcp.tables.sales_data import register_table  # ← Your table

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s [%(name)s] %(message)s',
    stream=sys.stderr
)

log = logging.getLogger(__name__)

def main():
    log.info("Starting Sales Data MCP Server")  # ← Your name
    mcp = FastMCP("Sales Data Server")  # ← Your name
    registry = TableRegistry()

    register_table(registry)  # ← Uncommented

    log.info(f"Registered {len(registry)} table")

    for table in registry.get_all_tables():
        log.info(f"Registering {len(table.tools)} tools from {table.display_name}")
        for tool_func in table.tools:
            mcp.tool()(tool_func)
            log.info(f"  - Registered tool: {tool_func.__name__}")

    total_tools = sum(len(table.tools) for table in registry.get_all_tables())
    log.info(f"Total tools registered: {total_tools}")

    mcp.run()

if __name__ == "__main__":
    main()
```

**2. Launch script: `run_sales_data.sh`**
```bash
#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$SCRIPT_DIR/.."
ENV_FILE="$ROOT_DIR/../.env.sh"

if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
else
    echo "Error: .env.sh not found at $ENV_FILE" >&2
    exit 1
fi

export PYTHONPATH="$ROOT_DIR/src:$ROOT_DIR/..:$PYTHONPATH"

if [ -f "$ROOT_DIR/../.venv/bin/python3" ]; then
    PYTHON="$ROOT_DIR/../.venv/bin/python3"
else
    PYTHON="python3"
fi

exec "$PYTHON" "$ROOT_DIR/src/ds_mcp/servers/sales_data_server.py"
```

**3. Claude config:**
```json
{
  "mcpServers": {
    "sales-data": {
      "command": "bash",
      "args": ["/Users/you/agents/ds-mcp/scripts/run_sales_data.sh"]
    }
  }
}
```

## Naming Conventions

| Component | Pattern | Example |
|-----------|---------|---------|
| Table module | `your_table` | `sales_data` |
| Server file | `{table}_server.py` | `sales_data_server.py` |
| Launch script | `run_{table}.sh` | `run_sales_data.sh` |
| Config key | `{table-kebab}` | `sales-data` |
| Server name | `{Table Title} Server` | `Sales Data Server` |

## Checklist

Before adding to Claude Desktop, verify:

- [ ] Server file created from template
- [ ] Import statement updated with your table
- [ ] Server name and log messages customized
- [ ] `register_table(registry)` uncommented
- [ ] Launch script created and customized
- [ ] Launch script is executable (`chmod +x`)
- [ ] Script runs without errors
- [ ] Tools are listed in output
- [ ] Full path used in Claude config
- [ ] Config file is valid JSON

## Troubleshooting

### Import Error: Module not found

**Problem**: `ModuleNotFoundError: No module named 'ds_mcp.tables.your_table'`

**Solution**: Ensure your table module exists:
```bash
ls -la src/ds_mcp/tables/your_table/
# Should show: __init__.py, config.py, tools.py
```

### Server Not Starting

**Problem**: Script runs but server doesn't start

**Solution**: Check for syntax errors:
```bash
python3 -m py_compile src/ds_mcp/servers/your_table_server.py
```

### Tools Not Appearing

**Problem**: Server starts but Claude doesn't see tools

**Solution**:
1. Check Claude Desktop logs: `tail -f ~/Library/Logs/Claude/mcp*.log`
2. Verify tools are registered in output
3. Restart Claude Desktop completely (Cmd+Q)

### Script Permission Denied

**Problem**: `Permission denied` when running script

**Solution**:
```bash
chmod +x scripts/run_your_table.sh
```

## Advanced: Custom Configuration

### Different AWS Credentials Per Server

```json
{
  "mcpServers": {
    "your-table": {
      "command": "bash",
      "args": ["/path/to/scripts/run_your_table.sh"],
      "env": {
        "AWS_ACCESS_KEY_ID": "specific-key-for-this-table",
        "AWS_SECRET_ACCESS_KEY": "specific-secret"
      }
    }
  }
}
```

### Different Database Connector

Edit your server file to use a different connector:

```python
# In your_table_server.py, if you need MySQL instead of Redshift:
from ds_mcp.core.connectors import DatabaseConnectorFactory

# Your table's tools should specify:
connector = DatabaseConnectorFactory.get_connector("mysql")  # or "analytics"
```

## Next Steps

After successfully adding your table server:

1. **Test thoroughly**: Try all tools in Claude
2. **Document tools**: Update your table's README
3. **Share patterns**: If you create useful tools, share with team
4. **Monitor logs**: Check for any errors or warnings

## Resources

- **Template Files**:
  - `src/ds_mcp/servers/table_server_template.py`
  - `scripts/run_table_template.sh`

- **Examples**:
  - `src/ds_mcp/servers/market_anomalies_server.py`
  - `scripts/run_market_anomalies.sh`

- **Documentation**:
  - [MULTI_SERVER_SETUP.md](../MULTI_SERVER_SETUP.md)
  - [docs/adding_tables.md](adding_tables.md)

---

**Questions?** Check the main documentation or review the market_anomalies_v3 example.
