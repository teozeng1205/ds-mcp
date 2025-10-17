#!/usr/bin/env python3
"""
Market Anomalies V3 MCP Server Entry Point

This server exposes ONLY the market_level_anomalies_v3 table.
Used when you want isolated access to just this table in Claude Desktop.
"""

import sys
import os
import logging

from mcp.server.fastmcp import FastMCP
from ds_mcp.core.registry import TableRegistry
from ds_mcp.tables.market_anomalies_v3 import register_table

# Configure logging to stderr (critical for MCP servers)
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s [%(name)s] %(message)s',
    stream=sys.stderr
)

log = logging.getLogger(__name__)


def main():
    """Run the Market Anomalies MCP server."""
    log.info("Starting Market Anomalies V3 MCP Server")

    # Initialize MCP server
    mcp = FastMCP("Market Anomalies V3")

    # Initialize table registry
    registry = TableRegistry()

    # Register ONLY the market anomalies table
    register_table(registry)

    log.info(f"Registered {len(registry)} table")

    for table in registry.get_all_tables():
        log.info(f"Registering {len(table.tools)} tools from {table.display_name}")

        for tool_func in table.tools:
            mcp.tool()(tool_func)
            log.info(f"  - Registered tool: {tool_func.__name__}")

    total_tools = sum(len(table.tools) for table in registry.get_all_tables())
    log.info(f"Total tools registered: {total_tools}")

    # Run the server
    mcp.run()


if __name__ == "__main__":
    main()
