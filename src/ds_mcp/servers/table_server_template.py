#!/usr/bin/env python3
"""
Template for Table-Specific MCP Server

Copy this file to create a new table-specific server.
Replace [TABLE_NAME] with your actual table name.

Example:
  - Copy to: my_table_server.py
  - Import: from ds_mcp.tables.my_table import register_table
  - Name: "My Table Server"
"""

import sys
import os
import logging

from mcp.server.fastmcp import FastMCP
from ds_mcp.core.registry import TableRegistry
# TODO: Replace with your table import
# from ds_mcp.tables.[TABLE_NAME] import register_table

# Configure logging to stderr (critical for MCP servers)
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s [%(name)s] %(message)s',
    stream=sys.stderr
)

log = logging.getLogger(__name__)


def main():
    """Run the [TABLE_NAME] MCP server."""
    log.info("Starting [TABLE_NAME] MCP Server")

    # Initialize MCP server
    mcp = FastMCP("[TABLE_NAME] Server")

    # Initialize table registry
    registry = TableRegistry()

    # Register ONLY this table
    # TODO: Uncomment and replace with your register function
    # register_table(registry)

    log.info(f"Registered {len(registry)} table")

    # Register all tools from the table
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
