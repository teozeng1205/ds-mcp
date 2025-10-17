#!/usr/bin/env python3
"""
Provider Combined Audit MCP Server Entry Point

This server exposes ONLY the monitoring_prod.provider_combined_audit table.
Use when you want isolated access to just this table in Claude Desktop.
"""

import sys
import os
import logging

# Add parent directory to path to import threevictors
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../..'))

from mcp.server.fastmcp import FastMCP
from ds_mcp.core.registry import TableRegistry
from ds_mcp.tables.provider_combined_audit import register_table

# Configure logging to stderr (critical for MCP servers)
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s [%(name)s] %(message)s',
    stream=sys.stderr
)

log = logging.getLogger(__name__)


def main():
    """Run the Provider Combined Audit MCP server."""
    log.info("Starting Provider Combined Audit MCP Server")

    # Initialize MCP server
    mcp = FastMCP("Provider Combined Audit")

    # Initialize table registry
    registry = TableRegistry()

    # Register ONLY the provider_combined_audit table
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
