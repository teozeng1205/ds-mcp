"""
Base MCP server implementation.

Creates an MCP server that automatically registers all tools from the table registry.
"""

import sys
import logging

from mcp.server.fastmcp import FastMCP
from ds_mcp.core.registry import TableRegistry
from ds_mcp.tables import register_all_tables

# Configure logging to stderr (critical for MCP servers)
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s [%(name)s] %(message)s',
    stream=sys.stderr
)

log = logging.getLogger(__name__)


def create_mcp_server(server_name: str = "DS-MCP Server") -> FastMCP:
    """
    Create an MCP server with all registered tables.

    This function creates a FastMCP server instance and registers all tools
    from all tables in the registry.

    Args:
        server_name: Name for the MCP server

    Returns:
        FastMCP server instance ready to run
    """
    log.info(f"Creating MCP server: {server_name}")

    # Initialize MCP server
    mcp = FastMCP(server_name)

    # Initialize table registry
    registry = TableRegistry()

    # Register all tables
    register_all_tables(registry)

    log.info(f"Registered {len(registry)} tables")

    # Register all tools from all tables
    for table in registry.get_all_tables():
        log.info(f"Registering {len(table.tools)} tools from {table.display_name}")

        for tool_func in table.tools:
            # Register the tool with MCP
            # The tool function already has its docstring which FastMCP will use
            mcp.tool()(tool_func)
            log.info(f"  - Registered tool: {tool_func.__name__}")

    total_tools = sum(len(table.tools) for table in registry.get_all_tables())
    log.info(f"Total tools registered: {total_tools}")

    return mcp


def run_server(server_name: str = "DS-MCP Server"):
    """
    Create and run the MCP server.

    This is the main entry point for running the server.

    Args:
        server_name: Name for the MCP server
    """
    log.info(f"Starting {server_name}")
    mcp = create_mcp_server(server_name)
    mcp.run()
