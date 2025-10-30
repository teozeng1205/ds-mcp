"""
Base MCP server implementation.

Provides helpers to create servers either with all available tables or a
filtered subset. Individual server modules simply call these functions with
the desired table slug(s).
"""

from __future__ import annotations

import logging
import sys
from typing import Sequence

from mcp.server.fastmcp import FastMCP

from ds_mcp.core.registry import TableRegistry
from ds_mcp.tables import get_table_definition, register_all_tables

# Configure logging to stderr (critical for MCP servers)
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s [%(name)s] %(message)s",
    stream=sys.stderr,
)

log = logging.getLogger(__name__)


def create_mcp_server(
    server_name: str = "DS-MCP Server",
    table_slugs: Sequence[str] | None = None,
) -> FastMCP:
    """
    Create a FastMCP server and register tables/tools.

    Args:
        server_name: Name presented to MCP clients.
        table_slugs: Optional sequence of table slugs to include. When omitted
            all discovered tables are registered.
    """
    log.info("Creating MCP server: %s", server_name)

    mcp = FastMCP(server_name)
    registry = TableRegistry()
    registered = register_all_tables(registry, only=table_slugs)
    log.info("Registered tables: %s", ", ".join(registered))

    total_tools = 0
    for table in registry.get_all_tables():
        log.info("Registering %s tools from %s", len(table.tools), table.display_name)
        for tool_func in table.tools:
            mcp.tool()(tool_func)
            total_tools += 1
            log.debug("  - Registered tool: %s", tool_func.__name__)

    log.info("Total tools registered: %s", total_tools)
    return mcp


def run_server(
    server_name: str = "DS-MCP Server",
    table_slugs: Sequence[str] | None = None,
) -> None:
    """Create and run the MCP server."""
    log.info("Starting %s", server_name)
    mcp = create_mcp_server(server_name, table_slugs=table_slugs)
    mcp.run()


def run_table_server(slug: str, server_name: str | None = None) -> None:
    """
    Convenience helper for servers that expose a single table.

    Args:
        slug: Table slug (see :func:`ds_mcp.tables.list_available_tables`).
        server_name: Optional server name override.
    """
    definition = get_table_definition(slug)
    label = server_name or definition.display_name
    run_server(label, table_slugs=[slug])
