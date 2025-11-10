#!/usr/bin/env python3
"""
DS-MCP server entry point.

Usage::

    python -m ds_mcp.server                 # run with all tables
    python -m ds_mcp.server --table provider
    python -m ds_mcp.server --table provider --table anomalies
    python -m ds_mcp.server --list
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import List, Sequence

from mcp.server.fastmcp import FastMCP

from ds_mcp.core.registry import TableRegistry
from ds_mcp.tables import get_table, list_available_tables, register_all_tables


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
    log.info("Creating MCP server: %s", server_name)
    mcp = FastMCP(server_name)
    registry = TableRegistry()
    registered = register_all_tables(registry, only=table_slugs)
    log.info("Registered tables: %s", ", ".join(registered))

    total_tools = 0
    for table in registry.get_all_tables():
        log.info("Registering %s tools from %s", len(table.tools), table.display_name)
        for tool in table.tools:
            mcp.tool()(tool)
            total_tools += 1
    log.info("Total tools registered: %s", total_tools)
    return mcp


def run_server(server_name: str = "DS-MCP Server", table_slugs: Sequence[str] | None = None) -> None:
    log.info("Starting %s", server_name)
    mcp = create_mcp_server(server_name, table_slugs=table_slugs)
    mcp.run()


def _print_available_tables() -> None:
    for slug, display_name in list_available_tables():
        table = get_table(slug)
        print(f"{slug:>12}  {table.full_name}  ({display_name})")


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the DS-MCP server.")
    parser.add_argument(
        "--table",
        "-t",
        action="append",
        dest="tables",
        help="Limit the server to the specified table slug (repeatable).",
    )
    parser.add_argument(
        "--name",
        help="Optional override for the MCP server name.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available table slugs and exit.",
    )

    args = parser.parse_args(argv)

    if args.list:
        _print_available_tables()
        return 0

    tables = args.tables or []
    if tables:
        server_name = args.name or "DS-MCP Server"
        if len(tables) == 1 and not args.name:
            table = get_table(tables[0])
            server_name = table.table_name.replace("_", " ").title()
        run_server(server_name, table_slugs=tables)
    else:
        server_name = args.name or "DS-MCP Multi-Table Server"
        run_server(server_name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
