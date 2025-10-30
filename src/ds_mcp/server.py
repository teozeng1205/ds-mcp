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
import sys
from typing import List

from ds_mcp.servers.base_server import run_server
from ds_mcp.tables import get_table_definition, list_available_tables


def _print_available_tables() -> None:
    for slug, display_name in list_available_tables():
        definition = get_table_definition(slug)
        print(f"{slug:>12}  {definition.full_table_name()}  ({display_name})")


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
            server_name = get_table_definition(tables[0]).display_name
        run_server(server_name, table_slugs=tables)
    else:
        server_name = args.name or "DS-MCP Multi-Table Server"
        run_server(server_name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
