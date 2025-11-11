#!/usr/bin/env python3
"""
DS-MCP server entry point.

Provides a minimal MCP server framework for database exploration.

Usage::

    python -m ds_mcp.server --name "My Server"
    python -m ds_mcp.server --table schema.table_name
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import List, Sequence

from mcp.server.fastmcp import FastMCP


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
    """Create an MCP server instance with optional table configuration."""
    log.info("Creating MCP server: %s", server_name)
    mcp = FastMCP(server_name)

    if table_slugs:
        log.info("Configured tables: %s", ", ".join(table_slugs))

    # Core MCP server created - tools would be registered by integrating systems
    return mcp


def run_server(server_name: str = "DS-MCP Server", table_slugs: Sequence[str] | None = None) -> None:
    """Run the MCP server."""
    log.info("Starting %s", server_name)
    mcp = create_mcp_server(server_name, table_slugs=table_slugs)
    mcp.run()


def main(argv: List[str] | None = None) -> int:
    """Main entry point for the MCP server."""
    parser = argparse.ArgumentParser(description="Run the DS-MCP server.")
    parser.add_argument(
        "--table",
        "-t",
        action="append",
        dest="tables",
        help="Table identifier to configure (repeatable).",
    )
    parser.add_argument(
        "--name",
        help="Optional override for the MCP server name.",
    )

    args = parser.parse_args(argv)

    tables = args.tables or []
    server_name = args.name or "DS-MCP Server"

    run_server(server_name, table_slugs=tables)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
