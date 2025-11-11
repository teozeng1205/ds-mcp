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

from ds_mcp.core.connectors import AnalyticsReader


logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s [%(name)s] %(message)s",
    stream=sys.stderr,
)

log = logging.getLogger(__name__)

# Global AnalyticsReader instance (initialized lazily)
_analytics_reader = None


def get_analytics_reader() -> AnalyticsReader:
    """Get or create the global AnalyticsReader instance."""
    global _analytics_reader
    if _analytics_reader is None:
        _analytics_reader = AnalyticsReader()
    return _analytics_reader


def create_mcp_server(
    server_name: str = "DS-MCP Server",
    table_slugs: Sequence[str] | None = None,
) -> FastMCP:
    """Create an MCP server instance with optional table configuration."""
    log.info("Creating MCP server: %s", server_name)
    mcp = FastMCP(server_name)

    if table_slugs:
        log.info("Configured tables: %s", ", ".join(table_slugs))

    # Register analytics tools
    _register_analytics_tools(mcp)

    return mcp


def _register_analytics_tools(mcp: FastMCP) -> None:
    """Register analytics database tools with the MCP server."""
    reader = get_analytics_reader()

    @mcp.tool()
    def describe_table(table_name: str) -> dict:
        """
        Get metadata and key information about a table.

        Args:
            table_name: Full table name (e.g., 'analytics.market_level_anomalies')

        Returns:
            Dictionary with table metadata
        """
        return reader.describe_table(table_name)

    @mcp.tool()
    def get_table_schema(table_name: str) -> str:
        """
        Get full column information for a table.

        Args:
            table_name: Full table name (e.g., 'analytics.oag_score_v2')

        Returns:
            JSON string of column information DataFrame
        """
        df = reader.get_table_schema(table_name)
        return df.to_json(orient='records', indent=2)

    @mcp.tool()
    def read_table_head(table_name: str, limit: int = 50) -> str:
        """
        Get data preview (first N rows) from a table.

        Args:
            table_name: Full table name (e.g., 'analytics.revenue_score_v1')
            limit: Number of rows to return (default: 50)

        Returns:
            JSON string of DataFrame with first N rows
        """
        df = reader.read_table_head(table_name, limit)
        return df.to_json(orient='records', indent=2)

    @mcp.tool()
    def query_table(query: str, limit: int = 1000) -> str:
        """
        Execute a SELECT query on the database.

        Args:
            query: SQL SELECT statement
            limit: Maximum rows to return (default: 1000, safety limit)

        Returns:
            JSON string of query results DataFrame
        """
        df = reader.query_table(query, limit)
        return df.to_json(orient='records', indent=2)

    log.info("Registered analytics tools: describe_table, get_table_schema, read_table_head, "
             "query_table")


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
