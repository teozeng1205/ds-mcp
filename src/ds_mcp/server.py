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
        try:
            _analytics_reader = AnalyticsReader()
        except Exception as e:
            log.error(f"Failed to initialize AnalyticsReader: {e}")
            log.error("Please ensure you are connected to VPN and have proper database credentials")
            raise RuntimeError(
                "Cannot connect to Redshift database. "
                "Please check: (1) VPN connection, (2) database credentials, (3) network connectivity"
            ) from e
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

    @mcp.resource("table://available_tables")
    def get_available_tables() -> str:
        """
        List of commonly used tables in the analytics database.

        Returns:
            Information about available tables and their full names.
        """
        return "There is a table called prod.monitoring.provider_combined_audit"

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
        Get data preview (first N rows) from a table. Use for schema exploration only.
        For filtered data or analysis, write a SQL query using query_table instead.

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

    @mcp.tool()
    def get_top_site_issues(target_date: str | None = None) -> str:
        """
        Get top site issues for a specific date and compare with last week and last month.

        This function analyzes the provider_combined_audit table to identify the most common
        site issues and provides trend comparison.

        Args:
            target_date: Date in YYYYMMDD format (e.g., '20251109'). If not provided, uses today's date.

        Returns:
            JSON string with columns:
            - sitecode: Site code with issues
            - issue_sources: Source of the issue (e.g., 'request', 'response')
            - issue_reasons: Reason for the issue (e.g., 'Flights N/A', 'Direct Flight N/A')
            - today_count: Number of issues on target date
            - last_week_count: Number of issues 7 days ago
            - last_month_count: Number of issues 30 days ago
            - week_over_week_change: Change from last week
            - month_over_month_change: Change from last month

        Example:
            get_top_site_issues('20251109')
            get_top_site_issues()  # Uses today's date
        """
        try:
            df = reader.get_top_site_issues(target_date)
            return df.to_json(orient='records', indent=2)
        except Exception as e:
            log.error(f"get_top_site_issues failed: {e}", exc_info=True)
            return f'{{"error": "Failed to get top site issues: {str(e)}"}}'

    @mcp.tool()
    def analyze_issue_scope(
        providercode: str | None = None,
        sitecode: str | None = None,
        target_date: str | None = None,
        lookback_days: int = 7
    ) -> str:
        """
        Analyze the scope and dimensions of issues for providers and/or sites.

        This function breaks down issues by multiple dimensions to identify patterns and
        concentrations in the data. You can filter by provider, site, or both.

        Args:
            providercode: Provider code(s) - single (e.g., 'QL2') or comma-separated (e.g., 'QL2,Atlas')
            sitecode: Site code(s) - single (e.g., 'QF') or comma-separated (e.g., 'QF,DY')
            target_date: End date in YYYYMMDD format (default: today)
            lookback_days: Number of days to look back from target_date (default: 7)

        Returns:
            JSON string with dimensional breakdown including:
            - Geographic dimensions: POS, origin/destination airports/cities/countries
            - Travel dimensions: triptype, LOS (length of stay), cabin, departdate, depart_dow (day of week)
            - Temporal dimensions: observation_hour (hour of observation)
            - Issue details: issue_sources, issue_reasons, response_statuses, filterreason
            - Metrics: issue_count, days_with_issues, first_seen_date, last_seen_date

        Example:
            analyze_issue_scope(providercode='QL2', sitecode='QF')  # Single provider and site
            analyze_issue_scope(sitecode='QF')  # All providers for QF site
            analyze_issue_scope(providercode='QL2')  # All sites for QL2 provider
            analyze_issue_scope(sitecode='QF,DY,ET')  # Multiple sites
            analyze_issue_scope(providercode='QL2,Atlas')  # Multiple providers
        """
        try:
            df = reader.analyze_issue_scope(providercode, sitecode, target_date, lookback_days)
            if len(df) == 0:
                filter_desc = []
                if providercode:
                    filter_desc.append(f"provider={providercode}")
                if sitecode:
                    filter_desc.append(f"site={sitecode}")
                filter_str = ", ".join(filter_desc) if filter_desc else "specified filters"
                return f'{{"message": "No issues found for {filter_str}"}}'
            return df.to_json(orient='records', indent=2)
        except Exception as e:
            log.error(f"analyze_issue_scope failed: {e}", exc_info=True)
            return f'{{"error": "Failed to analyze issue scope: {str(e)}"}}'

    log.info("Registered analytics tools: describe_table, get_table_schema, read_table_head, "
             "query_table, get_top_site_issues, analyze_issue_scope")


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
