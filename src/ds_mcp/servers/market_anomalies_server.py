#!/usr/bin/env python3
"""Market Anomalies MCP server entry point."""

from __future__ import annotations

from ds_mcp.servers.base_server import run_table_server


def main() -> None:
    run_table_server("anomalies")


if __name__ == "__main__":
    main()
