#!/usr/bin/env python3
"""
DS-MCP Server Entry Point

Main entry point for running the DS-MCP server.
This server automatically registers all available tables and their tools.
"""

import sys
import os

# Add parent directory to path to import threevictors
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from ds_mcp.servers.base_server import run_server


if __name__ == "__main__":
    run_server("DS-MCP Multi-Table Server")
