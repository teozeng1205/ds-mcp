#!/usr/bin/env python3
"""
Test script to verify imports.
Requires ds-mcp and threevictors installed in the active environment.
"""

import sys

print("Testing imports...")

try:
    from mcp.server.fastmcp import FastMCP
    print("✓ MCP FastMCP imported successfully")
except ImportError as e:
    print(f"✗ Failed to import FastMCP: {e}")
    sys.exit(1)

try:
    from threevictors.dao import mysql_connector, redshift_connector
    print("✓ Database connectors imported successfully")
except ImportError as e:
    print(f"✗ Failed to import database connectors: {e}")
    sys.exit(1)

try:
    from threevictors.config_reader import config_reader
    print("✓ Config reader imported successfully")
except ImportError as e:
    print(f"✗ Failed to import config reader: {e}")
    sys.exit(1)

print("\n✓ All imports successful!")
print("\nNote: Full server test requires AWS credentials and database access.")
print("To run the server: source ../.env.sh && python server.py")
