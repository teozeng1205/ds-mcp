"""
Core functionality for DS-MCP server.

This module provides the base classes and utilities for database connections,
table registry, and tool generation.
"""

from ds_mcp.core.connectors import AnalyticsReader
from ds_mcp.core.registry import TableRegistry, TableConfig

__all__ = ["AnalyticsReader", "TableRegistry", "TableConfig"]
