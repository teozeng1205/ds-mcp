"""
DS-MCP: Data Science Model Context Protocol Server

A scalable MCP server framework for exposing database tables as AI-accessible tools.
"""

__version__ = "2.0.0"

from ds_mcp.core.connectors import AnalyticsReader
from ds_mcp.core.registry import TableRegistry

__all__ = ["AnalyticsReader", "TableRegistry", "__version__"]
