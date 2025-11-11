"""
DS-MCP: Data Science Model Context Protocol Server

A scalable MCP server framework for database exploration and AI-accessible tools.
Provides AnalyticsReader for Redshift analytics database access.
"""

from ds_mcp.core.connectors import AnalyticsReader

__version__ = "2.0.0"

__all__ = ["AnalyticsReader", "__version__"]
