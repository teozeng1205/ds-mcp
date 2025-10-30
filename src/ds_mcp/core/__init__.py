"""
Core functionality for DS-MCP.

This re-export keeps the public surface small and intentionally focused on the
pieces users customise most often.
"""

from ds_mcp.core.connectors import (
    AnalyticsReader,
    ConnectorRegistry,
    get_connector,
    register_connector,
)
from ds_mcp.core.registry import TableConfig, TableRegistry

__all__ = [
    "AnalyticsReader",
    "ConnectorRegistry",
    "TableConfig",
    "TableRegistry",
    "get_connector",
    "register_connector",
]
