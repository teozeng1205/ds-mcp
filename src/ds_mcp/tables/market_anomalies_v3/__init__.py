"""
Market Level Anomalies V3 table module.

Provides MCP tools for querying and analyzing the analytics.market_level_anomalies_v3 table.
"""

from ds_mcp.core.registry import TableRegistry
from ds_mcp.tables.market_anomalies_v3.config import get_table_config
from ds_mcp.tables.market_anomalies_v3.tools import (
    query_anomalies,
    get_table_schema,
    get_available_customers,
)

__all__ = ["register_table", "query_anomalies", "get_table_schema", "get_available_customers"]


def register_table(registry: TableRegistry) -> None:
    """
    Register the market_level_anomalies_v3 table with the registry.

    Args:
        registry: TableRegistry instance
    """
    config = get_table_config()
    registry.register_table(config)
