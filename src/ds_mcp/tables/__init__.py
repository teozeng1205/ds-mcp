"""
Table-specific modules for DS-MCP.

Each table has its own module with configuration, tools, and queries.
To add a new table, create a new directory here with config.py, tools.py, and queries.py.
"""

from ds_mcp.core.registry import TableRegistry
from ds_mcp.tables.market_anomalies_v3.config import get_table_config

__all__ = ["register_all_tables"]


def register_all_tables(registry: TableRegistry) -> None:
    """
    Register all available tables with the registry.

    This function is called by the server to load all table configurations.
    Add new table registrations here as you create new table modules.

    Args:
        registry: TableRegistry instance to register tables with
    """
    # Register market_level_anomalies_v3 table
    from ds_mcp.tables.market_anomalies_v3 import register_table as register_market_anomalies
    register_market_anomalies(registry)

    # Add future table registrations here:
    # from ds_mcp.tables.another_table import register_table as register_another_table
    # register_another_table(registry)
