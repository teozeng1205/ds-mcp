"""
Table-specific modules for DS-MCP.

Each table has its own module with configuration, tools, and queries.
To add a new table, create a new directory here with config.py, tools.py, and queries.py.
"""

from ds_mcp.core.registry import TableRegistry

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
    from ds_mcp.tables.market_anomalies_v3 import (
        register_table as register_market_anomalies,
    )
    register_market_anomalies(registry)

    # Register prod.monitoring.provider_combined_audit table
    try:
        from ds_mcp.tables.provider_combined_audit import (
            register_table as register_provider_audit,
        )

        register_provider_audit(registry)
    except Exception as e:
        # Don't crash server if optional tables fail to import
        import logging

        logging.getLogger(__name__).warning(
            f"Failed to register provider_combined_audit: {e}"
        )
