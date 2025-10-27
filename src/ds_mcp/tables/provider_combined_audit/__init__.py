"""
Provider Combined Audit table module.

Provides MCP tools for querying and analyzing the prod.monitoring.provider_combined_audit table.
"""

from ds_mcp.core.registry import TableRegistry
from ds_mcp.tables.provider_combined_audit.config import get_table_config

__all__ = ["register_table"]


def register_table(registry: TableRegistry) -> None:
    """
    Register the prod.monitoring.provider_combined_audit table with the registry.

    Args:
        registry: TableRegistry instance
    """
    config = get_table_config()
    registry.register_table(config)
