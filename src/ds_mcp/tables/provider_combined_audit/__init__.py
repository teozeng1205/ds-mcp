"""
Provider Combined Audit table module.

Provides MCP tools for querying and analyzing the monitoring_prod.provider_combined_audit table.
"""

from ds_mcp.core.registry import TableRegistry
from ds_mcp.tables.provider_combined_audit.config import get_table_config
from ds_mcp.tables.provider_combined_audit.tools import (
    query_audit,
    get_table_schema,
    top_site_issues,
    issue_scope_quick_by_site,
    issue_scope_by_site_dimensions,
)

__all__ = [
    "register_table",
    "query_audit",
    "get_table_schema",
    "top_site_issues",
    "issue_scope_quick_by_site",
    "issue_scope_by_site_dimensions",
]


def register_table(registry: TableRegistry) -> None:
    """
    Register the monitoring_prod.provider_combined_audit table with the registry.

    Args:
        registry: TableRegistry instance
    """
    config = get_table_config()
    registry.register_table(config)
