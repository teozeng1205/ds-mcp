"""
Provider Combined Audit table module.

Provides MCP tools for querying and analyzing the monitoring_prod.provider_combined_audit table.
"""

from ds_mcp.core.registry import TableRegistry
from ds_mcp.tables.provider_combined_audit.config import get_table_config
from ds_mcp.tables.provider_combined_audit.tools import (
    query_audit,
    get_table_schema,
    get_row_count,
    get_distinct_values,
    get_recent_events,
    get_overview_today,
    summarize_provider_status,
    summarize_issues_today,
    summarize_issue_impact,
)

__all__ = [
    "register_table",
    "query_audit",
    "get_table_schema",
    "get_row_count",
    "get_distinct_values",
    "get_recent_events",
    "get_overview_today",
    "summarize_provider_status",
    "summarize_issues_today",
    "summarize_issue_impact",
]


def register_table(registry: TableRegistry) -> None:
    """
    Register the monitoring_prod.provider_combined_audit table with the registry.

    Args:
        registry: TableRegistry instance
    """
    config = get_table_config()
    registry.register_table(config)
