"""
Configuration for monitoring_prod.provider_combined_audit table.

Defines table metadata and tool registration.
"""

from ds_mcp.core.registry import TableConfig


def get_table_config() -> TableConfig:
    """
    Get the configuration for the monitoring_prod.provider_combined_audit table.

    Returns:
        TableConfig instance with table metadata and tools
    """
    from ds_mcp.tables.provider_combined_audit import tools

    config = TableConfig(
        name="monitoring_prod.provider_combined_audit",
        display_name="Provider Combined Audit",
        description=(
            "Audit trail for provider-level monitoring. Contains historical events/changes "
            "to combined provider monitoring data."
        ),
        schema_name="monitoring_prod",
        table_name="provider_combined_audit",
        connector_type="analytics",
        tools=[
            tools.query_audit,
            tools.get_table_schema,
            tools.top_site_issues,
            tools.list_provider_sites,
            tools.issue_scope_quick_by_site,
            tools.issue_scope_by_site_dimensions,
            tools.issue_scope_combined,
            tools.overview_site_issues_today,
        ],
        metadata={
            "primary_key": "unknown",
            "notes": "Use get_table_schema to see actual columns and types",
        },
    )

    return config
