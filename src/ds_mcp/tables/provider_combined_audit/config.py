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
            # Essential, comprehensive set
            tools.get_overview_today,
            tools.summarize_provider_status,
            tools.summarize_issues_today,
            tools.summarize_issue_impact,
            tools.get_rows_by_sales_date,
            tools.get_top_by_dimension,
            tools.get_date_range,
            tools.get_table_schema,
            tools.query_audit,
        ],
        metadata={
            "primary_key": "unknown",
            "notes": "Use get_table_schema to see actual columns and types",
        },
    )

    return config
