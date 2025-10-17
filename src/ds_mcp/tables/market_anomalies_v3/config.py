"""
Configuration for market_level_anomalies_v3 table.

Defines table metadata, schema information, and tool registration.
"""

from ds_mcp.core.registry import TableConfig


def get_table_config() -> TableConfig:
    """
    Get the configuration for the market_level_anomalies_v3 table.

    Returns:
        TableConfig instance with table metadata and tools
    """
    from ds_mcp.tables.market_anomalies_v3 import tools

    config = TableConfig(
        name="analytics.market_level_anomalies_v3",
        display_name="Market Level Anomalies V3",
        description="Market-level pricing anomalies with impact scores and competitive position data",
        schema_name="analytics",
        table_name="market_level_anomalies_v3",
        connector_type="analytics",
        tools=[
            tools.query_anomalies,
            tools.get_table_schema,
            tools.get_available_customers,
            tools.overview_anomalies_today,
        ],
        metadata={
            "version": "3.0",
            "primary_key": ["customer", "sales_date", "seg_mkt"],
            "date_range": "2025-09-14 to 2025-10-14",
            "customers": ["AS", "SK", "B6", "INS"],
            "total_records": "~9.4 million",
            "key_metrics": [
                "impact_score",
                "any_anomaly",
                "freq_pcnt_val",
                "mag_pcnt_val",
                "revenue_score"
            ],
            "dimensions": [
                "customer",
                "sales_date",
                "seg_mkt",
                "cp",
                "region_name",
                "cabin_group"
            ]
        }
    )

    return config


# Schema information for reference
TABLE_SCHEMA = {
    "columns": [
        {"name": "customer", "type": "VARCHAR", "description": "Customer code (AS, SK, B6, INS)"},
        {"name": "sales_date", "type": "INTEGER", "description": "Date in YYYYMMDD format"},
        {"name": "mkt", "type": "VARCHAR", "description": "Market code (e.g., BOS-ATL)"},
        {"name": "seg", "type": "VARCHAR", "description": "Segment description"},
        {"name": "seg_mkt", "type": "VARCHAR", "description": "Combined segment:market"},
        {"name": "cp", "type": "VARCHAR", "description": "Competitive position"},
        {"name": "region_name", "type": "VARCHAR", "description": "Geographic region"},
        {"name": "cabin_group", "type": "VARCHAR", "description": "Economy or Premium"},
        {"name": "any_anomaly", "type": "INTEGER", "description": "1 if anomaly detected"},
        {"name": "freq_pcnt_anomaly", "type": "INTEGER", "description": "Frequency anomaly flag"},
        {"name": "mag_pcnt_anomaly", "type": "INTEGER", "description": "Magnitude % anomaly flag"},
        {"name": "mag_nominal_anomaly", "type": "INTEGER", "description": "Magnitude nominal anomaly flag"},
        {"name": "freq_pcnt_val", "type": "DOUBLE PRECISION", "description": "Frequency % value (0-1)"},
        {"name": "mag_pcnt_val", "type": "DOUBLE PRECISION", "description": "Magnitude % value"},
        {"name": "mag_nominal_val", "type": "DOUBLE PRECISION", "description": "Magnitude nominal value ($)"},
        {"name": "impact_score", "type": "DOUBLE PRECISION", "description": "Primary impact score"},
        {"name": "revenue_score", "type": "DOUBLE PRECISION", "description": "Revenue importance (0-1)"},
    ]
}
