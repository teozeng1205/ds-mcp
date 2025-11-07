"""Market Level Anomalies table tools defined via the blueprint helper."""

from __future__ import annotations

from ds_mcp.core.registry import TableRegistry
from ds_mcp.tables.base import (
    PartitionGuardrail,
    TableBlueprint,
    export_tools,
)

from . import tools


class MarketLevelAnomaliesTable(TableBlueprint):
    slug = "anomalies"
    schema_name = "analytics"
    table_name = "market_level_anomalies_v3"
    display_name = "Market Level Anomalies V3"
    description = "Market-level pricing anomalies with impact scores and competitive position data."
    query_tool_name = "query_anomalies"
    default_limit = 100
    macros = tools.MACROS
    sql_tools = tools.SQL_TOOL_SPECS
    partition_guardrail = PartitionGuardrail(
        column="sales_date",
        hint="Include a sales_date predicate (e.g., sales_date = CAST(TO_CHAR(CURRENT_DATE, 'YYYYMMDD') AS INT)).",
    )


TABLE = MarketLevelAnomaliesTable().build()
TABLE_NAME = TABLE.definition.full_table_name()

export_tools(TABLE, globals())


def register_table(registry: TableRegistry) -> None:
    """Register the table definition with *registry*."""
    TABLE.register(registry)


__all__ = ["TABLE_NAME", "TABLE", "register_table", *sorted(TABLE.tools)]
