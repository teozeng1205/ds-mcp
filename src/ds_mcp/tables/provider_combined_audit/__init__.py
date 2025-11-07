"""Provider Combined Audit table exports built via the blueprint helper."""

from __future__ import annotations

from ds_mcp.core.registry import TableRegistry
from ds_mcp.tables.base import (
    PartitionGuardrail,
    TableBlueprint,
    export_tools,
)

from . import tools


class ProviderCombinedAuditTable(TableBlueprint):
    slug = "provider"
    schema_name = "monitoring"
    table_name = "provider_combined_audit"
    database_name = "prod"
    display_name = "Provider Combined Audit"
    description = (
        "Audit trail for provider-level monitoring combining issue signals and contextual metadata."
    )
    query_tool_name = "query_audit"
    macros = tools.MACROS
    sql_tools = tools.SQL_TOOL_SPECS
    partition_guardrail = PartitionGuardrail(
        column="sales_date",
        hint=(
            "Add a WHERE clause on sales_date (e.g., sales_date >= CAST(TO_CHAR(CURRENT_DATE - 7, 'YYYYMMDD') AS INT))."
        ),
    )


TABLE = ProviderCombinedAuditTable().build()
TABLE_NAME = TABLE.definition.full_table_name()

export_tools(TABLE, globals())


def register_table(registry: TableRegistry) -> None:
    """Register the table definition with *registry*."""
    TABLE.register(registry)


__all__ = ["TABLE_NAME", "TABLE", "register_table", *sorted(TABLE.tools)]
