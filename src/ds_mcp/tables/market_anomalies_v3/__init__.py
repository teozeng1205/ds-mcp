"""Market Level Anomalies table tools built via the simplified table builder."""

from __future__ import annotations

from ds_mcp.core.registry import TableRegistry
from ds_mcp.tables.base import build_table, export_tools

from . import tools

TABLE = build_table(
    slug="anomalies",
    schema_name="analytics",
    table_name="market_level_anomalies_v3",
    display_name="Market Level Anomalies V3",
    description="Market-level pricing anomalies with impact scores and competitive position data.",
    query_tool_name="query_anomalies",
    default_limit=100,
    macros=tools.MACROS,
    sql_tools=tools.SQL_TOOL_SPECS,
)

TABLE_NAME = TABLE.definition.full_table_name()

# Export generated MCP tools (query/schema/custom SQL helpers) directly at the module level.
export_tools(TABLE, globals())


def register_table(registry: TableRegistry) -> None:
    """Register the table definition with *registry*."""
    TABLE.register(registry)


__all__ = ["TABLE_NAME", "TABLE", "register_table", *sorted(TABLE.tools)]
