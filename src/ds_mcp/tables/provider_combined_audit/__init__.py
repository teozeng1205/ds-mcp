"""Provider Combined Audit table exports built via the new table builder."""

from __future__ import annotations

from ds_mcp.core.registry import TableRegistry
from ds_mcp.tables.base import build_table, export_tools

from . import tools

TABLE = build_table(
    slug="provider",
    schema_name="monitoring",
    table_name="provider_combined_audit",
    database_name="prod",
    display_name="Provider Combined Audit",
    description=(
        "Audit trail for provider-level monitoring combining issue signals and contextual metadata."
    ),
    query_tool_name="query_audit",
    default_limit=200,
    macros=tools.MACROS,
    sql_tools=tools.SQL_TOOL_SPECS,
)

TABLE_NAME = TABLE.definition.full_table_name()

export_tools(TABLE, globals())


def register_table(registry: TableRegistry) -> None:
    """Register the table definition with *registry*."""
    TABLE.register(registry)


__all__ = ["TABLE_NAME", "TABLE", "register_table", *sorted(TABLE.tools)]
