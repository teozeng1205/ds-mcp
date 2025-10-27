"""
Table registry system for DS-MCP.

Provides a centralized registry for managing multiple database tables
and their associated MCP tools.
"""

import logging
from typing import Dict, List, Callable, Optional, Any
from dataclasses import dataclass, field

log = logging.getLogger(__name__)


@dataclass
class TableConfig:
    """
    Configuration for a database table exposed via MCP.

    Attributes:
        name: Full table name (e.g., 'analytics.market_level_anomalies_v3')
        display_name: Human-readable name for the table
        description: Description of the table and its purpose
        database_name: Optional database/catalog name (for cross-database tables)
        schema_name: Database schema name
        table_name: Table name without schema
        connector_type: Type of database connector to use
        tools: List of tool functions to register for this table
        metadata: Additional metadata about the table
    """
    name: str
    display_name: str
    description: str
    schema_name: str
    table_name: str
    database_name: str | None = None
    connector_type: str = "analytics"
    tools: List[Callable] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def full_name(self) -> str:
        """Return the full table name (schema.table)."""
        if self.database_name:
            return f"{self.database_name}.{self.schema_name}.{self.table_name}"
        return f"{self.schema_name}.{self.table_name}"


class TableRegistry:
    """
    Registry for managing database tables and their MCP tools.

    This class maintains a registry of all tables available via the MCP server
    and provides methods to register tables and retrieve their configurations.
    """

    def __init__(self):
        self._tables: Dict[str, TableConfig] = {}
        log.info("Initialized TableRegistry")

    def register_table(self, config: TableConfig) -> None:
        """
        Register a table configuration.

        Args:
            config: TableConfig instance with table metadata and tools
        """
        if config.name in self._tables:
            log.warning(f"Table {config.name} already registered, overwriting")

        self._tables[config.name] = config
        log.info(f"Registered table: {config.name} with {len(config.tools)} tools")

    def get_table(self, name: str) -> Optional[TableConfig]:
        """
        Get a table configuration by name.

        Args:
            name: Table name (full name or table name only)

        Returns:
            TableConfig if found, None otherwise
        """
        return self._tables.get(name)

    def get_all_tables(self) -> List[TableConfig]:
        """
        Get all registered table configurations.

        Returns:
            List of all registered TableConfig instances
        """
        return list(self._tables.values())

    def get_all_tools(self) -> List[Callable]:
        """
        Get all tools from all registered tables.

        Returns:
            List of all tool functions
        """
        tools = []
        for table in self._tables.values():
            tools.extend(table.tools)
        return tools

    def list_tables(self) -> List[str]:
        """
        Get names of all registered tables.

        Returns:
            List of table names
        """
        return list(self._tables.keys())

    def clear(self) -> None:
        """Clear all registered tables."""
        self._tables.clear()
        log.info("Cleared TableRegistry")

    def __len__(self) -> int:
        """Return the number of registered tables."""
        return len(self._tables)

    def __contains__(self, name: str) -> bool:
        """Check if a table is registered."""
        return name in self._tables
