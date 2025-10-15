"""
Database connector classes for DS-MCP.

Provides reusable database connection classes for Redshift/Analytics databases.
"""

import sys
import os
import logging

# Add parent directory to path to import threevictors
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))

from threevictors.dao import redshift_connector

log = logging.getLogger(__name__)


class AnalyticsReader(redshift_connector.RedshiftConnector):
    """
    Redshift/Analytics database reader.

    This class provides read-only access to the Analytics Redshift database
    using the threevictors library for connection management.
    """

    def get_properties_filename(self):
        """Return the properties file for Analytics Redshift connection."""
        return "database-analytics-redshift-serverless-reader.properties"


class DatabaseConnectorFactory:
    """
    Factory for creating database connectors.

    This allows for easy extension to support multiple database types
    (MySQL, Postgres, etc.) in the future.
    """

    _connectors = {}

    @classmethod
    def get_connector(cls, connector_type: str = "analytics"):
        """
        Get or create a database connector.

        Args:
            connector_type: Type of connector ('analytics', 'mysql', etc.)

        Returns:
            Database connector instance
        """
        if connector_type not in cls._connectors:
            if connector_type == "analytics":
                log.info(f"Creating new {connector_type} connector")
                cls._connectors[connector_type] = AnalyticsReader()
            else:
                raise ValueError(f"Unknown connector type: {connector_type}")

        return cls._connectors[connector_type]

    @classmethod
    def close_all(cls):
        """Close all database connections."""
        for name, connector in cls._connectors.items():
            try:
                connector.close()
                log.info(f"Closed {name} connector")
            except Exception as e:
                log.error(f"Error closing {name} connector: {e}")
        cls._connectors.clear()
