"""
Database connectors for DS-MCP using threevictors.dao.

Provides AnalyticsReader that wraps RedshiftConnector for querying analytics database.
"""

from __future__ import annotations

import logging
import pandas as pd
from threevictors.dao import redshift_connector

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)s [%(name)s] %(message)s')
stream_handler.setFormatter(formatter)
log.addHandler(stream_handler)
log.propagate = False


class AnalyticsReader(redshift_connector.RedshiftConnector):
    """
    Analytics database reader using Redshift connector.

    Provides connection management and query execution for analytics.* tables.
    """

    def __init__(self):
        log.info("Initializing AnalyticsReader")
        super().__init__()
        log.info("AnalyticsReader initialized successfully")

    def get_properties_filename(self):
        """Properties file for Redshift connection configuration."""
        return "database-analytics-redshift-serverless-reader.properties"

    def describe_table(self, table_name: str) -> dict:
        """
        Get metadata and key information about a table.

        Args:
            table_name: Full table name (e.g., 'monitoring.provider_combined_audit')

        Returns:
            dict with table metadata
        """
        query = f"""
        SELECT
            table_schema,
            table_name,
            table_type
        FROM information_schema.tables
        WHERE table_schema || '.' || table_name = '{table_name}'
        LIMIT 1;
        """

        with self.get_connection().cursor() as cursor:
            cursor.execute(query)
            colnames = [desc[0] for desc in cursor.description]
            records = cursor.fetchall()

            if not records:
                return {"error": f"Table {table_name} not found"}

            df = pd.DataFrame(records, columns=colnames)
            return df.to_dict(orient='records')[0]

    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        """
        Get full column information for a table.

        Args:
            table_name: Full table name (e.g., 'monitoring.provider_combined_audit')

        Returns:
            DataFrame with column information
        """
        schema, table = table_name.split('.')

        query = f"""
        SELECT
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
          AND table_name = '{table}'
        ORDER BY ordinal_position;
        """

        with self.get_connection().cursor() as cursor:
            cursor.execute(query)
            colnames = [desc[0] for desc in cursor.description]
            records = cursor.fetchall()
            df = pd.DataFrame(records, columns=colnames)
            return df

    def read_table_head(self, table_name: str, limit: int = 50) -> pd.DataFrame:
        """
        Get data preview (first N rows) from a table.

        Args:
            table_name: Full table name (e.g., 'monitoring.provider_combined_audit')
            limit: Number of rows to return (default: 50)

        Returns:
            DataFrame with first N rows
        """
        query = f"""
        SELECT *
        FROM {table_name}
        LIMIT {limit};
        """

        with self.get_connection().cursor() as cursor:
            cursor.execute(query)
            colnames = [desc[0] for desc in cursor.description]
            records = cursor.fetchall()
            df = pd.DataFrame(records, columns=colnames)
            return df

    def query_table(self, query: str, limit: int = 1000) -> pd.DataFrame:
        """
        Execute a SELECT query on the database.

        Args:
            query: SQL SELECT statement
            limit: Maximum rows to return (default: 1000, safety limit)

        Returns:
            DataFrame with query results
        """
        # Ensure it's a SELECT query for safety
        if not query.strip().upper().startswith('SELECT'):
            raise ValueError("Only SELECT queries are allowed")

        # Add LIMIT if not present
        if 'LIMIT' not in query.upper():
            query = query.rstrip(';') + f' LIMIT {limit};'

        log.info(f"Executing query: {query[:100]}...")

        with self.get_connection().cursor() as cursor:
            cursor.execute(query)
            colnames = [desc[0] for desc in cursor.description]
            records = cursor.fetchall()
            df = pd.DataFrame(records, columns=colnames)

            log.info(f"Query returned {len(df)} rows")
            return df


__all__ = ["AnalyticsReader"]
