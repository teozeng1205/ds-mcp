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
            table_name: Full table name (e.g., 'price_anomalies.anomaly_table').
                       For cross-database queries (e.g., 'prod.monitoring.table'),
                       note that information_schema only shows tables in the current database.
                       Use read_table_head() or query_table() for cross-database access.

        Returns:
            dict with table metadata
        """
        # Parse table name - handle both 2-part and 3-part names
        parts = table_name.split('.')
        if len(parts) == 3:
            # database.schema.table format
            schema = parts[1]
            table = parts[2]
        elif len(parts) == 2:
            # schema.table format
            schema = parts[0]
            table = parts[1]
        else:
            return {"error": f"Invalid table name format: {table_name}. Use 'schema.table' or 'database.schema.table'"}

        query = f"""
        SELECT
            table_schema,
            table_name,
            table_type
        FROM information_schema.tables
        WHERE table_schema = '{schema}'
          AND table_name = '{table}'
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
            table_name: Full table name (e.g., 'price_anomalies.anomaly_table').
                       For cross-database queries (e.g., 'prod.monitoring.table'),
                       note that information_schema only shows tables in the current database.
                       Use read_table_head() or query_table() for cross-database access.

        Returns:
            DataFrame with column information
        """
        # Parse table name - handle both 2-part and 3-part names
        parts = table_name.split('.')
        if len(parts) == 3:
            # database.schema.table format
            schema = parts[1]
            table = parts[2]
        elif len(parts) == 2:
            # schema.table format
            schema = parts[0]
            table = parts[1]
        else:
            raise ValueError(f"Invalid table name format: {table_name}. Use 'schema.table' or 'database.schema.table'")

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
