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
            table_name: Full table name (e.g., 'prod.monitoring.provider_combined_audit')
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

    def get_top_site_issues(self, target_date: str | None = None) -> pd.DataFrame:
        """
        Get top site issues for today and compare with last week and last month.

        Args:
            target_date: Date in YYYYMMDD format (default: today)

        Returns:
            DataFrame with issue_sources, issue_reasons, and counts for today, last week, last month
        """
        import datetime

        if target_date is None:
            target_date = datetime.date.today().strftime("%Y%m%d")

        # Parse target date
        target = datetime.datetime.strptime(str(target_date), "%Y%m%d").date()
        last_week = (target - datetime.timedelta(days=7)).strftime("%Y%m%d")
        last_month = (target - datetime.timedelta(days=30)).strftime("%Y%m%d")

        query = f"""
        WITH today_issues AS (
            SELECT
                issue_sources,
                issue_reasons,
                sitecode,
                COUNT(*) as today_count
            FROM prod.monitoring.provider_combined_audit
            WHERE sales_date = {target_date}
              AND issue_sources IS NOT NULL
              AND issue_reasons IS NOT NULL
            GROUP BY issue_sources, issue_reasons, sitecode
        ),
        last_week_issues AS (
            SELECT
                issue_sources,
                issue_reasons,
                sitecode,
                COUNT(*) as last_week_count
            FROM prod.monitoring.provider_combined_audit
            WHERE sales_date = {last_week}
              AND issue_sources IS NOT NULL
              AND issue_reasons IS NOT NULL
            GROUP BY issue_sources, issue_reasons, sitecode
        ),
        last_month_issues AS (
            SELECT
                issue_sources,
                issue_reasons,
                sitecode,
                COUNT(*) as last_month_count
            FROM prod.monitoring.provider_combined_audit
            WHERE sales_date = {last_month}
              AND issue_sources IS NOT NULL
              AND issue_reasons IS NOT NULL
            GROUP BY issue_sources, issue_reasons, sitecode
        )
        SELECT
            COALESCE(t.sitecode, lw.sitecode, lm.sitecode) as sitecode,
            COALESCE(t.issue_sources, lw.issue_sources, lm.issue_sources) as issue_sources,
            COALESCE(t.issue_reasons, lw.issue_reasons, lm.issue_reasons) as issue_reasons,
            COALESCE(t.today_count, 0) as today_count,
            COALESCE(lw.last_week_count, 0) as last_week_count,
            COALESCE(lm.last_month_count, 0) as last_month_count,
            COALESCE(t.today_count, 0) - COALESCE(lw.last_week_count, 0) as week_over_week_change,
            COALESCE(t.today_count, 0) - COALESCE(lm.last_month_count, 0) as month_over_month_change
        FROM today_issues t
        FULL OUTER JOIN last_week_issues lw
            ON t.sitecode = lw.sitecode
            AND t.issue_sources = lw.issue_sources
            AND t.issue_reasons = lw.issue_reasons
        FULL OUTER JOIN last_month_issues lm
            ON COALESCE(t.sitecode, lw.sitecode) = lm.sitecode
            AND COALESCE(t.issue_sources, lw.issue_sources) = lm.issue_sources
            AND COALESCE(t.issue_reasons, lw.issue_reasons) = lm.issue_reasons
        WHERE COALESCE(t.today_count, lw.last_week_count, lm.last_month_count) > 0
        ORDER BY today_count DESC
        LIMIT 50;
        """

        log.info(f"Getting top site issues for date: {target_date}")
        with self.get_connection().cursor() as cursor:
            cursor.execute(query)
            colnames = [desc[0] for desc in cursor.description]
            records = cursor.fetchall()
            df = pd.DataFrame(records, columns=colnames)
            log.info(f"Found {len(df)} issue combinations")
            return df

    def analyze_issue_scope(
        self,
        providercode: str | None = None,
        sitecode: str | None = None,
        target_date: str | None = None,
        lookback_days: int = 7
    ) -> pd.DataFrame:
        """
        Analyze the scope of issues for providers and/or sites.

        Args:
            providercode: Provider code(s) - single code (e.g., 'QL2') or comma-separated (e.g., 'QL2,Atlas')
            sitecode: Site code(s) - single code (e.g., 'QF') or comma-separated (e.g., 'QF,DY')
            target_date: Date in YYYYMMDD format (default: today)
            lookback_days: Number of days to analyze (default: 7)

        Returns:
            DataFrame with issue breakdown by multiple dimensions
        """
        import datetime

        if target_date is None:
            target_date = datetime.date.today().strftime("%Y%m%d")

        # Parse target date and calculate lookback
        target = datetime.datetime.strptime(str(target_date), "%Y%m%d").date()
        start_date = (target - datetime.timedelta(days=lookback_days)).strftime("%Y%m%d")

        # Build WHERE clause dynamically
        where_clauses = []

        if providercode:
            # Handle multiple providers
            providers = [p.strip() for p in providercode.split(',')]
            if len(providers) == 1:
                where_clauses.append(f"providercode = '{providers[0]}'")
            else:
                provider_list = "', '".join(providers)
                where_clauses.append(f"providercode IN ('{provider_list}')")

        if sitecode:
            # Handle multiple sites
            sites = [s.strip() for s in sitecode.split(',')]
            if len(sites) == 1:
                where_clauses.append(f"sitecode = '{sites[0]}'")
            else:
                site_list = "', '".join(sites)
                where_clauses.append(f"sitecode IN ('{site_list}')")

        where_clauses.append(f"sales_date BETWEEN {start_date} AND {target_date}")
        where_clauses.append("(issue_sources IS NOT NULL OR filterreason IS NOT NULL)")

        where_clause = " AND ".join(where_clauses)

        query = f"""
        SELECT
            providercode,
            sitecode,
            pos,
            triptype,
            los,
            cabin,
            originairportcode,
            destinationairportcode,
            origincitycode,
            destinationcitycode,
            origincountrycode,
            destinationcountrycode,
            departdate,
            EXTRACT(DOW FROM TO_DATE(CAST(departdate AS VARCHAR), 'YYYYMMDD')) as depart_dow,
            DATE_PART('hour', observationtimestamp) as observation_hour,
            issue_sources,
            issue_reasons,
            response_statuses,
            filterreason,
            COUNT(*) as issue_count,
            COUNT(DISTINCT sales_date) as days_with_issues,
            MIN(sales_date) as first_seen_date,
            MAX(sales_date) as last_seen_date
        FROM prod.monitoring.provider_combined_audit
        WHERE {where_clause}
        GROUP BY
            providercode, sitecode, pos, triptype, los, cabin,
            originairportcode, destinationairportcode,
            origincitycode, destinationcitycode,
            origincountrycode, destinationcountrycode,
            departdate, depart_dow, observation_hour,
            issue_sources, issue_reasons, response_statuses, filterreason
        ORDER BY issue_count DESC
        LIMIT 100;
        """

        log.info(f"Analyzing issue scope for provider={providercode}, site={sitecode}, date={target_date}")
        with self.get_connection().cursor() as cursor:
            cursor.execute(query)
            colnames = [desc[0] for desc in cursor.description]
            records = cursor.fetchall()
            df = pd.DataFrame(records, columns=colnames)
            log.info(f"Found {len(df)} dimensional breakdowns")
            return df


__all__ = ["AnalyticsReader"]
