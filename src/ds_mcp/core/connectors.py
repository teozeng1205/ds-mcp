"""
Database connectors for DS-MCP.

Provides AnalyticsReader class for querying Redshift analytics database.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from threevictors.dao import redshift_connector

log = logging.getLogger("AnalyticsReader")
log.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)s [%(name)s] %(message)s')
stream_handler.setFormatter(formatter)
log.addHandler(stream_handler)
log.propagate = False


class AnalyticsReader(redshift_connector.RedshiftConnector):
    """
    Reader for analytics Redshift database using threevictors connector.

    Extends RedshiftConnector from threevictors.dao to provide domain-specific
    query methods for analytics tables (market anomalies, revenue scores, etc.).
    """

    def __init__(self) -> None:
        """Initialize AnalyticsReader and connect to Redshift."""
        log.info("Initializing AnalyticsReader")
        super().__init__()
        log.info("AnalyticsReader initialized")

    def get_properties_filename(self) -> str:
        """Get the properties file for database configuration."""
        return "database-analytics-redshift-serverless-reader.properties"

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute a SQL query and return results as DataFrame.

        Args:
            query: SQL query string to execute

        Returns:
            pandas DataFrame with query results
        """
        log.info(f"Executing query: {query[:100]}...")
        with self.get_connection().cursor() as cursor:
            cursor.execute(query)
            colnames = [desc[0] for desc in cursor.description]
            records = cursor.fetchall()
            df = pd.DataFrame(records, columns=colnames)
            log.info(f"Query returned {len(df)} rows")
            return df

    def get_provider_combined_audit(
        self,
        sales_date: int,
        limit: int = 10
    ) -> pd.DataFrame:
        """
        Query provider_combined_audit table.

        Args:
            sales_date: Date in YYYYMMDD format (partition key)
            limit: Maximum rows to return

        Returns:
            pandas DataFrame with audit data
        """
        query = f"""
        SELECT *
        FROM monitoring.provider_combined_audit
        WHERE sales_date = {sales_date}
        LIMIT {limit}
        """
        return self.execute_query(query)

    def get_market_anomalies_df(
        self,
        start_date: int,
        end_date: int,
        customer: str
    ) -> pd.DataFrame:
        """
        Get market anomalies data.

        Args:
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            customer: Customer name to filter by

        Returns:
            pandas DataFrame with anomalies data
        """
        query = f"""
        SELECT sales_date, customer, metro_market AS mkt, segment_name AS seg, region_name,
               depart_period, carrier_group, cabin_group, top_offenders, impacted_dates,
               ROUND(impacted_dates_percentage, 2) AS impact_dates_pcnt,
               itinerary_count, segment_name || ':' || metro_market AS seg_mkt,
               competitive_position,
               ROUND(itinerary_percentage, 2) AS freq_pcnt,
               ROUND(avg_diff_min_ow, 2) AS mag_nominal,
               ROUND(avg_pcnt_diff_min_ow, 2) AS mag_pcnt
        FROM analytics.market_level_anomalies
        WHERE sales_date >= {start_date}
          AND sales_date <= {end_date}
          AND customer = '{customer}'
        """

        df = self.execute_query(query)

        # Process dates
        if not df.empty and 'sales_date' in df.columns:
            df["sales_date"] = pd.to_datetime(df["sales_date"].astype(str), format="%Y%m%d")
            df["dow"] = df["sales_date"].dt.strftime("%A").str[:3]
            df["sales_date"] = df["sales_date"].dt.strftime("%Y%m%d").astype(int)

        return df

    def get_oag_scores(self, customer: str) -> pd.DataFrame:
        """
        Get OAG scores for a customer.

        Args:
            customer: Customer name

        Returns:
            pandas DataFrame with OAG scores
        """
        query = f"""
        SELECT *
        FROM analytics.oag_score_v2
        WHERE run_date = (
            SELECT MAX(run_date)
            FROM analytics.oag_score_v2
        ) AND customer = '{customer}'
        """
        return self.execute_query(query)

    def get_revenue_scores(
        self,
        customer: str,
        sales_date: int | None = None
    ) -> pd.DataFrame:
        """
        Get revenue scores for a customer.

        Args:
            customer: Customer name
            sales_date: Optional specific sales date (uses max if not provided)

        Returns:
            pandas DataFrame with revenue scores
        """
        query = f"""
        SELECT customer, ap_band, origin_metro, destination_metro, cabin_group,
               midt_pax, selected_fare, estimated_revenue, revenue_score
        FROM analytics.revenue_score_v1
        WHERE sales_date = (
            SELECT MAX(sales_date)
            FROM analytics.revenue_score_v1
        )
          AND customer = '{customer}'
        """
        return self.execute_query(query)

    def get_impact_score_weights(self, customer: str | None = None) -> pd.DataFrame:
        """
        Retrieve impact score weights from analytics.anomalies_impact_score_weights.

        Falls back to '*' (all customers) if specific customer weights not found.

        Args:
            customer: Optional customer name (uses '*' default if not found)

        Returns:
            pandas DataFrame with impact score weights
        """
        query = """
        SELECT customer, dimension, weights
        FROM analytics.anomalies_impact_score_weights
        """

        df = self.execute_query(query)

        if df.empty:
            log.warning("No weights found in analytics.anomalies_impact_score_weights")
            return df

        # If customer is specified, try to get customer-specific weights first
        if customer:
            customer_weights = df[df['customer'] == customer]
            if not customer_weights.empty:
                log.info(f"Found weights for customer: {customer}")
                return customer_weights

        # Fall back to '*' (all customers) weights
        default_weights = df[df['customer'] == '*']
        if not default_weights.empty:
            log.info("Using default weights ('*')")
            return default_weights

        # If no weights found, return empty dataframe
        log.warning(f"No weights found for customer {customer} or default '*'")
        return df
