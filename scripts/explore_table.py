#!/usr/bin/env python3
"""
Script to explore analytics.market_level_anomalies_v3 table
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from threevictors.dao import redshift_connector
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 50)


class AnalyticsReader(redshift_connector.RedshiftConnector):
    def get_properties_filename(self):
        return "database-analytics-redshift-serverless-reader.properties"


def main():
    print("=" * 80)
    print("Exploring analytics.market_level_anomalies_v3")
    print("=" * 80)

    reader = AnalyticsReader()

    # 1. Get table structure
    print("\n1. TABLE STRUCTURE:")
    print("-" * 80)
    query_structure = """
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_schema = 'analytics'
      AND table_name = 'market_level_anomalies_v3'
    ORDER BY ordinal_position;
    """

    with reader.get_connection().cursor() as cursor:
        cursor.execute(query_structure)
        colnames = [desc[0] for desc in cursor.description]
        records = cursor.fetchall()
        df_structure = pd.DataFrame(records, columns=colnames)
        print(df_structure.to_string(index=False))

    # 2. Get row count
    print("\n\n2. ROW COUNT:")
    print("-" * 80)
    query_count = "SELECT COUNT(*) as total_rows FROM analytics.market_level_anomalies_v3;"

    with reader.get_connection().cursor() as cursor:
        cursor.execute(query_count)
        count = cursor.fetchone()[0]
        print(f"Total rows: {count:,}")

    # 3. Get date range
    print("\n\n3. DATE RANGE:")
    print("-" * 80)
    query_dates = """
    SELECT
        MIN(sales_date) as min_date,
        MAX(sales_date) as max_date,
        COUNT(DISTINCT sales_date) as distinct_dates
    FROM analytics.market_level_anomalies_v3;
    """

    with reader.get_connection().cursor() as cursor:
        cursor.execute(query_dates)
        colnames = [desc[0] for desc in cursor.description]
        record = cursor.fetchone()
        for i, col in enumerate(colnames):
            print(f"{col}: {record[i]}")

    # 4. Get distinct customers
    print("\n\n4. CUSTOMERS:")
    print("-" * 80)
    query_customers = """
    SELECT
        customer,
        COUNT(*) as record_count,
        MIN(sales_date) as first_date,
        MAX(sales_date) as last_date
    FROM analytics.market_level_anomalies_v3
    GROUP BY customer
    ORDER BY record_count DESC
    LIMIT 20;
    """

    with reader.get_connection().cursor() as cursor:
        cursor.execute(query_customers)
        colnames = [desc[0] for desc in cursor.description]
        records = cursor.fetchall()
        df_customers = pd.DataFrame(records, columns=colnames)
        print(df_customers.to_string(index=False))

    # 5. Sample records
    print("\n\n5. SAMPLE RECORDS (5 rows):")
    print("-" * 80)
    query_sample = """
    SELECT *
    FROM analytics.market_level_anomalies_v3
    LIMIT 5;
    """

    with reader.get_connection().cursor() as cursor:
        cursor.execute(query_sample)
        colnames = [desc[0] for desc in cursor.description]
        records = cursor.fetchall()
        df_sample = pd.DataFrame(records, columns=colnames)
        print("\nColumns:", ", ".join(colnames))
        print(df_sample.to_string(index=False))

    # 6. Check for categorical columns
    print("\n\n6. CATEGORICAL COLUMN VALUES:")
    print("-" * 80)

    # Check cp (competitive position)
    query_cp = """
    SELECT cp, COUNT(*) as count
    FROM analytics.market_level_anomalies_v3
    GROUP BY cp
    ORDER BY count DESC;
    """

    print("\ncp (competitive position) values:")
    with reader.get_connection().cursor() as cursor:
        cursor.execute(query_cp)
        records = cursor.fetchall()
        for record in records:
            print(f"  {record[0]}: {record[1]:,}")

    # Check region_name
    print("\nregion_name values:")
    query_region = """
    SELECT region_name, COUNT(*) as count
    FROM analytics.market_level_anomalies_v3
    GROUP BY region_name
    ORDER BY count DESC
    LIMIT 10;
    """

    with reader.get_connection().cursor() as cursor:
        cursor.execute(query_region)
        records = cursor.fetchall()
        for record in records:
            print(f"  {record[0]}: {record[1]:,}")

    # Check cabin_group
    print("\ncabin_group values:")
    query_cabin = """
    SELECT cabin_group, COUNT(*) as count
    FROM analytics.market_level_anomalies_v3
    GROUP BY cabin_group
    ORDER BY count DESC;
    """

    with reader.get_connection().cursor() as cursor:
        cursor.execute(query_cabin)
        records = cursor.fetchall()
        for record in records:
            print(f"  {record[0]}: {record[1]:,}")

    # 7. Check anomaly columns
    print("\n\n7. ANOMALY STATISTICS:")
    print("-" * 80)

    # Check how many records have anomalies
    query_anomalies = """
    SELECT
        COUNT(*) as total_records,
        SUM(CASE WHEN any_anomaly = 1 THEN 1 ELSE 0 END) as with_anomalies,
        SUM(CASE WHEN freq_pcnt_anomaly = 1 THEN 1 ELSE 0 END) as freq_anomalies,
        SUM(CASE WHEN mag_pcnt_anomaly = 1 THEN 1 ELSE 0 END) as mag_pcnt_anomalies,
        SUM(CASE WHEN mag_nominal_anomaly = 1 THEN 1 ELSE 0 END) as mag_nominal_anomalies,
        ROUND(100.0 * SUM(CASE WHEN any_anomaly = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_with_anomalies
    FROM analytics.market_level_anomalies_v3;
    """

    with reader.get_connection().cursor() as cursor:
        cursor.execute(query_anomalies)
        colnames = [desc[0] for desc in cursor.description]
        record = cursor.fetchone()
        for i, col in enumerate(colnames):
            val = f"{record[i]:,}" if isinstance(record[i], int) else record[i]
            print(f"{col}: {val}")

    # 8. Check score ranges
    print("\n\n8. SCORE RANGES:")
    print("-" * 80)

    score_cols = ['impact_score_v2', 'impact_score', 'direction_score', 'oag_score', 'revenue_score']

    for col in score_cols:
        query = f"""
        SELECT
            MIN({col}) as min_val,
            MAX({col}) as max_val,
            AVG({col}) as avg_val
        FROM analytics.market_level_anomalies_v3
        WHERE {col} IS NOT NULL;
        """
        with reader.get_connection().cursor() as cursor:
            cursor.execute(query)
            record = cursor.fetchone()
            if record[0] is not None:
                print(f"\n{col}:")
                print(f"  Min: {record[0]}")
                print(f"  Max: {record[1]}")
                print(f"  Avg: {record[2]:.4f}")

    reader.close()
    print("\n" + "=" * 80)
    print("Exploration complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
