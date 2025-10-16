#!/usr/bin/env python3
"""
Script to explore monitoring_prod.provider_combined_audit table
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from threevictors.dao import redshift_connector
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 80)


class AnalyticsReader(redshift_connector.RedshiftConnector):
    def get_properties_filename(self):
        return "database-analytics-redshift-serverless-reader.properties"


def main():
    print("=" * 80)
    print("Exploring monitoring_prod.provider_combined_audit")
    print("=" * 80)

    reader = AnalyticsReader()

    # 1. Get table structure
    print("\n1. TABLE STRUCTURE:")
    print("-" * 80)
    query_structure = """
    SELECT column_name, data_type, is_nullable, ordinal_position
    FROM information_schema.columns
    WHERE table_schema = 'monitoring_prod'
      AND table_name = 'provider_combined_audit'
    ORDER BY ordinal_position;
    """
    with reader.get_connection().cursor() as cursor:
        cursor.execute(query_structure)
        colnames = [desc[0] for desc in cursor.description]
        records = cursor.fetchall()
        df_structure = pd.DataFrame(records, columns=colnames)
        print(df_structure.to_string(index=False))

    # 2. Row count
    print("\n\n2. ROW COUNT:")
    print("-" * 80)
    with reader.get_connection().cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS total_rows FROM monitoring_prod.provider_combined_audit;")
        count = cursor.fetchone()[0]
        print(f"Total rows: {count:,}")

    # 3. Try common timestamp columns
    print("\n\n3. TIMESTAMP COLUMNS:")
    print("-" * 80)
    with reader.get_connection().cursor() as cursor:
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'monitoring_prod'
              AND table_name = 'provider_combined_audit'
              AND data_type ILIKE '%timestamp%'
            ORDER BY ordinal_position;
            """
        )
        ts_cols = [row[0] for row in cursor.fetchall()]
        if ts_cols:
            print("Timestamp columns:", ", ".join(ts_cols))
        else:
            print("No timestamp columns detected")

    # 4. Sample rows
    print("\n\n4. SAMPLE ROWS (10):")
    print("-" * 80)
    with reader.get_connection().cursor() as cursor:
        cursor.execute("SELECT * FROM monitoring_prod.provider_combined_audit LIMIT 10;")
        colnames = [desc[0] for desc in cursor.description]
        records = cursor.fetchall()
        df_sample = pd.DataFrame(records, columns=colnames)
        print("Columns:", ", ".join(colnames))
        print(df_sample.to_string(index=False))

    # 5. Distinct values for likely categorical columns
    print("\n\n5. DISTINCT VALUES (likely columns):")
    print("-" * 80)
    likely_cols = ["provider", "provider_id", "status", "action", "changed_by", "source"]
    with reader.get_connection().cursor() as cursor:
        # derive existing columns
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'monitoring_prod'
              AND table_name = 'provider_combined_audit'
            """
        )
        present = {row[0] for row in cursor.fetchall()}
        to_check = [c for c in likely_cols if c in present]
        for c in to_check:
            print(f"\nDistinct {c}:")
            cursor.execute(
                f"SELECT {c}, COUNT(*) cnt FROM monitoring_prod.provider_combined_audit GROUP BY {c} ORDER BY cnt DESC NULLS LAST LIMIT 10"
            )
            rows = cursor.fetchall()
            for v, cnt in rows:
                print(f"  {v}: {cnt:,}")

    # 6. Targeted query per request
    print("\n\n6. TARGETED QUERY (sales_date = 20251014, LIMIT 10):")
    print("-" * 80)
    targeted_sql = (
        "SELECT * FROM monitoring_prod.provider_combined_audit "
        "WHERE sales_date = 20251014 LIMIT 10;"
    )
    with reader.get_connection().cursor() as cursor:
        cursor.execute(targeted_sql)
        colnames = [desc[0] for desc in cursor.description]
        records = cursor.fetchall()
        df_target = pd.DataFrame(records, columns=colnames)
        print("Columns:", ", ".join(colnames))
        print(df_target.to_string(index=False))

    reader.close()
    print("\n" + "=" * 80)
    print("Exploration complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
