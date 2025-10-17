#!/usr/bin/env python3
"""
Run and validate Market Anomalies queries for today's date for customer B6.

This script:
- Prints schema column count
- Lists available customers and confirms B6 exists
- Chooses the date to use (today if available, else last available for B6)
- Executes a few representative queries via tools.query_anomalies and checks rows

Environment requirements:
- AWS/Redshift credentials configured (see .env.sh)
- ds-threevictors installed and discoverable
"""

import json
import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from ds_mcp.tables.market_anomalies_v3.tools import (
    get_table_schema,
    get_available_customers,
    query_anomalies,
    TABLE_NAME,
)


def as_int_yyyymmdd(dt: datetime) -> int:
    return int(dt.strftime("%Y%m%d"))


def pretty(title: str, payload: str):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)
    print(payload)


def ensure_rows(title: str, payload: str) -> int:
    try:
        data = json.loads(payload)
        n = data.get("row_count") or data.get("count") or 0
        if isinstance(n, int) and n > 0:
            print(f"✓ {title}: {n} rows")
            return n
        # Fallbacks for endpoints
        rows = (
            data.get("rows")
            or data.get("customers")
            or data.get("columns")
            or []
        )
        if isinstance(rows, list) and len(rows) > 0:
            print(f"✓ {title}: {len(rows)} rows")
            return len(rows)
        print(f"✗ {title}: No rows returned")
        return 0
    except Exception as e:
        print(f"✗ {title}: Failed to parse JSON: {e}")
        return 0


def main():
    customer = "B6"
    today = as_int_yyyymmdd(datetime.now(timezone.utc))
    print(f"Customer: {customer}")
    print(f"Today (UTC): {today}")
    print(f"Table: {TABLE_NAME}")

    # 1) Schema
    schema_json = get_table_schema()
    pretty("Schema", schema_json)
    ensure_rows("Schema columns", schema_json)

    # 2) Available customers
    cust_json = get_available_customers()
    pretty("Available customers", cust_json)
    cust_data = json.loads(cust_json)
    b6_entry = None
    for c in cust_data.get("customers", []):
        if (c.get("customer") or "").upper() == customer:
            b6_entry = c
            break
    if not b6_entry:
        print("✗ B6 not found in available customers; cannot proceed with queries.")
        sys.exit(2)

    # Determine date to use
    last_date = int(b6_entry.get("last_date") or 0)
    first_date = int(b6_entry.get("first_date") or 0)
    use_date = today if last_date >= today else last_date
    if use_date == 0:
        print("✗ B6 has no date entries; cannot proceed.")
        sys.exit(3)
    print(f"Using sales_date={use_date} (today if available, else last available)")

    # 3) Representative queries
    queries = [
        (
            "Top anomalies by impact",
            f"""
            SELECT customer, sales_date, seg_mkt, mkt, cp, region_name, impact_score
            FROM {TABLE_NAME}
            WHERE customer = '{customer}' AND sales_date = {use_date} AND any_anomaly = 1
            ORDER BY impact_score DESC NULLS LAST
            LIMIT 25
            """.strip(),
        ),
        (
            "Anomaly counts by region",
            f"""
            SELECT region_name, COUNT(*) AS anomaly_count
            FROM {TABLE_NAME}
            WHERE customer = '{customer}' AND sales_date = {use_date} AND any_anomaly = 1
            GROUP BY region_name
            ORDER BY anomaly_count DESC NULLS LAST
            LIMIT 25
            """.strip(),
        ),
        (
            "Competitive position breakdown",
            f"""
            SELECT cp, COUNT(*) AS cnt
            FROM {TABLE_NAME}
            WHERE customer = '{customer}' AND sales_date = {use_date} AND any_anomaly = 1
            GROUP BY cp
            ORDER BY cnt DESC NULLS LAST
            LIMIT 25
            """.strip(),
        ),
    ]

    all_ok = True
    for title, sql in queries:
        res = query_anomalies(sql)
        pretty(title, res)
        n = ensure_rows(title, res)
        if n <= 0:
            all_ok = False

    print("\n" + "=" * 80)
    if all_ok:
        print("ALL QUERIES RETURNED ROWS ✓")
        sys.exit(0)
    else:
        print("SOME QUERIES RETURNED NO ROWS ✗")
        sys.exit(4)


if __name__ == "__main__":
    main()
