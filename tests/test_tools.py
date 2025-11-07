#!/usr/bin/env python3
"""
Ad-hoc test for Market Anomalies tools.
Requires ds-mcp and threevictors installed and AWS/Redshift access.
"""

import sys
import json

from ds_mcp.tables.market_anomalies_v3 import (
    TABLE_NAME,
    get_available_customers,
    get_table_schema,
    query_anomalies,
    query_table,
)

def print_result(tool_name, result):
    print(f"\n{'='*80}")
    print(f"Testing: {tool_name}")
    print(f"{'='*80}")
    data = json.loads(result)
    if "error" in data:
        print(f"ERROR: {data['error']}")
    else:
        print(f"Columns: {', '.join(data['columns'])}")
        print(f"Row count: {data['row_count']}")
        print(f"Truncated: {data['truncated']}")
        if data['row_count'] > 0:
            print(f"\nFirst row:")
            for key, value in data['rows'][0].items():
                print(f"  {key}: {value}")

def main():
    print("="*80)
    print("Market Anomalies Tools Test")
    print("="*80)

    # Schema
    print_result("get_table_schema()", get_table_schema())

    # Available customers
    print_result("get_available_customers()", get_available_customers())

    # Simple sample query (LIMIT 1)
    sql = (
        f"SELECT * FROM {TABLE_NAME} "
        "WHERE sales_date >= CAST(TO_CHAR(CURRENT_DATE - 1, 'YYYYMMDD') AS INT) "
        "LIMIT 1"
    )
    print_result("query_table(partitioned LIMIT 1)", query_table(sql))

    # Alias remains for backwards compatibility
    print_result("query_anomalies(partitioned LIMIT 1)", query_anomalies(sql))

    print("\n"*2)
    print("="*80)
    print("All tests completed!")
    print("="*80)

if __name__ == "__main__":
    main()
