#!/usr/bin/env python3
"""
Test script for MCP server tools
Tests each tool with real database queries
"""

import sys
import os
import json

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Import server tools
from server import (
    get_available_customers,
    get_date_range,
    get_anomaly_summary_by_date,
    get_top_anomalies_by_impact,
    get_frequency_anomalies,
    search_anomalies
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
    print("MCP Server Tools Test")
    print("="*80)

    # Test 1: Get available customers
    print_result("get_available_customers()", get_available_customers())

    # Get first customer for subsequent tests
    customers_data = json.loads(get_available_customers())
    if customers_data['row_count'] > 0:
        customer = customers_data['rows'][0]['customer']
        print(f"\n\nUsing customer '{customer}' for subsequent tests")

        # Test 2: Get date range for customer
        print_result(f"get_date_range('{customer}')", get_date_range(customer))

        # Get a sales date for testing
        date_data = json.loads(get_date_range(customer))
        if date_data['row_count'] > 0:
            sales_date = date_data['rows'][0]['max_date']
            print(f"\n\nUsing sales_date {sales_date} for subsequent tests")

            # Test 3: Get anomaly summary
            print_result(
                f"get_anomaly_summary_by_date('{customer}', {sales_date})",
                get_anomaly_summary_by_date(customer, sales_date)
            )

            # Test 4: Get top anomalies by impact
            print_result(
                f"get_top_anomalies_by_impact('{customer}', {sales_date}, min_impact_score=5.0, max_rows=5)",
                get_top_anomalies_by_impact(customer, sales_date, min_impact_score=5.0, max_rows=5)
            )

            # Test 5: Get frequency anomalies
            print_result(
                f"get_frequency_anomalies('{customer}', {sales_date}, min_freq_pcnt=0.05, max_rows=5)",
                get_frequency_anomalies(customer, sales_date, min_freq_pcnt=0.05, max_rows=5)
            )

            # Test 6: Search anomalies
            print_result(
                f"search_anomalies('{customer}', {sales_date}, 'BOS', max_rows=5)",
                search_anomalies(customer, sales_date, "BOS", max_rows=5)
            )

    print("\n"*2)
    print("="*80)
    print("All tests completed!")
    print("="*80)

if __name__ == "__main__":
    main()
