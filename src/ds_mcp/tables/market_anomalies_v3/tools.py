"""
MCP tools for market_level_anomalies_v3 table.

Provides query tools for accessing and analyzing market anomaly data.
"""

import json
import logging
import re
from typing import Optional

from ds_mcp.core.connectors import DatabaseConnectorFactory

log = logging.getLogger(__name__)

# Table name constant
TABLE_NAME = "analytics.market_level_anomalies_v3"


def _get_connector():
    """Get the database connector for this table."""
    return DatabaseConnectorFactory.get_connector("analytics")


def _execute_query(sql_query: str, max_rows: int = 100) -> str:
    """
    Execute a SQL query and return results as JSON.

    Args:
        sql_query: SQL query to execute
        max_rows: Maximum number of rows to return

    Returns:
        JSON string with results or error
    """
    connector = _get_connector()
    conn = connector.get_connection()

    # Ensure autocommit where possible to reduce aborted transaction states
    try:
        if getattr(conn, "autocommit", None) is False:
            conn.autocommit = True
    except Exception:
        pass

    attempts = 0
    while attempts < 2:
        try:
            with conn.cursor() as cursor:
                # Clear any aborted transaction state defensively
                try:
                    cursor.execute("ROLLBACK;")
                except Exception:
                    pass

                cursor.execute(sql_query)

                # Get column names
                colnames = [desc[0] for desc in cursor.description]

                # Fetch results
                records = cursor.fetchmany(max_rows)

                # Convert to list of dicts
                results = []
                for record in records:
                    row_dict = {}
                    for i, col in enumerate(colnames):
                        value = record[i]
                        if value is not None and not isinstance(value, (str, int, float, bool)):
                            value = str(value)
                        row_dict[col] = value
                    results.append(row_dict)

                output = {
                    "columns": colnames,
                    "rows": results,
                    "row_count": len(results),
                    "truncated": len(results) == max_rows,
                }

                log.info(f"Query returned {len(results)} rows")
                return json.dumps(output, indent=2)

        except Exception as e:
            msg = str(e)
            # Redshift aborted transaction code 25P02 or message text
            if ("25P02" in msg or "current transaction is aborted" in msg) and attempts == 0:
                try:
                    # Attempt to rollback and retry once
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                finally:
                    attempts += 1
                    continue
            log.error(f"Error executing query: {msg}")
            return json.dumps({"error": msg}, indent=2)

    return json.dumps({"error": "Query failed after retry due to aborted transaction state"}, indent=2)


def query_anomalies(sql_query: str) -> str:
    """
    Execute a SQL query against the analytics.market_level_anomalies_v3 table.

    This is the main tool for querying market anomaly data. Use this to explore anomalies
    by any dimension (customer, date, market, region, competitive position, etc.).

    Important table schema notes:
    - Table name: analytics.market_level_anomalies_v3
    - Primary key: customer, sales_date, seg_mkt
    - Key columns for ordering: impact_score (main metric), sales_date
    - Anomaly flags: any_anomaly (1 for anomalies), freq_pcnt_anomaly, mag_pcnt_anomaly, mag_nominal_anomaly
    - Date format: sales_date as integer YYYYMMDD (e.g., 20251014)
    - Customer: Two-letter code (e.g., 'SK', 'AS', 'B6')

    Key metrics:
    - impact_score: Overall impact metric (use for ordering by calculated importance)
    - freq_pcnt_val, mag_pcnt_val, mag_nominal_val: Anomaly values
    - revenue_score, oag_score: Additional scoring dimensions
    - cp: Competitive position ('Undercut', 'Overpriced', 'Match', 'N/A')

    Query must:
    - Start with SELECT
    - Include FROM analytics.market_level_anomalies_v3
    - Be properly formatted SQL
    - Not contain DELETE, UPDATE, INSERT, DROP, or other modifying statements

    The query is automatically limited to 100 rows maximum for performance.

    Args:
        sql_query: SQL SELECT query to execute

    Returns:
        JSON string with query results containing columns, rows, row_count, and truncated flag

    Example queries:
        "SELECT * FROM analytics.market_level_anomalies_v3 WHERE customer = 'SK' AND sales_date = 20251014 AND any_anomaly = 1 ORDER BY impact_score DESC LIMIT 20"
        "SELECT customer, sales_date, COUNT(*) as anomaly_count FROM analytics.market_level_anomalies_v3 WHERE any_anomaly = 1 GROUP BY customer, sales_date ORDER BY sales_date DESC LIMIT 50"
    """
    try:
        # Basic SQL safety checks
        query_upper = sql_query.upper().strip()

        # Must be a SELECT statement
        if not query_upper.startswith('SELECT'):
            return json.dumps({"error": "Only SELECT queries are allowed"}, indent=2)

        # Block modifying statements
        forbidden_keywords = ['DELETE', 'UPDATE', 'INSERT', 'DROP', 'TRUNCATE', 'ALTER', 'CREATE']
        for keyword in forbidden_keywords:
            if keyword in query_upper:
                return json.dumps({"error": f"Forbidden keyword: {keyword}"}, indent=2)

        # Must reference the correct table
        if 'MARKET_LEVEL_ANOMALIES_V3' not in query_upper:
            return json.dumps({"error": "Query must reference analytics.market_level_anomalies_v3 table"}, indent=2)

        log.info(f"Executing query: {sql_query[:200]}...")

        # Add LIMIT if not present to prevent huge result sets
        if 'LIMIT' not in query_upper:
            sql_query = sql_query.rstrip(';') + ' LIMIT 100'
            max_rows = 100
        else:
            # Extract limit value
            match = re.search(r'LIMIT\s+(\d+)', query_upper)
            max_rows = int(match.group(1)) if match else 100
            max_rows = min(max_rows, 100)  # Cap at 100

        return _execute_query(sql_query, max_rows)

    except Exception as e:
        log.error(f"Error in query_anomalies: {str(e)}")
        return json.dumps({"error": str(e)}, indent=2)


def get_table_schema() -> str:
    """
    Get the schema information for the market_level_anomalies_v3 table.

    Returns column names, types, and descriptions to help construct queries.
    This is useful for understanding what data is available before querying.

    Returns:
        JSON string containing table schema information
    """
    query = f"""
    SELECT
        column_name,
        data_type,
        character_maximum_length,
        is_nullable
    FROM information_schema.columns
    WHERE table_schema = 'analytics'
      AND table_name = 'market_level_anomalies_v3'
    ORDER BY ordinal_position;
    """

    connector = _get_connector()
    conn = connector.get_connection()
    # Try to ensure autocommit and clear any aborted state
    try:
        if getattr(conn, "autocommit", None) is False:
            conn.autocommit = True
    except Exception:
        pass

    attempts = 0
    while attempts < 2:
        try:
            with conn.cursor() as cursor:
                try:
                    cursor.execute("ROLLBACK;")
                except Exception:
                    pass

                cursor.execute(query)
                colnames = [desc[0] for desc in cursor.description]
                records = cursor.fetchall()

                results = []
                for record in records:
                    row_dict = {}
                    for i, col in enumerate(colnames):
                        row_dict[col] = record[i]
                    results.append(row_dict)

                # Fallback to SVV_COLUMNS if information_schema returns no rows
                if len(results) == 0:
                    try:
                        log.info("information_schema returned 0 columns; falling back to SVV_COLUMNS")
                        cursor.execute(
                            """
                            SELECT column_name, data_type, character_maximum_length, is_nullable
                            FROM svv_columns
                            WHERE table_schema = 'analytics'
                              AND table_name = 'market_level_anomalies_v3'
                            ORDER BY ordinal_position;
                            """
                        )
                        colnames = [desc[0] for desc in cursor.description]
                        records = cursor.fetchall()
                        for record in records:
                            row_dict = {}
                            for i, col in enumerate(colnames):
                                row_dict[col] = record[i]
                            results.append(row_dict)
                    except Exception as e2:
                        log.warning(f"SVV_COLUMNS fallback failed: {e2}")

                output = {
                    "table": TABLE_NAME,
                    "columns": results,
                    "notes": {
                        "primary_key": ["customer", "sales_date", "seg_mkt"],
                        "important_metrics": {
                            "impact_score": "Main impact metric - use for ordering by importance",
                            "any_anomaly": "Flag indicating if record is an anomaly (1=yes, 0=no)",
                            "freq_pcnt_val": "Frequency percentage value",
                            "mag_pcnt_val": "Magnitude percentage value",
                            "mag_nominal_val": "Magnitude nominal value"
                        },
                        "date_format": "sales_date is YYYYMMDD integer (e.g., 20251014)",
                        "customer_format": "Two-letter code (e.g., 'SK', 'AS', 'B6')"
                    }
                }

                log.info(f"Schema returned {len(results)} columns")
                return json.dumps(output, indent=2)

        except Exception as e:
            msg = str(e)
            if ("25P02" in msg or "current transaction is aborted" in msg) and attempts == 0:
                try:
                    conn.rollback()
                except Exception:
                    pass
                attempts += 1
                continue
            log.error(f"Error fetching schema: {msg}")
            return json.dumps({"error": msg}, indent=2)

    return json.dumps({"error": "Schema query failed after retry due to aborted transaction state"}, indent=2)


def get_available_customers() -> str:
    """
    Get list of available customers in the dataset with their date ranges.

    Useful for understanding which customers have data available before querying.

    Returns:
        JSON string containing customer codes, record counts, and date ranges
    """
    query = f"""
    SELECT
        customer,
        COUNT(*) as total_records,
        SUM(CASE WHEN any_anomaly = 1 THEN 1 ELSE 0 END) as anomaly_records,
        MIN(sales_date) as first_date,
        MAX(sales_date) as last_date
    FROM {TABLE_NAME}
    GROUP BY customer
    ORDER BY customer;
    """

    connector = _get_connector()
    conn = connector.get_connection()
    try:
        if getattr(conn, "autocommit", None) is False:
            conn.autocommit = True
    except Exception:
        pass

    attempts = 0
    while attempts < 2:
        try:
            with conn.cursor() as cursor:
                try:
                    cursor.execute("ROLLBACK;")
                except Exception:
                    pass

                cursor.execute(query)
                colnames = [desc[0] for desc in cursor.description]
                records = cursor.fetchall()

                results = []
                for record in records:
                    row_dict = {}
                    for i, col in enumerate(colnames):
                        row_dict[col] = record[i]
                    results.append(row_dict)

                output = {
                    "customers": results,
                    "count": len(results)
                }

                log.info(f"Found {len(results)} customers")
                return json.dumps(output, indent=2)

        except Exception as e:
            msg = str(e)
            if ("25P02" in msg or "current transaction is aborted" in msg) and attempts == 0:
                try:
                    conn.rollback()
                except Exception:
                    pass
                attempts += 1
                continue
            log.error(f"Error fetching customers: {msg}")
            return json.dumps({"error": msg}, indent=2)

    return json.dumps({"error": "Customer list query failed after retry due to aborted transaction state"}, indent=2)
