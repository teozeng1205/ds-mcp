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


def _expand_macros(sql_query: str) -> str:
    """Expand simple macros for market anomalies queries.

    Supported:
    - {{MLA}} -> analytics.market_level_anomalies_v3
    """
    return (
        sql_query
        .replace("{{MLA}}", TABLE_NAME)
    )


def _get_connector():
    """Get the database connector for this table."""
    return DatabaseConnectorFactory.get_connector("analytics")


def _today_int() -> str:
    """Returns SQL expression for today's YYYYMMDD int."""
    return "CAST(TO_CHAR(CURRENT_DATE, 'YYYYMMDD') AS INT)"


def _execute_query(sql_query: str, max_rows: int = 100) -> str:
    """
    Execute a SQL query and return results as JSON.

    Args:
        sql_query: SQL query to execute
        max_rows: Maximum number of rows to return

    Returns:
        JSON string with results or error
    """
    # Expand macros and enforce read-only safety
    sql_query = _expand_macros(sql_query)
    query_upper = sql_query.upper().strip()
    if not (query_upper.startswith('SELECT') or query_upper.startswith('WITH')):
        return json.dumps({"error": "Only SELECT or WITH ... SELECT queries are allowed"}, indent=2)
    forbidden_keywords = ['DELETE', 'UPDATE', 'INSERT', 'DROP', 'TRUNCATE', 'ALTER', 'CREATE', 'COPY', 'UNLOAD', 'GRANT', 'REVOKE']
    for keyword in forbidden_keywords:
        if keyword in query_upper:
            return json.dumps({"error": f"Forbidden keyword: {keyword}"}, indent=2)

    # Ensure a LIMIT for safety (robust across newlines/spacing)
    if not re.search(r"\blimit\b\s+\d+", sql_query, flags=re.IGNORECASE):
        sql_query = sql_query.rstrip(';') + f' LIMIT {max_rows}'

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
                    "sql": sql_query,
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


def overview_anomalies_today(per_dim_limit: int = 5) -> str:
    """
    Overview of anomalies today across all customers.

    Returns JSON sections with simple grouped counts:
      - customers: top customers by anomaly count (any_anomaly=1)
      - cp: competitive position distribution (top N)
    """
    per_dim_limit = min(max(1, per_dim_limit), 25)

    base = (
        " FROM {{MLA}} "
        + f"WHERE sales_date = {_today_int()} "
        + "AND any_anomaly = 1 "
    )

    customers_sql = (
        "SELECT customer AS bucket, COUNT(*) AS cnt" +
        base +
        f"GROUP BY 1 ORDER BY 2 DESC LIMIT {per_dim_limit}"
    )

    cp_sql = (
        "SELECT NULLIF(TRIM(cp::VARCHAR), '') AS bucket, COUNT(*) AS cnt" +
        base +
        f"GROUP BY 1 ORDER BY 2 DESC LIMIT {per_dim_limit}"
    )

    return json.dumps({
        "customers": json.loads(_execute_query(customers_sql, max_rows=per_dim_limit)),
        "cp": json.loads(_execute_query(cp_sql, max_rows=per_dim_limit)),
    }, indent=2)


def query_anomalies(sql_query: str) -> str:
    """
    Execute a read-only SQL query against analytics.market_level_anomalies_v3.

    Use this to explore anomalies by any dimension (customer, date, market, region, etc.).
    Supported macro: {{MLA}} expands to the fully-qualified table name.

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
    - Start with SELECT or WITH
    - Include FROM analytics.market_level_anomalies_v3 (or use {{MLA}})
    - Not contain DELETE, UPDATE, INSERT, DROP, TRUNCATE, ALTER, CREATE, COPY, UNLOAD, GRANT, REVOKE

    The query is automatically limited to 100 rows maximum for performance.

    Args:
        sql_query: SQL SELECT query to execute

    Returns:
        JSON string with query results containing columns, rows, row_count, and truncated flag

    Example queries:
        "SELECT * FROM {{MLA}} WHERE customer = 'SK' AND sales_date = 20251014 AND any_anomaly = 1 ORDER BY impact_score DESC LIMIT 20"
        "SELECT customer, sales_date, COUNT(*) as anomaly_count FROM {{MLA}} WHERE any_anomaly = 1 GROUP BY customer, sales_date ORDER BY sales_date DESC LIMIT 50"
    """
    try:
        # Basic SQL safety checks
        query_upper = sql_query.upper().strip()

        # Must be a read-only statement and reference table (via name or macro)
        if not (query_upper.startswith('SELECT') or query_upper.startswith('WITH')):
            return json.dumps({"error": "Only SELECT queries are allowed"}, indent=2)

        # Block modifying statements
        forbidden_keywords = ['DELETE', 'UPDATE', 'INSERT', 'DROP', 'TRUNCATE', 'ALTER', 'CREATE', 'COPY', 'UNLOAD', 'GRANT', 'REVOKE']
        for keyword in forbidden_keywords:
            if keyword in query_upper:
                return json.dumps({"error": f"Forbidden keyword: {keyword}"}, indent=2)

        # Must reference the correct table directly or via macro
        if 'MARKET_LEVEL_ANOMALIES_V3' not in query_upper and '{{MLA}}' not in sql_query:
            return json.dumps({"error": "Query must reference analytics.market_level_anomalies_v3 (or use {{MLA}})"}, indent=2)

        log.info(f"Executing query: {sql_query[:200]}...")

        # Add LIMIT if not present to prevent huge result sets (also capped in executor)
        if 'LIMIT' not in query_upper:
            sql_query = sql_query.rstrip(';') + ' LIMIT 100'
            max_rows = 100
        else:
            # Extract limit value
            match = re.search(r'LIMIT\s+(\d+)', query_upper)
            max_rows = int(match.group(1)) if match else 100
            max_rows = min(max_rows, 100)

        return _execute_query(sql_query, max_rows)

    except Exception as e:
        log.error(f"Error in query_anomalies: {str(e)}")
        return json.dumps({"error": str(e)}, indent=2)


def get_table_schema() -> str:
    """
    Get table schema using a simple SELECT; executed through the macro-aware executor.

    Returns:
        JSON rows with: column_name, data_type, character_maximum_length, is_nullable
    """
    return _execute_query(
        """
        SELECT column_name, data_type, character_maximum_length, is_nullable
        FROM svv_columns
        WHERE table_schema = 'analytics' AND table_name = 'market_level_anomalies_v3'
        ORDER BY ordinal_position
        """
    )


def get_available_customers() -> str:
    """
    Get list of available customers with record counts and date ranges.

    Returns:
        JSON rows containing customer, total_records, anomaly_records, first_date, last_date
    """
    return _execute_query(
        """
        SELECT
            customer,
            COUNT(*) AS total_records,
            SUM(CASE WHEN any_anomaly = 1 THEN 1 ELSE 0 END) AS anomaly_records,
            MIN(sales_date) AS first_date,
            MAX(sales_date) AS last_date
        FROM {{MLA}}
        GROUP BY customer
        ORDER BY customer
        """
    )
