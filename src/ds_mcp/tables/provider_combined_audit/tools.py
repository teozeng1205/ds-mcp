"""
MCP tools for monitoring_prod.provider_combined_audit.

Compact tools, similar in style to market anomalies:
- query_audit: Run read-only SELECTs (with a few macros)
- get_table_schema: List columns and types
- top_site_issues: Top site-related issues for a provider
- issue_scope_breakdown: Where site issues concentrate (time/pos/od/cabin)
"""

import json
import logging
import re
from typing import Dict, List, Optional

from ds_mcp.core.connectors import DatabaseConnectorFactory

log = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================

SCHEMA_NAME = "monitoring_prod"
TABLE_BASE = "provider_combined_audit"
TABLE_NAME = f"{SCHEMA_NAME}.{TABLE_BASE}"

# ============================================================================
# Issue Classification Patterns
# ============================================================================

SITE_KEYWORDS = [
    'site', 'website', 'web site', 'captcha', 'akamai', 'cloudflare',
    'timeout', 'forbidden', 'service unavailable', 'internal server error',
    'ssl', 'tls', 'handshake'
]

INVALID_REQUEST_KEYWORDS = [
    'invalid', 'bad request', 'bad_request', 'missing', 'malformed',
    'unsupported', 'param'
]


def _build_issue_type_case(alias: Optional[str] = None) -> str:
    """CASE expression classifying issues (site_related/invalid_request/other)."""
    site_conditions = " OR ".join([
        f"LOWER(COALESCE(issue_sources,'')) LIKE '%{kw}%'" for kw in SITE_KEYWORDS
    ] + [
        f"LOWER(COALESCE(issue_reasons,'')) LIKE '%{kw}%'" for kw in ['site', 'website']
    ])

    invalid_conditions = " OR ".join([
        f"LOWER(COALESCE(issue_sources,'')) LIKE '%{kw}%'" for kw in INVALID_REQUEST_KEYWORDS
    ] + [
        f"LOWER(COALESCE(issue_reasons,'')) LIKE '%{kw}%'" for kw in INVALID_REQUEST_KEYWORDS
    ])

    case_expr = (
        f"CASE "
        f"WHEN {site_conditions} THEN 'site_related' "
        f"WHEN {invalid_conditions} THEN 'invalid_request' "
        f"ELSE COALESCE(NULLIF(LOWER(issue_sources), ''), 'other') END"
    )
    return f"{case_expr} AS {alias}" if alias else case_expr


def _build_site_filter() -> str:
    """WHERE condition for site-related issues."""
    return "(" + " OR ".join([
        f"LOWER(COALESCE(issue_sources,'')) LIKE '%{kw}%'" for kw in SITE_KEYWORDS
    ] + [
        f"LOWER(COALESCE(issue_reasons,'')) LIKE '%{kw}%'" for kw in ['site', 'website']
    ]) + ")"


def _build_invalid_filter() -> str:
    """WHERE condition for invalid-request issues."""
    return "(" + " OR ".join([
        f"LOWER(COALESCE(issue_sources,'')) LIKE '%{kw}%'" for kw in INVALID_REQUEST_KEYWORDS
    ] + [
        f"LOWER(COALESCE(issue_reasons,'')) LIKE '%{kw}%'" for kw in INVALID_REQUEST_KEYWORDS
    ]) + ")"


def _build_event_ts(alias: Optional[str] = None) -> str:
    """Best-effort event timestamp from available columns."""
    case_expr = (
        "CASE "
        "WHEN response_timestamp IS NOT NULL AND TRIM(response_timestamp) <> '' "
        "THEN TO_TIMESTAMP(TRIM(response_timestamp), 'YYYY-MM-DD HH24:MI:SS.MS') "
        "WHEN actualscheduletimestamp IS NOT NULL THEN actualscheduletimestamp "
        "WHEN observationtimestamp IS NOT NULL THEN observationtimestamp "
        "WHEN dropdeadtimestamps IS NOT NULL AND TRIM(dropdeadtimestamps) <> '' "
        "THEN TO_TIMESTAMP(TRIM(dropdeadtimestamps), 'YYYY-MM-DD HH24:MI:SS.MS') "
        "ELSE NULL END"
    )
    return f"{case_expr} AS {alias}" if alias else case_expr


# ============================================================================
# SQL Macro Expansion
# ============================================================================

def _expand_macros(sql: str) -> str:
    """Expand convenient macros in SQL queries."""
    result = sql

    # Handle {{ISSUE_TYPE}} and {{ISSUE_TYPE:alias}}
    def expand_issue_type(match):
        alias = match.group(1)
        return _build_issue_type_case(alias)
    result = re.sub(r"\{\{ISSUE_TYPE(?::([A-Za-z_][A-Za-z0-9_]*))?\}\}", expand_issue_type, result)

    # Handle {{EVENT_TS}} and {{EVENT_TS:alias}}
    def expand_event_ts(match):
        alias = match.group(1)
        return _build_event_ts(alias)
    result = re.sub(r"\{\{EVENT_TS(?::([A-Za-z_][A-Za-z0-9_]*))?\}\}", expand_event_ts, result)

    # Simple replacements
    result = result.replace("{{PCA}}", TABLE_NAME)
    result = result.replace("{{OD}}", "(originairportcode || '-' || destinationairportcode)")
    result = result.replace("{{OBS_HOUR}}", f"DATE_TRUNC('hour', {_build_event_ts()})")
    result = result.replace("{{IS_SITE}}", _build_site_filter())
    result = result.replace("{{IS_INVALID}}", _build_invalid_filter())

    return result


# ============================================================================
# Provider/Site Parsing
# ============================================================================

# Note: Provider/site combined parsing removed for simplicity. Use provider text match.


# ============================================================================
# Database Helpers
# ============================================================================

def _get_conn():
    """Get database connection with autocommit enabled."""
    conn = DatabaseConnectorFactory.get_connector("analytics").get_connection()
    try:
        conn.autocommit = True
    except Exception:
        pass
    return conn


def _safe_execute(cursor, sql: str, params=None, retry=True):
    """Execute SQL with automatic rollback retry on aborted transactions."""
    try:
        cursor.execute("ROLLBACK;")
    except Exception:
        pass

    try:
        cursor.execute(sql, params) if params else cursor.execute(sql)
        return True
    except Exception as e:
        if retry and ("25P02" in str(e) or "aborted" in str(e)):
            try:
                cursor.connection.rollback()
                return _safe_execute(cursor, sql, params, retry=False)
            except Exception:
                pass
        raise


def _get_columns() -> Dict[str, str]:
    """Get column name -> data type mapping for the table."""
    conn = _get_conn()
    with conn.cursor() as cur:
        try:
            _safe_execute(cur, f"""
                SELECT column_name, data_type
                FROM svv_columns
                WHERE table_schema = '{SCHEMA_NAME}' AND table_name = '{TABLE_BASE}'
                ORDER BY ordinal_position
            """)
            return {row[0]: row[1] for row in cur.fetchall()}
        except Exception:
            return {}


def _pick_column(cols: Dict[str, str], candidates: List[str]) -> Optional[str]:
    """Pick first available column from candidates list."""
    return next((c for c in candidates if c in cols), None)


def _date_expr(col: str, dtype: str) -> str:
    """Convert column to DATE expression based on data type."""
    dtype_lower = dtype.lower()
    if "timestamp" in dtype_lower:
        return f"CAST({col} AS DATE)"
    if "date" in dtype_lower:
        return col
    # Assume YYYYMMDD integer format
    return f"TO_DATE({col}::VARCHAR, 'YYYYMMDD')"


# ============================================================================
# MCP Tools
# ============================================================================

def query_audit(sql_query: str) -> str:
    """
    Execute a read-only SQL query against provider monitoring data with macro support.

    This tool allows you to write custom SQL queries to analyze provider issues, with
    helpful macros that automatically expand to complex SQL expressions.

    INPUT PARAMETERS:
    ------------------
    sql_query (str, REQUIRED): A SQL SELECT query as a string. Must start with SELECT or WITH.
                                Can use macros (see below) for common patterns.

    IMPORTANT - Issue Type Classification:
    ---------------------------------------
    There are TWO types of issues that are automatically classified:

    1. SITE-RELATED ISSUES (use {{IS_SITE}} filter):
       - Timeouts, SSL/TLS errors, captcha blocks
       - Website downtime, cloudflare/akamai blocks
       - HTTP errors (403 forbidden, 503 unavailable, 500 internal server error)
       - Keywords: 'site', 'website', 'timeout', 'ssl', 'tls', 'captcha', 'akamai',
                  'cloudflare', 'forbidden', 'service unavailable', 'internal server error'

    2. INVALID-REQUEST ISSUES (use {{IS_INVALID}} filter):
       - Bad input parameters, malformed requests
       - Missing required fields, unsupported operations
       - Keywords: 'invalid', 'bad request', 'missing', 'malformed', 'unsupported', 'param'

    AVAILABLE MACROS:
    -----------------
    - {{PCA}}
      Expands to: monitoring_prod.provider_combined_audit
      Use in: FROM clause

    - {{OD}}
      Expands to: (originairportcode || '-' || destinationairportcode)
      Use in: SELECT, GROUP BY for origin-destination pairs

    - {{ISSUE_TYPE}} or {{ISSUE_TYPE:column_name}}
      Expands to: CASE statement that classifies issues as 'site_related', 'invalid_request', or 'other'
      Use in: SELECT clause to see issue type distribution

    - {{EVENT_TS}} or {{EVENT_TS:column_name}}
      Expands to: Best-effort event timestamp from multiple timestamp columns
      Use in: SELECT, WHERE, GROUP BY for time-based analysis

    - {{OBS_HOUR}}
      Expands to: DATE_TRUNC('hour', {{EVENT_TS}})
      Use in: SELECT, GROUP BY for hourly patterns

    - {{IS_SITE}}
      Expands to: Boolean condition for site-related issues only
      Use in: WHERE clause to filter to site issues

    - {{IS_INVALID}}
      Expands to: Boolean condition for invalid-request issues only
      Use in: WHERE clause to filter to invalid-request issues

    KEY TABLE COLUMNS:
    ------------------
    - providercode (VARCHAR): Provider identifier (e.g., 'AA', 'DL')
    - issue_sources (VARCHAR): Source/category of issue
    - issue_reasons (VARCHAR): Detailed reason for issue
    - scheduledate (BIGINT): Observation date in YYYYMMDD format
    - pos (VARCHAR): Point of sale
    - cabin (VARCHAR): Cabin class
    - triptype (CHAR): Trip type ('R' = round-trip, 'O' = one-way)
    - los (BIGINT): Length of stay in days
    - originairportcode (CHAR): Origin airport code
    - destinationairportcode (CHAR): Destination airport code
    - departdate (INT): Departure date YYYYMMDD
    - returndate (INT): Return date YYYYMMDD

    EXAMPLE QUERIES:
    ----------------

    Example 1: "Show me top site-related issues for AA in the last 7 days"
    ```sql
    SELECT COALESCE(NULLIF(TRIM(issue_reasons::VARCHAR), ''), 'unknown') AS issue,
           COUNT(*) AS count
    FROM {{PCA}}
    WHERE providercode ILIKE '%AA%'
      AND {{IS_SITE}}
      AND TO_DATE(scheduledate::VARCHAR, 'YYYYMMDD') >= CURRENT_DATE - 7
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 20;
    ```

    Example 2: "Show hourly pattern of site issues for AA in last 3 days"
    ```sql
    SELECT {{OBS_HOUR}} AS hour, COUNT(*) AS count
    FROM {{PCA}}
    WHERE providercode ILIKE '%AA%'
      AND {{IS_SITE}}
      AND TO_DATE(scheduledate::VARCHAR, 'YYYYMMDD') >= CURRENT_DATE - 3
    GROUP BY 1
    ORDER BY 1;
    ```

    Example 3: "Compare site vs invalid-request issues for all providers yesterday"
    ```sql
    SELECT {{ISSUE_TYPE:issue_type}}, COUNT(*) AS count
    FROM {{PCA}}
    WHERE TO_DATE(scheduledate::VARCHAR, 'YYYYMMDD') = CURRENT_DATE - 1
    GROUP BY 1
    ORDER BY 2 DESC;
    ```

    Example 4: "Show routes with most invalid-request issues"
    ```sql
    SELECT {{OD}} AS route, COUNT(*) AS count
    FROM {{PCA}}
    WHERE {{IS_INVALID}}
      AND TO_DATE(scheduledate::VARCHAR, 'YYYYMMDD') >= CURRENT_DATE - 7
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10;
    ```

    RETURNS:
    --------
    JSON string with:
    - columns (list): Column names from query result
    - rows (list): Array of result rows as JSON objects
    - row_count (int): Number of rows returned
    - expanded_sql (str): The actual SQL executed (with macros expanded)

    Or if error:
    - error (str): Error message
    """
    try:
        # Expand macros
        expanded_sql = _expand_macros(sql_query)

        # Safety checks
        sql_upper = expanded_sql.upper().strip()
        if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
            return json.dumps({"error": "Only SELECT or WITH ... SELECT queries allowed"}, indent=2)

        forbidden = ["DELETE", "UPDATE", "INSERT", "DROP", "TRUNCATE", "ALTER",
                     "CREATE", "COPY", "UNLOAD", "GRANT", "REVOKE"]
        for keyword in forbidden:
            if re.search(rf"\b{keyword}\b", sql_upper):
                return json.dumps({"error": f"Forbidden keyword: {keyword}"}, indent=2)

        # Auto-limit to 100 rows
        truncated = False
        if "LIMIT" not in sql_upper:
            expanded_sql = expanded_sql.rstrip(";") + " LIMIT 100"
            truncated = True

        # Execute query
        conn = _get_conn()
        with conn.cursor() as cur:
            _safe_execute(cur, expanded_sql)
            columns = [desc[0] for desc in cur.description]
            rows = []
            for row in cur.fetchmany(100):
                rows.append({
                    col: (str(val) if val is not None and not isinstance(val, (str, int, float, bool)) else val)
                    for col, val in zip(columns, row)
                })

            return json.dumps({
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
                "truncated": truncated or (len(rows) == 100),
                "expanded_sql": expanded_sql
            }, indent=2)

    except Exception as e:
        log.error(f"Error in query_audit: {e}")
        return json.dumps({"error": str(e)}, indent=2)


def get_table_schema() -> str:
    """
    Get the complete table schema showing all available columns and their data types.

    Use this tool when you need to:
    - Discover what columns are available before writing a query
    - Check column data types to know how to filter or format them
    - Find available dimensions for analysis (POS, cabin, triptype, etc.)

    INPUT PARAMETERS:
    ------------------
    None - This tool takes no parameters

    WHEN TO USE:
    ------------
    - User asks "what data is available?"
    - Before writing a custom query with query_audit()
    - To verify column names for top_site_issues() or issue_scope_breakdown()

    RETURNS:
    --------
    JSON string with:
    - table (str): Full table name "monitoring_prod.provider_combined_audit"
    - columns (list): Array of {column_name, data_type} objects for all 52 columns
    - notes (object): Helpful groupings:
        * issue_fields: Columns containing issue information
        * date_fields: Columns for date/time filtering
        * search_dimensions: Columns for dimensional analysis (POS, cabin, etc.)
        * travel_dates: Departure and return date columns

    EXAMPLE USAGE:
    --------------
    User: "What columns are in the provider audit table?"
    Action: Call get_table_schema() with no parameters

    User: "What dimensions can I analyze issues by?"
    Action: Call get_table_schema() and look at the 'notes.search_dimensions' field
    """
    cols = _get_columns()
    columns = [{"column_name": k, "data_type": v} for k, v in cols.items()]

    return json.dumps({
        "table": TABLE_NAME,
        "columns": columns,
        "notes": {
            "issue_fields": ["issue_sources", "issue_reasons"],
            "date_fields": ["scheduledate", "actualscheduletimestamp", "observationtimestamp"],
            "search_dimensions": ["pos", "cabin", "originairportcode", "destinationairportcode"],
            "travel_dates": ["departdate", "returndate"]
        }
    }, indent=2)


def top_site_issues(provider: str, lookback_days: int = 7, limit: int = 10) -> str:
    """
    Get the top site-related issues for a specific provider, ranked by frequency.

    This tool answers questions like:
    - "What are the top site issues for AA?"
    - "Show me the most common site problems for provider AA"
    - "What site errors is DL experiencing?"

    IMPORTANT - What are SITE-RELATED issues?
    ------------------------------------------
    Site-related issues are infrastructure/connectivity problems, including:
    - Timeouts and connection failures
    - SSL/TLS certificate errors and handshake failures
    - Captcha blocks and bot detection (akamai, cloudflare)
    - HTTP errors: 403 Forbidden, 503 Service Unavailable, 500 Internal Server Error
    - Website downtime or unavailability

    These are DIFFERENT from invalid-request issues (bad parameters, malformed requests).

    INPUT PARAMETERS:
    ------------------
    provider (str, REQUIRED):
        Provider code or name text to match (simple ILIKE)

        Format options:
        - Uses case-insensitive matching with SQL ILIKE '%pattern%'
        - Examples: "AA", "Delta", "United"

    lookback_days (int, OPTIONAL, default=7):
        How many days of data to analyze, counting back from most recent data
        - Default: 7 (last week)
        - Examples: 1 (yesterday only), 7 (last week), 30 (last month)
        - Range: 1 to 365

    limit (int, OPTIONAL, default=10):
        Maximum number of top issues to return
        - Default: 10
        - Maximum: 50
        - Returns issues ranked by count (most frequent first)

    RETURNS:
    --------
    JSON string with:
    - filters (object): The filters applied
        * provider_like: The provider pattern searched
        * issue_type: Always "site_related"
        * lookback_days: Days of data analyzed
    - rows (list): Top issues ranked by frequency, each with:
        * issue_key: The issue name (from issue_reasons or issue_sources)
        * count: Number of occurrences
    - row_count (int): Number of issues returned

    EXAMPLE USAGE:
    --------------
    User: "AA has an increase in site-related issues. What are the top site issues?"
    Action: top_site_issues(provider="AA", lookback_days=7, limit=20)

    User: "Show me site problems for American Airlines in the last 3 days"
    Action: top_site_issues(provider="AA", lookback_days=3, limit=10)

    User: "What are the most common site errors for all Delta providers?"
    Action: top_site_issues(provider="Delta", lookback_days=7, limit=15)

    WHEN TO USE:
    ------------
    - User mentions "site issues", "site problems", "site errors"
    - User asks "what are the top issues" (and means site-related)
    - User wants to see most frequent/common issues
    - User specifies a provider name or code

    WHEN NOT TO USE:
    ----------------
    - For invalid-request issues → use query_audit() with {{IS_INVALID}} filter
    - For dimensional breakdown → use issue_scope_breakdown()
    - For custom analysis → use query_audit()
    """
    limit = min(max(1, limit), 50)
    cols = _get_columns()

    # Find provider column
    provider_col = _pick_column(cols, ["providercode", "provider", "provider_id"]) or "providercode"
    date_col = _pick_column(cols, ["scheduledate"]) or "scheduledate"
    date_expr = _date_expr(date_col, cols.get(date_col, ""))

    # Simple provider pattern
    provider_filter = f"{provider_col} ILIKE %s"
    provider_params = [f"%{provider}%"]

    sql = f"""
    SELECT
        COALESCE(
            NULLIF(TRIM(issue_reasons::VARCHAR), ''),
            NULLIF(TRIM(issue_sources::VARCHAR), ''),
            'unknown'
        ) AS issue_key,
        COUNT(*) AS cnt
    FROM {TABLE_NAME}
    WHERE {provider_filter}
      AND {date_expr} >= CURRENT_DATE - {lookback_days}
      AND {_build_site_filter()}
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT {limit}
    """

    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            _safe_execute(cur, sql, tuple(provider_params))
            rows = [{"issue_key": r[0], "count": int(r[1])} for r in cur.fetchall()]

            filters_output = {
                "provider_like": provider,
                "issue_type": "site_related",
                "lookback_days": lookback_days
            }

            return json.dumps({
                "filters": filters_output,
                "rows": rows,
                "row_count": len(rows)
            }, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)


def issue_scope_breakdown(provider: str, lookback_days: int = 7, per_dim_limit: int = 10) -> str:
    """
    Analyze WHERE site-related issues are concentrated across multiple dimensions.

    This tool answers questions like:
    - "What is the scope of the issue?"
    - "What dimensions is this concentrated in?"
    - "Which POS/routes/cabin classes are most affected?"
    - "When do these issues occur? (hourly pattern)"

    Use this tool to understand the SCOPE and CONCENTRATION of issues, showing:
    - WHEN: Hourly observation patterns
    - WHERE (Geography): Point of sale (POS) distribution
    - WHAT (Product): Trip type, cabin class, length of stay
    - WHERE (Routes): Origin-destination pairs most affected
    - WHEN (Travel): Departure week and day-of-week patterns

    DIMENSIONS ANALYZED:
    --------------------
    1. obs_hour: Observation hour (when issue was detected)
       - Shows if issues happen at specific times of day
       - Example: "2025-10-16 14:00:00"

    2. pos: Point of Sale (where customer is searching from)
       - Geographic distribution of issues
       - Examples: "US", "CA", "GB", "DE"

    3. triptype: Trip Type
       - Round-trip vs one-way distribution
       - Values: "R" (round-trip), "O" (one-way)

    4. los: Length of Stay (in days)
       - How long between departure and return
       - Examples: "3", "7", "14"

    5. od: Origin-Destination pairs (routes)
       - Which routes are most affected
       - Examples: "JFK-LAX", "ORD-DFW", "ATL-LAS"

    6. cabin: Cabin Class
       - Which cabins have issues
       - Examples: "Economy", "Business", "First"

    7. depart_week: Departure Week
       - Which travel weeks are affected
       - Example: "2025-10-20" (week starting date)

    8. depart_dow: Departure Day of Week
       - Which days of week for departure
       - Values: 0 (Sunday) through 6 (Saturday)

    INPUT PARAMETERS:
    ------------------
    provider (str, REQUIRED):
        Provider code or name text to match (simple ILIKE)

        Format options:
        - Uses case-insensitive matching with SQL ILIKE '%pattern%'
        - Examples: "AA", "Delta", "United"

    lookback_days (int, OPTIONAL, default=7):
        How many days of data to analyze
        - Default: 7 (last week)
        - Examples: 3 (last 3 days), 14 (last 2 weeks), 30 (last month)
        - Range: 1 to 365

    per_dim_limit (int, OPTIONAL, default=10):
        Maximum rows to return per dimension
        - Default: 10 (top 10 values per dimension)
        - Maximum: 25
        - Shows most concentrated values first

    RETURNS:
    --------
    JSON string with:
    - filters (object): Filters applied
        * provider_like: Provider pattern searched
        * issue_type: Always "site_related"
        * lookback_days: Days analyzed
    - total_count (int): Total site issues found for this provider
    - available_dimensions (list): Which dimensions had data (may be subset of 8)
    - breakdowns (object): For each dimension, array of:
        * bucket: The dimension value (e.g., "US" for POS, "JFK-LAX" for od)
        * count: Number of issues for this value
        * pct: Percentage of total issues (0.0 to 1.0)

    EXAMPLE USAGE:
    --------------
    User: "What is the scope of the issue? What dimensions is this concentrated in?"
    Action: issue_scope_breakdown(provider="AA", lookback_days=7, per_dim_limit=10)
    Response interpretation:
        - Check breakdowns.obs_hour → "Issues concentrated in 14:00-16:00 hours"
        - Check breakdowns.pos → "80% of issues from US point of sale"
        - Check breakdowns.od → "JFK-LAX route has 45% of issues"

    User: "Where are the AA site issues concentrated?"
    Action: issue_scope_breakdown(provider="AA", lookback_days=7, per_dim_limit=15)

    User: "Show me the scope of Delta's site problems across all dimensions"
    Action: issue_scope_breakdown(provider="Delta", lookback_days=14, per_dim_limit=20)

    WHEN TO USE:
    ------------
    - User asks about "scope" of issues
    - User asks "what dimensions" or "where concentrated"
    - User wants to see distribution across POS, routes, cabin, time
    - After using top_site_issues() to understand WHERE issues occur

    WHEN NOT TO USE:
    ----------------
    - To see WHAT issues (use top_site_issues instead)
    - For invalid-request issues (use query_audit with {{IS_INVALID}})
    - For single dimension analysis (use query_audit with GROUP BY)
    """
    per_dim_limit = min(max(1, per_dim_limit), 25)
    cols = _get_columns()

    # Identify columns
    provider_col = _pick_column(cols, ["providercode", "provider"]) or "providercode"
    date_col = _pick_column(cols, ["scheduledate"]) or "scheduledate"
    date_expr = _date_expr(date_col, cols.get(date_col, ""))

    pos_col = _pick_column(cols, ["pos", "point_of_sale"])
    cabin_col = _pick_column(cols, ["cabin", "bookingcabin"])
    depart_col = _pick_column(cols, ["departdate", "legdeparturedate"])

    has_od = "originairportcode" in cols and "destinationairportcode" in cols

    # Simple provider pattern
    provider_filter = f"{provider_col} ILIKE %s"
    provider_params = [f"%{provider}%"]

    # Base filter
    base_where = f"""
    WHERE {provider_filter}
      AND {date_expr} >= CURRENT_DATE - {lookback_days}
      AND {_build_site_filter()}
    """

    filters_output = {
        "provider_like": provider,
        "issue_type": "site_related",
        "lookback_days": lookback_days
    }

    result = {
        "filters": filters_output,
        "total_count": 0,
        "available_dimensions": [],
        "breakdowns": {}
    }

    try:
        conn = _get_conn()

        # Get total count
        with conn.cursor() as cur:
            _safe_execute(cur, f"SELECT COUNT(*) FROM {TABLE_NAME} {base_where}", tuple(provider_params))
            result["total_count"] = int(cur.fetchone()[0])

        # Helper to run dimension breakdown
        def run_dimension(name: str, select_expr: str, extra_where: str = ""):
            sql = f"""
            SELECT {select_expr} AS bucket, COUNT(*) AS cnt
            FROM {TABLE_NAME}
            {base_where} {extra_where}
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT {per_dim_limit}
            """
            try:
                with conn.cursor() as cur:
                    _safe_execute(cur, sql, tuple(provider_params))
                    rows = []
                    for bucket, cnt in cur.fetchall():
                        pct = cnt / result["total_count"] if result["total_count"] > 0 else 0
                        rows.append({
                            "bucket": str(bucket) if bucket is not None else "null",
                            "count": int(cnt),
                            "pct": round(pct, 4)
                        })
                    if rows:
                        result["breakdowns"][name] = rows
                        result["available_dimensions"].append(name)
            except Exception:
                pass  # Skip dimension if query fails

        # Run breakdowns for available dimensions
        run_dimension("obs_hour", f"DATE_TRUNC('hour', {_build_event_ts()})")

        if pos_col:
            run_dimension("pos", f"NULLIF(TRIM({pos_col}::VARCHAR), '')", f"AND {pos_col} IS NOT NULL")

        if has_od:
            run_dimension("od", "(originairportcode || '-' || destinationairportcode)")

        if cabin_col:
            run_dimension("cabin", f"NULLIF(TRIM({cabin_col}::VARCHAR), '')", f"AND {cabin_col} IS NOT NULL")

        if depart_col:
            depart_expr = _date_expr(depart_col, cols.get(depart_col, ""))
            run_dimension("depart_week", f"DATE_TRUNC('week', {depart_expr})")

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)
