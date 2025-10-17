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


def _sales_date_bound(days: int) -> str:
    """Predicate to prune by partition: sales_date >= YYYYMMDD int for current_date - days."""
    return (
        f"sales_date >= CAST(TO_CHAR(CURRENT_DATE - {days}, 'YYYYMMDD') AS INT)"
    )


def _today_int() -> str:
    """Returns SQL expression for today's YYYYMMDD int."""
    return "CAST(TO_CHAR(CURRENT_DATE, 'YYYYMMDD') AS INT)"


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
    Run a read‑only SELECT (or WITH ... SELECT) on monitoring_prod.provider_combined_audit.
    Supports simple macros (e.g., {{PCA}}, {{IS_SITE}}, {{OD}}). Adds LIMIT 100 if missing.
    Returns JSON: columns, rows, row_count, truncated.
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
            log.info(f"SQL[query_audit]: {expanded_sql.strip()}")
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
    List columns and data types for monitoring_prod.provider_combined_audit.
    Returns JSON: table, columns, notes.
    """
    cols = _get_columns()
    columns = [{"column_name": k, "data_type": v} for k, v in cols.items()]

    schema_sql = (
        "SELECT column_name, data_type, character_maximum_length, is_nullable "
        "FROM svv_columns WHERE table_schema = 'monitoring_prod' AND table_name = 'provider_combined_audit' "
        "ORDER BY ordinal_position"
    )
    return json.dumps({
        "table": TABLE_NAME,
        "columns": columns,
        "notes": {
            "issue_fields": ["issue_sources", "issue_reasons"],
            "date_fields": ["scheduledate", "actualscheduletimestamp", "observationtimestamp"],
            "search_dimensions": ["pos", "cabin", "originairportcode", "destinationairportcode"],
            "travel_dates": ["departdate", "returndate"]
        },
        "sql": schema_sql
    }, indent=2)


def top_site_issues(provider: str, lookback_days: int = 7, limit: int = 10) -> str:
    """
    Top site‑related issues for a provider (ILIKE match on providercode).
    Args: provider, lookback_days (default 7), limit (default 10, max 50).
    Returns JSON: filters, rows [{issue_key, count}], row_count.
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
      AND {_sales_date_bound(lookback_days)}
      AND {date_expr} >= CURRENT_DATE - {lookback_days}
      AND {_build_site_filter()}
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT {limit}
    """

    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            log.info(f"SQL[top_site_issues]: {sql.strip()} | params={provider_params}")
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
                "row_count": len(rows),
                "sql": sql
            }, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)


# Note: removed issue_scope_breakdown (non-site) for simplicity as requested.


def issue_scope_breakdown_by_site(provider: str, site: str, lookback_days: int = 7, per_dim_limit: int = 10) -> str:
    """
    Scope of site issues for a provider+site across: obs_hour, pos, od, cabin, depart_week.
    Args: provider, site, lookback_days (default 7), per_dim_limit (default 10, max 25).
    Returns JSON: filters, total_count, available_dimensions, breakdowns.
    """
    per_dim_limit = min(max(1, per_dim_limit), 25)
    cols = _get_columns()

    provider_col = _pick_column(cols, ["providercode", "provider"]) or "providercode"
    site_col = _pick_column(cols, ["sitecode", "site", "site_code"]) or "sitecode"
    date_col = _pick_column(cols, ["scheduledate"]) or "scheduledate"
    date_expr = _date_expr(date_col, cols.get(date_col, ""))

    pos_col = _pick_column(cols, ["pos", "point_of_sale"])
    cabin_col = _pick_column(cols, ["cabin", "bookingcabin"])
    depart_col = _pick_column(cols, ["departdate", "legdeparturedate"])
    has_od = "originairportcode" in cols and "destinationairportcode" in cols

    provider_filter = f"{provider_col} ILIKE %s"
    site_filter = f"{site_col} ILIKE %s"
    params = [f"%{provider}%", f"%{site}%"]

    base_where = f"""
    WHERE {provider_filter}
      AND {site_filter}
      AND {_sales_date_bound(lookback_days)}
      AND {date_expr} >= CURRENT_DATE - {lookback_days}
      AND {_build_site_filter()}
    """

    filters_output = {
        "provider_like": provider,
        "site_like": site,
        "issue_type": "site_related",
        "lookback_days": lookback_days,
    }

    result = {
        "filters": filters_output,
        "total_count": 0,
        "available_dimensions": [],
        "breakdowns": {}
    }

    try:
        conn = _get_conn()

        executed_sql = {"total_count": f"SELECT COUNT(*) FROM {TABLE_NAME} {base_where}"}
        with conn.cursor() as cur:
            log.info(f"SQL[issue_scope_quick_by_site/total]: {executed_sql['total_count'].strip()} | params={params}")
            _safe_execute(cur, executed_sql["total_count"], tuple(params))
            result["total_count"] = int(cur.fetchone()[0])

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
                    executed_sql[name] = sql
                    log.info(f"SQL[issue_scope_quick_by_site/{name}]: {sql.strip()} | params={params}")
                    _safe_execute(cur, sql, tuple(params))
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
                pass

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

        result["executed_sql"] = executed_sql
        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)


def issue_scope_quick_by_site(provider: str, site: str, lookback_days: int = 3, per_dim_limit: int = 5) -> str:
    """
    Quick scope for provider+site (fast): returns only obs_hour and pos.
    Args: provider, site, lookback_days (default 3), per_dim_limit (default 5).
    Returns JSON: filters, total_count, available_dimensions, breakdowns.
    """
    per_dim_limit = min(max(1, per_dim_limit), 25)
    cols = _get_columns()

    provider_col = _pick_column(cols, ["providercode", "provider"]) or "providercode"
    site_col = _pick_column(cols, ["sitecode", "site", "site_code"]) or "sitecode"
    date_col = _pick_column(cols, ["scheduledate"]) or "scheduledate"
    date_expr = _date_expr(date_col, cols.get(date_col, ""))

    pos_col = _pick_column(cols, ["pos", "point_of_sale"])

    provider_filter = f"{provider_col} ILIKE %s"
    site_filter = f"{site_col} ILIKE %s"
    params = [f"%{provider}%", f"%{site}%"]

    base_where = f"""
    WHERE {provider_filter}
      AND {site_filter}
      AND {_sales_date_bound(lookback_days)}
      AND {date_expr} >= CURRENT_DATE - {lookback_days}
      AND {_build_site_filter()}
    """

    filters_output = {
        "provider_like": provider,
        "site_like": site,
        "issue_type": "site_related",
        "lookback_days": lookback_days,
    }

    result = {
        "filters": filters_output,
        "total_count": 0,
        "available_dimensions": [],
        "breakdowns": {}
    }

    try:
        conn = _get_conn()

        with conn.cursor() as cur:
            total_sql = f"SELECT COUNT(*) FROM {TABLE_NAME} {base_where}"
            log.info(f"SQL[issue_scope_by_site_dimensions/total]: {total_sql.strip()} | params={params}")
            _safe_execute(cur, total_sql, tuple(params))
            result["total_count"] = int(cur.fetchone()[0])

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
                    _safe_execute(cur, sql, tuple(params))
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
                pass

        # Only quick dimensions
        run_dimension("obs_hour", f"DATE_TRUNC('hour', {_build_event_ts()})")
        if pos_col:
            run_dimension("pos", f"NULLIF(TRIM({pos_col}::VARCHAR), '')", f"AND {pos_col} IS NOT NULL")

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)


def issue_scope_by_site_dimensions(
    provider: str,
    site: str,
    dims: List[str],
    lookback_days: int = 3,
    per_dim_limit: int = 5,
) -> str:
    """
    Site-scoped scope across requested dimensions only (fast, partition-pruned).
    Args: provider, site, dims (subset of: obs_hour,pos,od,cabin,triptype,los,depart_week,depart_dow),
          lookback_days (default 3), per_dim_limit (default 5, max 25).
    Returns JSON: filters, total_count, available_dimensions, breakdowns.
    """
    per_dim_limit = min(max(1, per_dim_limit), 25)
    dims_req = {d.strip().lower() for d in dims or []}
    allowed = {
        "obs_hour",
        "pos",
        "od",
        "cabin",
        "triptype",
        "los",
        "depart_week",
        "depart_dow",
    }
    dims_req = [d for d in dims_req if d in allowed]
    if not dims_req:
        return json.dumps({"error": "No valid dims requested"}, indent=2)

    cols = _get_columns()
    provider_col = _pick_column(cols, ["providercode", "provider"]) or "providercode"
    site_col = _pick_column(cols, ["sitecode", "site", "site_code"]) or "sitecode"
    date_col = _pick_column(cols, ["scheduledate"]) or "scheduledate"
    date_expr = _date_expr(date_col, cols.get(date_col, ""))

    pos_col = _pick_column(cols, ["pos", "point_of_sale"])
    cabin_col = _pick_column(cols, ["cabin", "bookingcabin"])
    triptype_col = _pick_column(cols, ["triptype", "trip_type"])
    los_col = _pick_column(cols, ["los", "lengthofstay"])
    depart_col = _pick_column(cols, ["departdate", "legdeparturedate"])
    has_od = "originairportcode" in cols and "destinationairportcode" in cols

    provider_filter = f"{provider_col} ILIKE %s"
    site_filter = f"{site_col} ILIKE %s"
    params = [f"%{provider}%", f"%{site}%"]

    base_where = f"""
    WHERE {provider_filter}
      AND {site_filter}
      AND {_sales_date_bound(lookback_days)}
      AND {date_expr} >= CURRENT_DATE - {lookback_days}
      AND {_build_site_filter()}
    """

    filters_output = {
        "provider_like": provider,
        "site_like": site,
        "issue_type": "site_related",
        "lookback_days": lookback_days,
        "requested_dims": dims_req,
    }

    result = {
        "filters": filters_output,
        "total_count": 0,
        "available_dimensions": [],
        "breakdowns": {},
    }

    def add_rows(name: str, rows):
        if rows:
            result["breakdowns"][name] = rows
            result["available_dimensions"].append(name)

    try:
        conn = _get_conn()

        with conn.cursor() as cur:
            _safe_execute(cur, f"SELECT COUNT(*) FROM {TABLE_NAME} {base_where}", tuple(params))
            result["total_count"] = int(cur.fetchone()[0])

        executed_sql = {"total_count": total_sql}
        def run_dimension(name: str, select_expr: str, extra_where: str = ""):
            sql = f"""
            SELECT {select_expr} AS bucket, COUNT(*) AS cnt
            FROM {TABLE_NAME}
            {base_where} {extra_where}
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT {per_dim_limit}
            """
            with conn.cursor() as cur:
                executed_sql[name] = sql
                log.info(f"SQL[issue_scope_by_site_dimensions/{name}]: {sql.strip()} | params={params}")
                _safe_execute(cur, sql, tuple(params))
                rows = []
                for bucket, cnt in cur.fetchall():
                    pct = cnt / result["total_count"] if result["total_count"] > 0 else 0
                    rows.append({
                        "bucket": str(bucket) if bucket is not None else "null",
                        "count": int(cnt),
                        "pct": round(pct, 4),
                    })
                add_rows(name, rows)

        # Dispatch per requested dim
        if "obs_hour" in dims_req:
            run_dimension("obs_hour", f"DATE_TRUNC('hour', {_build_event_ts()})")
        if "pos" in dims_req and pos_col:
            run_dimension("pos", f"NULLIF(TRIM({pos_col}::VARCHAR), '')", f"AND {pos_col} IS NOT NULL")
        if "od" in dims_req and has_od:
            run_dimension("od", "(originairportcode || '-' || destinationairportcode)")
        if "cabin" in dims_req and cabin_col:
            run_dimension("cabin", f"NULLIF(TRIM({cabin_col}::VARCHAR), '')", f"AND {cabin_col} IS NOT NULL")
        if "triptype" in dims_req and triptype_col:
            run_dimension("triptype", f"NULLIF(TRIM({triptype_col}::VARCHAR), '')", f"AND {triptype_col} IS NOT NULL")
        if "los" in dims_req and los_col:
            run_dimension("los", f"{los_col}::VARCHAR", f"AND {los_col} IS NOT NULL")
        if "depart_week" in dims_req and depart_col:
            depart_expr = _date_expr(depart_col, cols.get(depart_col, ""))
            run_dimension("depart_week", f"DATE_TRUNC('week', {depart_expr})")
        if "depart_dow" in dims_req and depart_col:
            depart_expr = _date_expr(depart_col, cols.get(depart_col, ""))
            run_dimension("depart_dow", f"EXTRACT(DOW FROM {depart_expr})::INT")

        result["executed_sql"] = executed_sql
        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)


def overview_site_issues_today(per_dim_limit: int = 5) -> str:
    """
    Overview of site-related issues today across all providers (fast, succinct).
    Returns JSON: date (YYYYMMDD), total_count, breakdowns: issues, providers, pos, obs_hour.
    """
    per_dim_limit = min(max(1, per_dim_limit), 25)
    cols = _get_columns()

    provider_col = _pick_column(cols, ["providercode", "provider"]) or "providercode"
    date_col = _pick_column(cols, ["scheduledate"]) or "scheduledate"
    date_expr = _date_expr(date_col, cols.get(date_col, ""))
    pos_col = _pick_column(cols, ["pos", "point_of_sale"])

    today_expr = _today_int()

    base_where = f"""
    WHERE sales_date = {today_expr}
      AND {date_expr} >= CURRENT_DATE
      AND {_build_site_filter()}
    """

    result = {
        "date": None,
        "total_count": 0,
        "breakdowns": {}
    }

    try:
        conn = _get_conn()

        # total_count and date
        with conn.cursor() as cur:
            total_sql = f"SELECT {today_expr} AS yyyymmdd, COUNT(*) FROM {TABLE_NAME} {base_where}"
            log.info(f"SQL[overview_site_issues_today/total]: {total_sql.strip()}")
            _safe_execute(cur, total_sql)
            row = cur.fetchone()
            result["date"] = int(row[0]) if row and row[0] is not None else None
            result["total_count"] = int(row[1]) if row and row[1] is not None else 0

        executed_sql = {"total_count": total_sql}
        def run_dim(name: str, sql: str):
            with conn.cursor() as cur:
                executed_sql[name] = sql
                log.info(f"SQL[overview_site_issues_today/{name}]: {sql.strip()}")
                _safe_execute(cur, sql)
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

        # Normalize issues by lowercasing to avoid duplicates like 'No Response' vs 'no response'
        issues_sql = f"""
        SELECT NULLIF(TRIM(LOWER(COALESCE(issue_reasons::VARCHAR, issue_sources::VARCHAR))), '') AS issue_key,
               COUNT(*) AS cnt
        FROM {TABLE_NAME}
        {base_where}
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT {per_dim_limit}
        """
        run_dim("issues", issues_sql)

        providers_sql = f"""
        SELECT NULLIF(TRIM({provider_col}::VARCHAR), '') AS provider, COUNT(*) AS cnt
        FROM {TABLE_NAME}
        {base_where}
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT {per_dim_limit}
        """
        run_dim("providers", providers_sql)

        if pos_col:
            pos_sql = f"""
            SELECT NULLIF(TRIM({pos_col}::VARCHAR), '') AS pos, COUNT(*) AS cnt
            FROM {TABLE_NAME}
            {base_where}
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT {per_dim_limit}
            """
            run_dim("pos", pos_sql)

        obs_hour_sql = f"""
        SELECT DATE_TRUNC('hour', {_build_event_ts()}) AS obs_hour, COUNT(*) AS cnt
        FROM {TABLE_NAME}
        {base_where}
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT {per_dim_limit}
        """
        run_dim("obs_hour", obs_hour_sql)

        result["executed_sql"] = executed_sql
        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)
