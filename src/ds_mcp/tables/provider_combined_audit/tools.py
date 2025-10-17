"""
MCP tools for monitoring_prod.provider_combined_audit.

Goal: keep each tool minimal — build a simple SELECT with a few helpful macros,
execute it, and return rows/columns as JSON. Follows MCP guidance for small,
predictable tools with clear docstrings and safe execution.
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

## Operate across all issues without extra classification helpers.


def _build_event_ts(alias: Optional[str] = None) -> str:
    """Event timestamp used by macros (prefer observationtimestamp)."""
    expr = "COALESCE(observationtimestamp, actualscheduletimestamp)"
    return f"{expr} AS {alias}" if alias else expr


def _sales_date_bound(days: int) -> str:
    """Predicate to prune by partition: sales_date >= YYYYMMDD int for current_date - days."""
    return (
        f"sales_date >= CAST(TO_CHAR(CURRENT_DATE - {days}, 'YYYYMMDD') AS INT)"
    )


def _today_int() -> str:
    """Returns SQL expression for today's YYYYMMDD int."""
    return "CAST(TO_CHAR(CURRENT_DATE, 'YYYYMMDD') AS INT)"


# ----------------------------------------------------------------------------
# SQL Macro Expansion
# ----------------------------------------------------------------------------

def _expand_macros(sql: str) -> str:
    """Expand convenient macros in SQL queries used by these tools."""
    result = sql

    # {{EVENT_TS}} and optional alias: {{EVENT_TS:alias}}
    def expand_event_ts(match):
        alias = match.group(1)
        return _build_event_ts(alias)

    result = re.sub(r"\{\{EVENT_TS(?::([A-Za-z_][A-Za-z0-9_]*))?\}\}", expand_event_ts, result)

    # Simple replacements
    result = result.replace("{{PCA}}", TABLE_NAME)
    result = result.replace("{{OD}}", "(originairportcode || '-' || destinationairportcode)")
    result = result.replace("{{OBS_HOUR}}", f"DATE_TRUNC('hour', {_build_event_ts()})")

    # Normalized issue label and site presence predicates
    # Issue label: treat empty issue_sources as OK and do not fallback to 'unknown'.
    # Use only non-empty issue_sources; callers should filter with
    #   AND NULLIF(TRIM(issue_sources::VARCHAR), '') IS NOT NULL
    # when aggregating.
    result = result.replace(
        "{{ISSUE_TYPE}}",
        "NULLIF(TRIM(issue_sources::VARCHAR), '')",
    )
    result = result.replace(
        "{{IS_SITE}}",
        "NULLIF(TRIM(sitecode::VARCHAR), '') IS NOT NULL",
    )
    # Placeholder for invalid markers – default to FALSE to be safe
    result = result.replace("{{IS_INVALID}}", "FALSE")

    return result

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


## Assume canonical column names present in provider_combined_audit
PROVIDER_COL = "providercode"
SITE_COL = "sitecode"
DATE_COL = "sales_date"
POS_COL = "pos"
CABIN_COL = "cabin"
TRIPTYPE_COL = "triptype"
LOS_COL = "los"
DEPARTDATE_COL = "departdate"
ORIGIN_COL = "originairportcode"
DEST_COL = "destinationairportcode"


def _date_expr(col: str, dtype: str) -> str:
    """Convert a YYYYMMDD INT or DATE/TIMESTAMP column to DATE.

    Minimal rule: if dtype contains 'date' or 'timestamp', return column; otherwise
    treat as YYYYMMDD integer and convert via TO_DATE.
    """
    dtype_lower = (dtype or "").lower()
    if "timestamp" in dtype_lower or "date" in dtype_lower:
        return col
    return f"TO_DATE({col}::VARCHAR, 'YYYYMMDD')"

def _execute_select(sql_query: str, params=None, max_rows: int = 100) -> str:
    """Run a read-only SELECT (or WITH ... SELECT), expand macros, and return JSON."""
    expanded_sql = _expand_macros(sql_query)
    sql_upper = expanded_sql.upper().strip()

    # Safety checks
    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
        return json.dumps({"error": "Only SELECT or WITH ... SELECT queries allowed"}, indent=2)
    forbidden = [
        "DELETE", "UPDATE", "INSERT", "DROP", "TRUNCATE", "ALTER",
        "CREATE", "COPY", "UNLOAD", "GRANT", "REVOKE",
    ]
    for kw in forbidden:
        if re.search(rf"\b{kw}\b", sql_upper):
            return json.dumps({"error": f"Forbidden keyword: {kw}"}, indent=2)

    truncated = False
    # Append LIMIT only if not already present (robust across newlines/spacing)
    if not re.search(r"\blimit\b\s+\d+", expanded_sql, flags=re.IGNORECASE):
        expanded_sql = expanded_sql.rstrip(";") + f" LIMIT {max_rows}"
        truncated = True

    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            log.info(f"SQL[execute]: {expanded_sql.strip()} | params={params}")
            _safe_execute(cur, expanded_sql, tuple(params) if params else None)
            columns = [d[0] for d in cur.description]
            records = cur.fetchmany(max_rows)
            rows = []
            for rec in records:
                out = {}
                for col, val in zip(columns, rec):
                    if val is not None and not isinstance(val, (str, int, float, bool)):
                        val = str(val)
                    out[col] = val
                rows.append(out)
            return json.dumps({
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
                "truncated": truncated or (len(rows) == max_rows),
                "sql": expanded_sql,
            }, indent=2)
    except Exception as e:
        log.error(f"execute error: {e}")
        return json.dumps({"error": str(e)}, indent=2)


# ============================================================================
# MCP Tools
# ============================================================================

def query_audit(sql_query: str, params: Optional[List] = None) -> str:
    """
    Execute a read-only SELECT (or WITH ... SELECT) with macro expansion.

    Args:
        sql_query: SQL with optional macros.
        params: Optional parameters tuple for placeholders (e.g., ILIKE %s).

    Macros:
      - {{PCA}}: monitoring_prod.provider_combined_audit
      - {{EVENT_TS[:alias]}}: COALESCE(observationtimestamp, actualscheduletimestamp)
      - {{OBS_HOUR}}: DATE_TRUNC('hour', {{EVENT_TS}})
      - {{OD}}: originairportcode || '-' || destinationairportcode
      - {{ISSUE_TYPE}}: normalized issue label (reasons/sources)
      - {{IS_SITE}}: sitecode present predicate

    Returns:
        JSON with keys: columns, rows, row_count, truncated, sql
    """
    return _execute_select(sql_query, params=params, max_rows=100)


def get_table_schema() -> str:
    """
    Get table schema for monitoring_prod.provider_combined_audit.

    Returns:
        JSON rows from SVV_COLUMNS: column_name, data_type, character_maximum_length, is_nullable
    """
    return _execute_select(
        """
        SELECT column_name, data_type, character_maximum_length, is_nullable
        FROM svv_columns
        WHERE table_schema = 'monitoring_prod' AND table_name = 'provider_combined_audit'
        ORDER BY ordinal_position
        """
    )


def top_site_issues(provider: str, lookback_days: int = 7, limit: int = 10) -> str:
    """
    Top site-related issues for a provider.

    Args:
        provider: ILIKE pattern (e.g., 'AA' or 'AA%').
        lookback_days: Window in days (partition-pruned by sales_date).
        limit: Max groups to return (1–50).

    Returns:
        JSON with columns [issue_key, cnt] and rows limited to 'limit'.
    """
    limit = min(max(1, limit), 50)
    date_expr = _date_expr(DATE_COL, "bigint")
    sql = (
        "SELECT {{ISSUE_TYPE}} AS issue_key, COUNT(*) AS cnt "
        "FROM {{PCA}} "
        f"WHERE {PROVIDER_COL} ILIKE %s "
        f"AND {_sales_date_bound(lookback_days)} "
        f"AND {date_expr} >= CURRENT_DATE - {lookback_days} "
        "AND NULLIF(TRIM(issue_sources::VARCHAR), '') IS NOT NULL "
        "GROUP BY 1 ORDER BY 2 DESC "
        f"LIMIT {limit}"
    )
    params = [f"%{provider}%"]
    return _execute_select(sql, params)

def issue_scope_quick_by_site(provider: str, site: str, lookback_days: int = 3, per_dim_limit: int = 5) -> str:
    """
    DEPRECATED: Quick scope for provider+site.

    This wrapper now calls issue_scope_combined with dims ['obs_hour','pos'] and returns
    a single table to replace the former multi-call behavior.
    """
    limit = min(max(1, per_dim_limit), 50)
    return issue_scope_combined(provider=provider, site=site, dims=["obs_hour", "pos"], lookback_days=lookback_days, limit=limit)


def issue_scope_by_site_dimensions(
    provider: str,
    site: str,
    dims: List[str],
    lookback_days: int = 3,
    per_dim_limit: int = 5,
) -> str:
    """
    DEPRECATED: Use issue_scope_combined instead.

    This wrapper now calls issue_scope_combined with the provided dims and returns
    a single table replacing the former per-dimension outputs.
    """
    limit = min(max(1, per_dim_limit), 1000)
    return issue_scope_combined(provider=provider, site=site, dims=dims, lookback_days=lookback_days, limit=limit)


def issue_scope_combined(
    provider: str,
    site: str,
    dims: Optional[List[str]] = None,
    lookback_days: int = 7,
    limit: int = 200,
) -> str:
    """
    Combined multi-dimensional scope for provider+site using ONE SQL statement.

    Args:
        provider: ILIKE pattern for providercode.
        site: ILIKE pattern for sitecode.
        dims: 2–4 dimensions from: obs_hour, pos, od, cabin, triptype, los, depart_week, depart_dow.
              If omitted or empty, defaults to ['obs_hour', 'pos', 'triptype', 'los'].
        lookback_days: Window in days.
        limit: Max rows to return (1–2000).

    Returns:
        Single table JSON with requested dimension columns and count.
    """
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
    # Default popular scope if not provided
    if not dims:
        dims = ["obs_hour", "pos", "triptype", "los"]
    dims_req = [d.strip().lower() for d in (dims or []) if d and d.strip().lower() in allowed]
    if not (2 <= len(dims_req) <= 4):
        return json.dumps({"error": "dims must contain 2 to 4 valid items"}, indent=2)

    limit = min(max(1, limit), 2000)
    date_expr = _date_expr(DATE_COL, "bigint")

    select_map = {
        "obs_hour": "{{OBS_HOUR}} AS obs_hour",
        "pos": f"NULLIF(TRIM({POS_COL}::VARCHAR), '') AS pos",
        "od": "(originairportcode || '-' || destinationairportcode) AS od",
        "cabin": f"NULLIF(TRIM({CABIN_COL}::VARCHAR), '') AS cabin",
        "triptype": f"NULLIF(TRIM({TRIPTYPE_COL}::VARCHAR), '') AS triptype",
        "los": f"{LOS_COL}::VARCHAR AS los",
        "depart_week": f"DATE_TRUNC('week', TO_DATE({DEPARTDATE_COL}::VARCHAR, 'YYYYMMDD')) AS depart_week",
        "depart_dow": f"EXTRACT(DOW FROM TO_DATE({DEPARTDATE_COL}::VARCHAR, 'YYYYMMDD'))::INT AS depart_dow",
    }

    # Build SELECT list and GROUP BY clause
    select_cols = [select_map[d] for d in dims_req]
    select_list = ", ".join(select_cols) + ", COUNT(*) AS cnt"
    group_by = ", ".join(str(i) for i in range(1, len(dims_req) + 1))

    # Base filters
    base_where = (
        f"WHERE {PROVIDER_COL} ILIKE %s "
        f"AND {SITE_COL} ILIKE %s "
        f"AND {_sales_date_bound(lookback_days)} "
        f"AND {date_expr} >= CURRENT_DATE - {lookback_days} "
        "AND NULLIF(TRIM(issue_sources::VARCHAR), '') IS NOT NULL "
    )

    # Additional not-null filters for specific dimensions to mirror 1D tools
    extra_filters = []
    if "pos" in dims_req:
        extra_filters.append(f"{POS_COL} IS NOT NULL")
    if "cabin" in dims_req:
        extra_filters.append(f"{CABIN_COL} IS NOT NULL")
    if "triptype" in dims_req:
        extra_filters.append(f"{TRIPTYPE_COL} IS NOT NULL")
    if "los" in dims_req:
        extra_filters.append(f"{LOS_COL} IS NOT NULL")

    extra_where = (" AND " + " AND ".join(extra_filters)) if extra_filters else ""

    sql = (
        "SELECT "
        + select_list
        + " FROM {{PCA}} "
        + f"{base_where}{extra_where} "
        + f"GROUP BY {group_by} "
        + "ORDER BY cnt DESC "
        + f"LIMIT {limit}"
    )
    params = [f"%{provider}%", f"%{site}%"]
    return _execute_select(sql, params)

def overview_site_issues_today(per_dim_limit: int = 50) -> str:
    """
    High-level overview for today across all providers using a single SQL query.

    Returns a single tabular result with columns:
      - issue_key (lowercased non-empty issue_sources)
      - provider (providercode)
      - pos (point-of-sale)
      - obs_hour (event hour)
      - cnt (row count)

    The query filters out rows where issue_sources is empty ("OK" rows).
    """
    per_dim_limit = min(max(1, per_dim_limit), 500)
    date_expr = _date_expr(DATE_COL, "bigint")
    sql = (
        "SELECT "
        "  NULLIF(TRIM(LOWER(issue_sources::VARCHAR)), '') AS issue_key, "
        f"  NULLIF(TRIM({PROVIDER_COL}::VARCHAR), '') AS provider, "
        f"  NULLIF(TRIM({POS_COL}::VARCHAR), '') AS pos, "
        "  {{OBS_HOUR}} AS obs_hour, "
        "  COUNT(*) AS cnt "
        "FROM {{PCA}} "
        f"WHERE sales_date = {_today_int()} "
        f"  AND {date_expr} >= CURRENT_DATE "
        "  AND NULLIF(TRIM(issue_sources::VARCHAR), '') IS NOT NULL "
        "GROUP BY 1, 2, 3, 4 "
        "ORDER BY 5 DESC "
        f"LIMIT {per_dim_limit}"
    )
    return _execute_select(sql)


def list_provider_sites(provider: str, lookback_days: int = 7, limit: int = 10) -> str:
    """
    Top site codes for a provider over a recent window.

    Args:
        provider: ILIKE pattern for providercode.
        lookback_days: Window in days.
        limit: Max site groups to return (1–50).

    Returns:
        JSON with columns [site, cnt] and rows limited to 'limit'.
    """
    limit = min(max(1, limit), 50)
    date_expr = _date_expr(DATE_COL, "bigint")
    sql = (
        f"SELECT NULLIF(TRIM({SITE_COL}::VARCHAR), '') AS site, COUNT(*) AS cnt "
        "FROM {{PCA}} "
        f"WHERE {PROVIDER_COL} ILIKE %s "
        f"AND {_sales_date_bound(lookback_days)} "
        f"AND {date_expr} >= CURRENT_DATE - {lookback_days} "
        "AND NULLIF(TRIM(issue_sources::VARCHAR), '') IS NOT NULL "
        f"AND NULLIF(TRIM({SITE_COL}::VARCHAR), '') IS NOT NULL "
        "GROUP BY 1 ORDER BY 2 DESC "
        f"LIMIT {limit}"
    )
    return _execute_select(sql, [f"%{provider}%"])
