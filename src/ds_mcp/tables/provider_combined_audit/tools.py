"""
MCP tools for prod.monitoring.provider_combined_audit.

Goal: keep each tool minimal — build a simple SELECT with a few helpful macros,
execute it, and return rows/columns as JSON. Follows MCP guidance for small,
predictable tools with clear docstrings and safe execution.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Dict, List, Optional

from ds_mcp.core.connectors import DatabaseConnectorFactory

log = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================

DATABASE_NAME = "prod"
SCHEMA_NAME = "monitoring"
TABLE_BASE = "provider_combined_audit"

if DATABASE_NAME:
    TABLE_NAME = f"{DATABASE_NAME}.{SCHEMA_NAME}.{TABLE_BASE}"
else:
    TABLE_NAME = f"{SCHEMA_NAME}.{TABLE_BASE}"


def _build_event_ts(alias: Optional[str] = None) -> str:
    """Event timestamp used by macros (prefer observationtimestamp)."""
    expr = "COALESCE(observationtimestamp, actualscheduletimestamp)"
    return f"{expr} AS {alias}" if alias else expr


def _sales_date_bound(days: int) -> str:
    """Predicate to prune by partition: sales_date >= YYYYMMDD int for current_date - days."""
    return f"sales_date >= CAST(TO_CHAR(CURRENT_DATE - {days}, 'YYYYMMDD') AS INT)"


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
    # Issue label: prefer explicit reasons, fall back to sources; exclude empties.
    # Callers should filter with:
    #   COALESCE(NULLIF(TRIM(issue_reasons::VARCHAR), ''), NULLIF(TRIM(issue_sources::VARCHAR), '')) IS NOT NULL
    result = result.replace(
        "{{ISSUE_TYPE}}",
        "COALESCE(NULLIF(TRIM(issue_reasons::VARCHAR), ''), NULLIF(TRIM(issue_sources::VARCHAR), ''))",
    )
    result = result.replace("{{IS_SITE}}", "NULLIF(TRIM(sitecode::VARCHAR), '') IS NOT NULL")
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
            query = [
                "SELECT column_name, data_type",
                "FROM svv_columns",
                "WHERE table_schema = %s",
                "  AND table_name = %s",
            ]
            params = [SCHEMA_NAME, TABLE_BASE]
            if DATABASE_NAME:
                query.append("  AND table_catalog = %s")
                params.append(DATABASE_NAME)
            query.append("ORDER BY ordinal_position")
            sql = "\n".join(query)
            _safe_execute(cur, sql, params)
            return {row[0]: row[1] for row in cur.fetchall()}
        except Exception:
            return {}


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
    """Convert a YYYYMMDD INT or DATE/TIMESTAMP column to DATE."""
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
        "DELETE",
        "UPDATE",
        "INSERT",
        "DROP",
        "TRUNCATE",
        "ALTER",
        "CREATE",
        "COPY",
        "UNLOAD",
        "GRANT",
        "REVOKE",
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
            return json.dumps(
                {
                    "columns": columns,
                    "rows": rows,
                    "row_count": len(rows),
                    "truncated": truncated or (len(rows) == max_rows),
                    "sql": expanded_sql,
                },
                indent=2,
            )
    except Exception as e:
        log.error(f"execute error: {e}")
        return json.dumps({"error": str(e)}, indent=2)


# ============================================================================
# MCP Tools
# ============================================================================


def query_audit(sql_query: str, params: Optional[List] = None) -> str:
    """
    Execute a read-only SELECT (or WITH ... SELECT) with macro expansion.

    Best practices:
    - Use sales_date (YYYYMMDD int) for all partition filters and date ranges.
    - Filter out "OK" rows by requiring non-empty COALESCE(issue_reasons, issue_sources).
    - Keep queries read-only; DDL/DML are rejected.

    Macros:
    - {{PCA}}: prod.monitoring.provider_combined_audit
    - {{EVENT_TS[:alias]}}: COALESCE(observationtimestamp, actualscheduletimestamp)
    - {{OBS_HOUR}}: DATE_TRUNC('hour', {{EVENT_TS}})
    - {{OD}}: originairportcode || '-' || destinationairportcode
    - {{ISSUE_TYPE}}: COALESCE(NULLIF(TRIM(issue_reasons), ''), NULLIF(TRIM(issue_sources), ''))

    Example (today vs usual daily average over last 7 days, partition-safe):
    SELECT {{ISSUE_TYPE}} AS issue_key,
           SUM(CASE WHEN sales_date = CAST(TO_CHAR(CURRENT_DATE,'YYYYMMDD') AS INT) THEN 1 ELSE 0 END) AS today_count,
           SUM(CASE WHEN sales_date BETWEEN CAST(TO_CHAR(CURRENT_DATE-7,'YYYYMMDD') AS INT)
                              AND CAST(TO_CHAR(CURRENT_DATE-1,'YYYYMMDD') AS INT)
                    THEN 1 ELSE 0 END)::FLOAT / 7.0 AS usual_daily_avg
    FROM {{PCA}}
    WHERE providercode ILIKE '%QL2%'
      AND sitecode ILIKE '%AV%'
      AND {{ISSUE_TYPE}} IS NOT NULL
      AND sales_date >= CAST(TO_CHAR(CURRENT_DATE-7,'YYYYMMDD') AS INT)
    GROUP BY 1
    ORDER BY today_count DESC, usual_daily_avg DESC
    LIMIT 10;

    Returns JSON with: columns, rows, row_count, truncated, sql
    """
    return _execute_select(sql_query, params=params, max_rows=100)


def get_table_schema() -> str:
    """Get table schema for prod.monitoring.provider_combined_audit."""
    query_parts = [
        "SELECT column_name, data_type, character_maximum_length, is_nullable",
        "FROM svv_columns",
        "WHERE table_schema = %s",
        "  AND table_name = %s",
    ]
    params: List[str] = [SCHEMA_NAME, TABLE_BASE]
    if DATABASE_NAME:
        query_parts.append("  AND table_catalog = %s")
        params.append(DATABASE_NAME)
    query_parts.append("ORDER BY ordinal_position")
    sql = "\n".join(query_parts)
    return _execute_select(sql, params=params)


def top_site_issues(
    provider: str,
    site: str | None = None,
    lookback_days: int = 7,
    limit: int = 10,
) -> str:
    """Top site-related issues for a provider, optionally filtered by site code."""
    limit = min(max(1, limit), 50)
    date_expr = _date_expr(DATE_COL, "bigint")
    params = [f"%{provider}%"]
    site_filter = ""
    if site and site.strip():
        site_filter = f"  AND {SITE_COL} ILIKE %s "
        params.append(f"%{site.strip()}%")
    sql = (
        "SELECT "
        "NULLIF(TRIM(issue_sources::VARCHAR), '') AS issue_source, "
        "NULLIF(TRIM(issue_reasons::VARCHAR), '') AS issue_reason, "
        "COUNT(*) AS cnt "
        "FROM {{PCA}} "
        f"WHERE {PROVIDER_COL} ILIKE %s "
        f"{site_filter}"
        f"AND {_sales_date_bound(lookback_days)} "
        f"AND {date_expr} >= CURRENT_DATE - {lookback_days} "
        "AND ("
        "  NULLIF(TRIM(issue_sources::VARCHAR), '') IS NOT NULL "
        "  OR NULLIF(TRIM(issue_reasons::VARCHAR), '') IS NOT NULL "
        ") "
        "GROUP BY 1, 2 "
        "ORDER BY cnt DESC "
        f"LIMIT {limit}"
    )
    return _execute_select(sql, params)


# Note: No deprecated wrappers; use issue_scope_combined directly for multi-dimension scope.


def issue_scope_combined(
    provider: str,
    site: str,
    dims: List[str],
    lookback_days: int = 7,
    limit: int = 200,
) -> str:
    """
    Combined multi-dimensional scope for provider+site using one SQL.

    - dims: choose 2–4 from [obs_hour, pos, od, cabin, triptype, los, depart_week, depart_dow].
    - Uses sales_date partition for time pruning and {{ISSUE_TYPE}} for issue labels.
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

    select_cols = [select_map[d] for d in dims_req]
    select_list = ", ".join(select_cols) + ", COUNT(*) AS cnt"
    group_by = ", ".join(str(i) for i in range(1, len(dims_req) + 1))

    base_where = (
        f"WHERE {PROVIDER_COL} ILIKE %s "
        f"AND {SITE_COL} ILIKE %s "
        f"AND {_sales_date_bound(lookback_days)} "
        f"AND {date_expr} >= CURRENT_DATE - {lookback_days} "
        "AND COALESCE(NULLIF(TRIM(issue_reasons::VARCHAR), ''), NULLIF(TRIM(issue_sources::VARCHAR), '')) IS NOT NULL "
    )

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
    High-level overview for today across all providers (single SQL).

    - Uses sales_date partition equality to constrain to today.
    - Labels via {{ISSUE_TYPE}}; filters out empty labels.
    """
    per_dim_limit = min(max(1, per_dim_limit), 500)
    date_expr = _date_expr(DATE_COL, "bigint")
    sql = (
        "SELECT "
        "  NULLIF(TRIM(LOWER(COALESCE(issue_reasons::VARCHAR, issue_sources::VARCHAR))), '') AS issue_key, "
        f"  NULLIF(TRIM({PROVIDER_COL}::VARCHAR), '') AS provider, "
        f"  NULLIF(TRIM({POS_COL}::VARCHAR), '') AS pos, "
        "  {{OBS_HOUR}} AS obs_hour, "
        "  COUNT(*) AS cnt "
        "FROM {{PCA}} "
        f"WHERE sales_date = {_today_int()} "
        f"  AND {date_expr} >= CURRENT_DATE "
        "  AND COALESCE(NULLIF(TRIM(issue_reasons::VARCHAR), ''), NULLIF(TRIM(issue_sources::VARCHAR), '')) IS NOT NULL "
        "GROUP BY 1, 2, 3, 4 "
        "ORDER BY 5 DESC "
        f"LIMIT {per_dim_limit}"
    )
    return _execute_select(sql)


def list_provider_sites(provider: str, lookback_days: int = 7, limit: int = 10) -> str:
    """Top site codes for a provider over a recent window."""
    limit = min(max(1, limit), 50)
    date_expr = _date_expr(DATE_COL, "bigint")
    sql = (
        f"SELECT NULLIF(TRIM({SITE_COL}::VARCHAR), '') AS site, COUNT(*) AS cnt "
        "FROM {{PCA}} "
        f"WHERE {PROVIDER_COL} ILIKE %s "
        f"AND {_sales_date_bound(lookback_days)} "
        f"AND {date_expr} >= CURRENT_DATE - {lookback_days} "
        "AND COALESCE(NULLIF(TRIM(issue_reasons::VARCHAR), ''), NULLIF(TRIM(issue_sources::VARCHAR), '')) IS NOT NULL "
        f"AND NULLIF(TRIM({SITE_COL}::VARCHAR), '') IS NOT NULL "
        "GROUP BY 1 ORDER BY 2 DESC "
        f"LIMIT {limit}"
    )
    return _execute_select(sql, [f"%{provider}%"])


def top_site_issues_today_vs_usual(
    provider: str,
    site: str,
    lookback_days: int = 7,
    limit: int = 10,
) -> str:
    """
    Compare today's site issues vs. usual (average per day over the prior window).

    Uses the partition column sales_date for all time filtering to keep scans efficient.

    Returns columns: issue_key, today_count, usual_daily_avg
    """
    limit = min(max(1, limit), 50)
    # Precompute integer bounds for partition pruning
    lower_bound = f"CAST(TO_CHAR(CURRENT_DATE - {lookback_days}, 'YYYYMMDD') AS INT)"
    upper_bound = f"CAST(TO_CHAR(CURRENT_DATE - 1, 'YYYYMMDD') AS INT)"
    sql = (
        "SELECT {{ISSUE_TYPE}} AS issue_key,\n"
        f"  SUM(CASE WHEN {DATE_COL} = {{TODAY}} THEN 1 ELSE 0 END) AS today_count,\n"
        f"  SUM(CASE WHEN {DATE_COL} BETWEEN {lower_bound} AND {upper_bound} THEN 1 ELSE 0 END)::FLOAT / {lookback_days} AS usual_daily_avg\n"
        f"FROM {{PCA}}\n"
        f"WHERE {PROVIDER_COL} ILIKE %s AND {SITE_COL} ILIKE %s\n"
        "  AND COALESCE(NULLIF(TRIM(issue_reasons::VARCHAR), ''), NULLIF(TRIM(issue_sources::VARCHAR), '')) IS NOT NULL\n"
        f"  AND {DATE_COL} >= {lower_bound}\n"
        "GROUP BY 1\n"
        "ORDER BY today_count DESC, usual_daily_avg DESC\n"
        f"LIMIT {limit}"
    )
    # Expand {{TODAY}} separately to keep code readable
    sql = sql.replace("{TODAY}", _today_int())
    return _execute_select(sql, [f"%{provider}%", f"%{site}%"])
