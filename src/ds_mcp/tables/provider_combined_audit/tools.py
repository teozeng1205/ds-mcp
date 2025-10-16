"""
MCP tools for monitoring_prod.provider_combined_audit

These tools allow safe exploration and summarization of the provider audit data.
They support direct SQL (validated), schema inspection, distinct values, and
high-level summaries suitable for conversational usage, e.g.:

- "In provider combined audit, tell me about provider 'AaPts' today"
- "Give me an overview of the status today"
"""

import json
import logging
import re
from typing import List

from ds_mcp.core.connectors import DatabaseConnectorFactory

log = logging.getLogger(__name__)

SCHEMA_NAME = "monitoring_prod"
TABLE_BASE = "provider_combined_audit"
TABLE_NAME = f"{SCHEMA_NAME}.{TABLE_BASE}"


def _get_connector():
    """Get the database connector for this table."""
    return DatabaseConnectorFactory.get_connector("analytics")


def _list_columns(cursor) -> List[str]:
    """List columns for the table with fallback to SVV_COLUMNS."""
    cursor.execute(
        f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{SCHEMA_NAME}'
          AND table_name = '{TABLE_BASE}'
        ORDER BY ordinal_position
        """
    )
    rows = cursor.fetchall()
    if not rows:
        try:
            cursor.execute(
                f"""
                SELECT column_name
                FROM svv_columns
                WHERE table_schema = '{SCHEMA_NAME}'
                  AND table_name = '{TABLE_BASE}'
                ORDER BY ordinal_position
                """
            )
            rows = cursor.fetchall()
        except Exception:
            rows = []
    return [row[0] for row in rows]


def query_audit(sql_query: str) -> str:
    """
    Run a SELECT query against monitoring_prod.provider_combined_audit (safe mode).

    Safety:
    - Only SELECT allowed; blocks DELETE/UPDATE/INSERT/DROP/TRUNCATE/ALTER/CREATE
    - Query must reference the full table name {TABLE_NAME}
    - Adds LIMIT 100 if not present

    Args:
        sql_query: A SELECT query string referencing {TABLE_NAME}

    Returns:
        JSON with keys: columns, rows, row_count, truncated

    Example:
        SELECT * FROM monitoring_prod.provider_combined_audit
        WHERE sales_date = 20251014 AND providercode = 'AaPts'
        LIMIT 10
    """
    try:
        query_upper = sql_query.upper().strip()
        if not query_upper.startswith("SELECT"):
            return json.dumps({"error": "Only SELECT queries are allowed"}, indent=2)

        forbidden_keywords = [
            "DELETE",
            "UPDATE",
            "INSERT",
            "DROP",
            "TRUNCATE",
            "ALTER",
            "CREATE",
        ]
        for keyword in forbidden_keywords:
            if re.search(rf"\b{keyword}\b", query_upper):
                return json.dumps({"error": f"Forbidden keyword: {keyword}"}, indent=2)

        if f"{SCHEMA_NAME.upper()}.{TABLE_BASE.upper()}" not in query_upper:
            return json.dumps(
                {
                    "error": f"Query must reference {TABLE_NAME}",
                },
                indent=2,
            )

        if "LIMIT" not in query_upper:
            sql_query = sql_query.rstrip(";") + " LIMIT 100"
            max_rows = 100
        else:
            match = re.search(r"LIMIT\s+(\d+)", query_upper)
            max_rows = int(match.group(1)) if match else 100
            max_rows = min(max_rows, 100)

        connector = _get_connector()
        conn = connector.get_connection()

        if getattr(conn, "autocommit", None) is False:
            try:
                conn.autocommit = True
            except Exception:
                pass

        with conn.cursor() as cursor:
            try:
                cursor.execute("ROLLBACK;")
            except Exception:
                pass

            cursor.execute(sql_query)
            colnames = [desc[0] for desc in cursor.description]
            records = cursor.fetchmany(max_rows)

            rows = []
            for rec in records:
                row = {}
                for i, col in enumerate(colnames):
                    val = rec[i]
                    if val is not None and not isinstance(val, (str, int, float, bool)):
                        val = str(val)
                    row[col] = val
                rows.append(row)

            return json.dumps(
                {
                    "columns": colnames,
                    "rows": rows,
                    "row_count": len(rows),
                    "truncated": len(rows) == max_rows,
                },
                indent=2,
            )

    except Exception as e:
        log.error(f"Error in query_audit: {e}")
        return json.dumps({"error": str(e)}, indent=2)


def get_table_schema() -> str:
    """
    Return schema (column names, types, lengths, nullability, ordinal).

    Uses information_schema first, then svv_columns as fallback for Redshift.
    """
    query_info = f"""
    SELECT
        column_name,
        data_type,
        character_maximum_length,
        is_nullable,
        ordinal_position
    FROM information_schema.columns
    WHERE table_schema = '{SCHEMA_NAME}'
      AND table_name = '{TABLE_BASE}'
    ORDER BY ordinal_position;
    """

    query_svv = f"""
    SELECT
        column_name,
        data_type,
        character_maximum_length,
        is_nullable,
        ordinal_position
    FROM svv_columns
    WHERE table_schema = '{SCHEMA_NAME}'
      AND table_name = '{TABLE_BASE}'
    ORDER BY ordinal_position;
    """

    attempts = 0
    while attempts < 2:
        try:
            connector = _get_connector()
            conn = connector.get_connection()
            try:
                if getattr(conn, "autocommit", None) is False:
                    conn.autocommit = True
            except Exception:
                pass

            with conn.cursor() as cursor:
                try:
                    cursor.execute("ROLLBACK;")
                except Exception:
                    pass

                cursor.execute(query_info)
                colnames = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()

                if not rows:
                    cursor.execute(query_svv)
                    colnames = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()

                results = [
                    {col: row[i] for i, col in enumerate(colnames)} for row in rows
                ]

                return json.dumps({"table": TABLE_NAME, "columns": results}, indent=2)

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

    return json.dumps({"error": "Schema query failed after retry"}, indent=2)


def get_row_count() -> str:
    """Return total row count for monitoring_prod.provider_combined_audit."""
    query = f"SELECT COUNT(*) AS total_rows FROM {TABLE_NAME};"

    try:
        connector = _get_connector()
        conn = connector.get_connection()

        # be defensive about transaction state
        try:
            if getattr(conn, "autocommit", None) is False:
                conn.autocommit = True
        except Exception:
            pass

        with conn.cursor() as cursor:
            try:
                cursor.execute("ROLLBACK;")
            except Exception:
                pass

            cursor.execute(query)
            count = cursor.fetchone()[0]
            return json.dumps({"table": TABLE_NAME, "total_rows": count}, indent=2)
    except Exception as e:
        log.error(f"Error in get_row_count: {e}")
        return json.dumps({"error": str(e)}, indent=2)


def get_distinct_values(column_name: str, max_rows: int = 100) -> str:
    """
    Return distinct values for a column in provider_combined_audit (validated).

    Args:
        column_name: Name of column (validated against schema)
        max_rows: Max values to return (<= 500)
    """
    try:
        max_rows = max(1, min(int(max_rows), 500))
    except Exception:
        max_rows = 100

    try:
        connector = _get_connector()
        conn = connector.get_connection()
        with conn.cursor() as cursor:
            cols = _list_columns(cursor)
            if column_name not in cols:
                return json.dumps({"error": f"Unknown column: {column_name}", "available": cols}, indent=2)

            cursor.execute(
                f"SELECT DISTINCT {column_name} FROM {TABLE_NAME} ORDER BY 1 DESC LIMIT {max_rows}"
            )
            values = [row[0] for row in cursor.fetchall()]
            return json.dumps({"column": column_name, "values": values, "count": len(values)}, indent=2)
    except Exception as e:
        log.error(f"Error in get_distinct_values: {e}")
        return json.dumps({"error": str(e)}, indent=2)


def get_recent_events(limit: int = 100) -> str:
    """
    Return recent rows ordered by a best-effort event timestamp.

    If no timestamp columns exist, returns sample rows ordered by default.
    """
    try:
        limit = max(1, min(int(limit), 200))
    except Exception:
        limit = 100

    try:
        connector = _get_connector()
        conn = connector.get_connection()
        with conn.cursor() as cursor:
            # Find timestamp-like columns
            cursor.execute(
                f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{SCHEMA_NAME}'
                  AND table_name = '{TABLE_BASE}'
                  AND data_type ILIKE '%timestamp%'
                ORDER BY ordinal_position
                """
            )
            ts_cols = [row[0] for row in cursor.fetchall()]

            # Prefer common names if present
            preferred = [
                "event_time",
                "updated_at",
                "created_at",
                "last_modified",
                "timestamp",
                "ts",
            ]
            order_col = None
            for p in preferred:
                if p in ts_cols:
                    order_col = p
                    break
            if not order_col and ts_cols:
                order_col = ts_cols[0]

            if order_col:
                cursor.execute(
                    f"SELECT * FROM {TABLE_NAME} ORDER BY {order_col} DESC LIMIT {limit}"
                )
                colnames = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                data = []
                for rec in rows:
                    row = {}
                    for i, col in enumerate(colnames):
                        val = rec[i]
                        if val is not None and not isinstance(val, (str, int, float, bool)):
                            val = str(val)
                        row[col] = val
                    data.append(row)
                return json.dumps(
                    {
                        "order_column": order_col,
                        "rows": data,
                        "row_count": len(data),
                    },
                    indent=2,
                )
            else:
                # No timestamp columns: return sample rows
                cursor.execute(f"SELECT * FROM {TABLE_NAME} LIMIT {limit}")
                colnames = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                data = []
                for rec in rows:
                    row = {}
                    for i, col in enumerate(colnames):
                        val = rec[i]
                        if val is not None and not isinstance(val, (str, int, float, bool)):
                            val = str(val)
                        row[col] = val
                    data.append(row)
                return json.dumps({"rows": data, "row_count": len(data)}, indent=2)
    except Exception as e:
        log.error(f"Error in get_recent_events: {e}")
        return json.dumps({"error": str(e)}, indent=2)


# -------------------------
# New, richer exploration tools
# -------------------------

def _validate_column_name(cursor, name: str) -> bool:
    cols = set(_list_columns(cursor))
    return name in cols


def _coalesce_timestamp_expression_alias(alias: str = "event_ts") -> str:
    """Return a CASE expression aliasing best-effort parsed event timestamp.

    Avoids COALESCE eager evaluation by conditionally applying TO_TIMESTAMP
    only when the source string is non-empty.
    """
    return (
        "CASE "
        "WHEN response_timestamp IS NOT NULL AND TRIM(response_timestamp) <> '' "
        "THEN TO_TIMESTAMP(TRIM(response_timestamp), 'YYYY-MM-DD HH24:MI:SS.MS') "
        "WHEN actualscheduletimestamp IS NOT NULL AND TRIM(actualscheduletimestamp) <> '' "
        "THEN TO_TIMESTAMP(TRIM(actualscheduletimestamp), 'YYYY-MM-DD HH24:MI:SS.MS') "
        "WHEN observationtimestamp IS NOT NULL AND TRIM(observationtimestamp) <> '' "
        "THEN TO_TIMESTAMP(TRIM(observationtimestamp), 'YYYY-MM-DD HH24:MI:SS.MS') "
        "WHEN dropdeadtimestamps IS NOT NULL AND TRIM(dropdeadtimestamps) <> '' "
        "THEN TO_TIMESTAMP(TRIM(dropdeadtimestamps), 'YYYY-MM-DD HH24:MI:SS.MS') "
        "ELSE NULL END AS " + alias
    )


def get_date_range() -> str:
    """
    Get min and max sales_date available in the table.

    Returns JSON with table, min_date, max_date.
    """
    sql = f"SELECT MIN(sales_date) AS min_date, MAX(sales_date) AS max_date FROM {TABLE_NAME}"
    try:
        conn = _get_connector().get_connection()
        with conn.cursor() as cursor:
            cursor.execute(sql)
            row = cursor.fetchone()
            return json.dumps({"table": TABLE_NAME, "min_date": row[0], "max_date": row[1]}, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)


def get_rows_by_sales_date(sales_date: int, limit: int = 100) -> str:
    """
    Fetch rows for a given sales_date (ordered by best-effort event timestamp).

    Args:
        sales_date: YYYYMMDD integer
        limit: Max rows to return (<= 200)
    """
    try:
        limit = max(1, min(int(limit), 200))
    except Exception:
        limit = 100

    if not isinstance(sales_date, int) or len(str(sales_date)) != 8:
        return json.dumps({"error": "sales_date must be YYYYMMDD int"}, indent=2)

    ts_expr = _coalesce_timestamp_expression_alias()
    sql = f"""
        SELECT *, {ts_expr}
        FROM {TABLE_NAME}
        WHERE sales_date = {sales_date}
        ORDER BY event_ts DESC NULLS LAST, sales_date DESC
        LIMIT {limit}
    """
    try:
        conn = _get_connector().get_connection()
        with conn.cursor() as cursor:
            try:
                cursor.execute("ROLLBACK;")
            except Exception:
                pass
            cursor.execute(sql)
            colnames = [d[0] for d in cursor.description]
            recs = cursor.fetchall()
            rows = []
            for rec in recs:
                row = {}
                for i, c in enumerate(colnames):
                    v = rec[i]
                    if v is not None and not isinstance(v, (str, int, float, bool)):
                        v = str(v)
                    row[c] = v
                rows.append(row)
        return json.dumps({"columns": colnames, "rows": rows, "row_count": len(rows)}, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)


def get_top_by_dimension(dim_column: str, sales_date: int | None = None, top_n: int = 20) -> str:
    """
    Top values by count for a dimension (e.g., providercode, sitecode, pos).

    Args:
        dim_column: Column to group by (validated)
        sales_date: Optional YYYYMMDD to filter a specific date
        top_n: Rows to return (<= 200)
    """
    try:
        top_n = max(1, min(int(top_n), 200))
    except Exception:
        top_n = 20

    try:
        conn = _get_connector().get_connection()
        with conn.cursor() as cursor:
            try:
                cursor.execute("ROLLBACK;")
            except Exception:
                pass
            if not _validate_column_name(cursor, dim_column):
                return json.dumps({"error": f"Unknown column: {dim_column}"}, indent=2)

            where = ""
            if sales_date is not None:
                if not isinstance(sales_date, int) or len(str(sales_date)) != 8:
                    return json.dumps({"error": "sales_date must be YYYYMMDD int"}, indent=2)
                where = f"WHERE sales_date = {sales_date}"

            sql = f"""
                SELECT {dim_column} AS dim_value, COUNT(*) AS cnt
                FROM {TABLE_NAME}
                {where}
                GROUP BY {dim_column}
                ORDER BY cnt DESC NULLS LAST
                LIMIT {top_n}
            """
            cursor.execute(sql)
            colnames = [d[0] for d in cursor.description]
            recs = cursor.fetchall()
            rows = []
            for rec in recs:
                rows.append({colnames[i]: rec[i] for i in range(len(colnames))})
            return json.dumps({"columns": colnames, "rows": rows, "row_count": len(rows)}, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)


def get_volume_by_date(start_date: int | None = None, end_date: int | None = None, top_n: int = 60) -> str:
    """
    Volume (row counts) grouped by sales_date, optionally in a date range.

    Args:
        start_date: Inclusive YYYYMMDD
        end_date: Inclusive YYYYMMDD
        top_n: Max dates returned (ordered desc)
    """
    try:
        top_n = max(1, min(int(top_n), 366))
    except Exception:
        top_n = 60

    filters = []
    if start_date is not None:
        if not isinstance(start_date, int) or len(str(start_date)) != 8:
            return json.dumps({"error": "start_date must be YYYYMMDD int"}, indent=2)
        filters.append(f"sales_date >= {start_date}")
    if end_date is not None:
        if not isinstance(end_date, int) or len(str(end_date)) != 8:
            return json.dumps({"error": "end_date must be YYYYMMDD int"}, indent=2)
        filters.append(f"sales_date <= {end_date}")
    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    sql = f"""
        SELECT sales_date, COUNT(*) AS cnt
        FROM {TABLE_NAME}
        {where}
        GROUP BY sales_date
        ORDER BY sales_date DESC
        LIMIT {top_n}
    """
    try:
        conn = _get_connector().get_connection()
        with conn.cursor() as cursor:
            try:
                cursor.execute("ROLLBACK;")
            except Exception:
                pass
            cursor.execute(sql)
            colnames = [d[0] for d in cursor.description]
            recs = cursor.fetchall()
            rows = []
            for rec in recs:
                rows.append({colnames[i]: rec[i] for i in range(len(colnames))})
            return json.dumps({"columns": colnames, "rows": rows, "row_count": len(rows)}, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)


def summarize_provider_status(provider_code: str, sales_date: int | None = None, top_n: int = 10) -> str:
    """
    Summarize a provider’s performance for a date (or latest).

    Computes total volume, success rate, status breakdown, top sites,
    top issues (source/reason pairs), POS distribution, and top O-D pairs.

    Args:
        provider_code: Value from `providercode` (e.g., 'AaPts', 'AA', 'WN')
        sales_date: Optional YYYYMMDD; when omitted, uses latest sales_date
        top_n: Limit for top lists (<= 50)
    """
    try:
        top_n = max(1, min(int(top_n), 50))
    except Exception:
        top_n = 10

    try:
        conn = _get_connector().get_connection()
        with conn.cursor() as cursor:
            try:
                cursor.execute("ROLLBACK;")
            except Exception:
                pass

            # Determine target date
            if sales_date is None:
                cursor.execute(f"SELECT MAX(sales_date) FROM {TABLE_NAME}")
                target_date = cursor.fetchone()[0]
            else:
                target_date = int(sales_date)

            # Ensure provider exists
            cols = set(_list_columns(cursor))
            if 'providercode' not in cols:
                return json.dumps({"error": "Column providercode not found"}, indent=2)

            # Totals & success rate
            cursor.execute(
                f"""
                WITH base AS (
                    SELECT * FROM {TABLE_NAME}
                    WHERE sales_date = %s AND providercode = %s
                )
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN response_statuses = 'success' THEN 1 ELSE 0 END) AS success_count,
                    SUM(CASE WHEN response_statuses <> 'success' THEN 1 ELSE 0 END) AS non_success_count,
                    MIN(TRY_CAST(response_timestamp AS TIMESTAMP)) AS min_response_ts,
                    MAX(TRY_CAST(response_timestamp AS TIMESTAMP)) AS max_response_ts
                FROM base;
                """,
                (target_date, provider_code),
            )
            total_row = cursor.fetchone()

            totals = {
                "total": int(total_row[0] or 0),
                "success": int(total_row[1] or 0),
                "non_success": int(total_row[2] or 0),
                "min_response_ts": (str(total_row[3]) if total_row[3] is not None else None),
                "max_response_ts": (str(total_row[4]) if total_row[4] is not None else None),
            }
            totals["success_rate"] = (
                round((totals["success"] / totals["total"]) * 100.0, 2)
                if totals["total"] else 0.0
            )

            # Status breakdown
            cursor.execute(
                f"""
                WITH base AS (
                    SELECT response_statuses FROM {TABLE_NAME}
                    WHERE sales_date = %s AND providercode = %s
                )
                SELECT response_statuses, COUNT(*) AS cnt
                FROM base
                GROUP BY 1
                ORDER BY cnt DESC NULLS LAST
                LIMIT {top_n}
                """,
                (target_date, provider_code),
            )
            status_rows = cursor.fetchall()
            statuses = [{"response_statuses": r[0], "cnt": r[1]} for r in status_rows]

            # Top sites (with success counts)
            cursor.execute(
                f"""
                WITH base AS (
                    SELECT sitecode, response_statuses FROM {TABLE_NAME}
                    WHERE sales_date = %s AND providercode = %s
                )
                SELECT sitecode,
                       COUNT(*) AS total,
                       SUM(CASE WHEN response_statuses='success' THEN 1 ELSE 0 END) AS success
                FROM base
                GROUP BY sitecode
                ORDER BY total DESC NULLS LAST
                LIMIT {top_n}
                """,
                (target_date, provider_code),
            )
            site_rows = cursor.fetchall()
            top_sites = [
                {
                    "sitecode": r[0],
                    "total": r[1],
                    "success": r[2],
                    "success_rate": round((r[2] / r[1]) * 100.0, 2) if r[1] else 0.0,
                }
                for r in site_rows
            ]

            # Top issues (non-success only)
            cursor.execute(
                f"""
                WITH base AS (
                    SELECT issue_sources, issue_reasons
                    FROM {TABLE_NAME}
                    WHERE sales_date = %s AND providercode = %s AND response_statuses <> 'success'
                )
                SELECT COALESCE(NULLIF(issue_sources,''),'(null)') AS issue_sources,
                       COALESCE(NULLIF(issue_reasons,''),'(null)') AS issue_reasons,
                       COUNT(*) AS cnt
                FROM base
                GROUP BY 1,2
                ORDER BY cnt DESC NULLS LAST
                LIMIT {top_n}
                """,
                (target_date, provider_code),
            )
            issue_rows = cursor.fetchall()
            top_issues = [
                {"issue_sources": r[0], "issue_reasons": r[1], "cnt": r[2]} for r in issue_rows
            ]

            # POS distribution
            cursor.execute(
                f"""
                WITH base AS (
                    SELECT pos FROM {TABLE_NAME}
                    WHERE sales_date = %s AND providercode = %s
                )
                SELECT pos, COUNT(*) AS cnt
                FROM base
                GROUP BY pos
                ORDER BY cnt DESC NULLS LAST
                LIMIT {top_n}
                """,
                (target_date, provider_code),
            )
            pos_rows = cursor.fetchall()
            pos = [{"pos": r[0], "cnt": r[1]} for r in pos_rows]

            # OD distribution
            cursor.execute(
                f"""
                WITH base AS (
                    SELECT originairportcode, destinationairportcode
                    FROM {TABLE_NAME}
                    WHERE sales_date = %s AND providercode = %s
                )
                SELECT (originairportcode || '-' || destinationairportcode) AS od, COUNT(*) AS cnt
                FROM base
                GROUP BY 1
                ORDER BY cnt DESC NULLS LAST
                LIMIT {top_n}
                """,
                (target_date, provider_code),
            )
            od_rows = cursor.fetchall()
            od = [{"od": r[0], "cnt": r[1]} for r in od_rows]

            return json.dumps(
                {
                    "provider": provider_code,
                    "target_date": target_date,
                    "totals": totals,
                    "statuses": statuses,
                    "top_sites": top_sites,
                    "top_issues": top_issues,
                    "pos": pos,
                    "od": od,
                },
                indent=2,
            )
    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)


def get_overview_today(sales_date: int | None = None, top_n: int = 20) -> str:
    """
    High-level overview (today/latest) across all providers.

    Includes per-provider volume + success rate, status distribution,
    top sites overall, and top (issue_source, issue_reason) pairs.

    Args:
        sales_date: Optional YYYYMMDD; defaults to latest available
        top_n: Limit for each section (<= 50)
    """
    try:
        top_n = max(1, min(int(top_n), 50))
    except Exception:
        top_n = 20

    try:
        conn = _get_connector().get_connection()
        with conn.cursor() as cursor:
            try:
                cursor.execute("ROLLBACK;")
            except Exception:
                pass

            if sales_date is None:
                cursor.execute(f"SELECT MAX(sales_date) FROM {TABLE_NAME}")
                target_date = cursor.fetchone()[0]
            else:
                target_date = int(sales_date)

            # Provider summary
            cursor.execute(
                f"""
                WITH base AS (
                    SELECT providercode, response_statuses FROM {TABLE_NAME}
                    WHERE sales_date = %s
                )
                SELECT providercode,
                       COUNT(*) AS total,
                       SUM(CASE WHEN response_statuses='success' THEN 1 ELSE 0 END) AS success
                FROM base
                GROUP BY providercode
                ORDER BY total DESC NULLS LAST
                LIMIT {top_n}
                """,
                (target_date,),
            )
            prov_rows = cursor.fetchall()
            providers = [
                {
                    "providercode": r[0],
                    "total": r[1],
                    "success": r[2],
                    "success_rate": round((r[2] / r[1]) * 100.0, 2) if r[1] else 0.0,
                }
                for r in prov_rows
            ]

            # Status distribution
            cursor.execute(
                f"""
                WITH base AS (
                    SELECT response_statuses FROM {TABLE_NAME}
                    WHERE sales_date = %s
                )
                SELECT response_statuses, COUNT(*) AS cnt
                FROM base
                GROUP BY 1
                ORDER BY cnt DESC NULLS LAST
                LIMIT {top_n}
                """,
                (target_date,),
            )
            status_rows = cursor.fetchall()
            statuses = [{"response_statuses": r[0], "cnt": r[1]} for r in status_rows]

            # Top sites overall
            cursor.execute(
                f"""
                WITH base AS (
                    SELECT sitecode, response_statuses FROM {TABLE_NAME}
                    WHERE sales_date = %s
                )
                SELECT sitecode,
                       COUNT(*) AS total,
                       SUM(CASE WHEN response_statuses='success' THEN 1 ELSE 0 END) AS success
                FROM base
                GROUP BY sitecode
                ORDER BY total DESC NULLS LAST
                LIMIT {top_n}
                """,
                (target_date,),
            )
            site_rows = cursor.fetchall()
            sites = [
                {
                    "sitecode": r[0],
                    "total": r[1],
                    "success": r[2],
                    "success_rate": round((r[2] / r[1]) * 100.0, 2) if r[1] else 0.0,
                }
                for r in site_rows
            ]

            # Top issues overall
            cursor.execute(
                f"""
                WITH base AS (
                    SELECT issue_sources, issue_reasons, response_statuses
                    FROM {TABLE_NAME}
                    WHERE sales_date = %s AND response_statuses <> 'success'
                )
                SELECT COALESCE(NULLIF(issue_sources,''),'(null)') AS issue_sources,
                       COALESCE(NULLIF(issue_reasons,''),'(null)') AS issue_reasons,
                       COUNT(*) AS cnt
                FROM base
                GROUP BY 1,2
                ORDER BY cnt DESC NULLS LAST
                LIMIT {top_n}
                """,
                (target_date,),
            )
            issue_rows = cursor.fetchall()
            issues = [
                {"issue_sources": r[0], "issue_reasons": r[1], "cnt": r[2]} for r in issue_rows
            ]

            return json.dumps(
                {
                    "target_date": target_date,
                    "providers": providers,
                    "statuses": statuses,
                    "sites": sites,
                    "issues": issues,
                },
                indent=2,
            )
    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)


def summarize_issues_today(sales_date: int | None = None, top_n: int = 10) -> str:
    """
    Summarize today's issues and their impacts.

    Returns, for the latest (or provided) sales_date:
    - total rows, successes, failures, failure_rate
    - top (issue_sources, issue_reasons) by failure count
    - per-issue impacts: counts and samples of affected providers/sites/pos

    Args:
        sales_date: Optional YYYYMMDD; defaults to latest available
        top_n: Number of top issue pairs to include (<= 50)
    """
    try:
        top_n = max(1, min(int(top_n), 50))
    except Exception:
        top_n = 10

    try:
        conn = _get_connector().get_connection()
        with conn.cursor() as cursor:
            try:
                cursor.execute("ROLLBACK;")
            except Exception:
                pass

            # Determine target date
            if sales_date is None:
                cursor.execute(f"SELECT MAX(sales_date) FROM {TABLE_NAME}")
                target_date = cursor.fetchone()[0]
            else:
                target_date = int(sales_date)

            # Totals for the day
            cursor.execute(
                f"""
                WITH base AS (
                    SELECT response_statuses
                    FROM {TABLE_NAME}
                    WHERE sales_date = %s
                )
                SELECT
                    COUNT(*) AS total_rows,
                    SUM(CASE WHEN response_statuses='success' THEN 1 ELSE 0 END) AS total_success,
                    SUM(CASE WHEN response_statuses<>'success' THEN 1 ELSE 0 END) AS total_failed
                FROM base
                """,
                (target_date,),
            )
            totals_row = cursor.fetchone()
            total_rows = int(totals_row[0] or 0)
            total_success = int(totals_row[1] or 0)
            total_failed = int(totals_row[2] or 0)
            failure_rate = round((total_failed / total_rows) * 100.0, 2) if total_rows else 0.0

            # Top issues (issue_sources, issue_reasons)
            cursor.execute(
                f"""
                WITH base AS (
                    SELECT COALESCE(NULLIF(issue_sources,''),'(null)') AS issue_sources,
                           COALESCE(NULLIF(issue_reasons,''),'(null)') AS issue_reasons
                    FROM {TABLE_NAME}
                    WHERE sales_date = %s AND response_statuses <> 'success'
                )
                SELECT issue_sources, issue_reasons, COUNT(*) AS failed_count
                FROM base
                GROUP BY 1,2
                ORDER BY failed_count DESC NULLS LAST
                LIMIT {top_n}
                """,
                (target_date,),
            )
            top_issue_rows = cursor.fetchall()

            issues: list[dict] = []
            for issue_sources, issue_reasons, failed_count in top_issue_rows:
                # Impacts: distinct providers/sites/pos
                cursor.execute(
                    f"""
                    SELECT COUNT(DISTINCT providercode) AS providers_impacted,
                           COUNT(DISTINCT sitecode)      AS sites_impacted,
                           COUNT(DISTINCT pos)           AS pos_impacted
                    FROM {TABLE_NAME}
                    WHERE sales_date = %s
                      AND response_statuses <> 'success'
                      AND COALESCE(NULLIF(issue_sources,''),'(null)') = %s
                      AND COALESCE(NULLIF(issue_reasons,''),'(null)') = %s
                    """,
                    (target_date, issue_sources, issue_reasons),
                )
                prov_cnt, site_cnt, pos_cnt = cursor.fetchone()

                # Top providers for this issue
                cursor.execute(
                    f"""
                    SELECT providercode, COUNT(*) AS cnt
                    FROM {TABLE_NAME}
                    WHERE sales_date = %s
                      AND response_statuses <> 'success'
                      AND COALESCE(NULLIF(issue_sources,''),'(null)') = %s
                      AND COALESCE(NULLIF(issue_reasons,''),'(null)') = %s
                    GROUP BY providercode
                    ORDER BY cnt DESC NULLS LAST
                    LIMIT 5
                    """,
                    (target_date, issue_sources, issue_reasons),
                )
                top_providers_rows = cursor.fetchall()
                top_providers = [
                    {"providercode": r[0], "failed_count": int(r[1])} for r in top_providers_rows
                ]

                # Top sites for this issue
                cursor.execute(
                    f"""
                    SELECT sitecode, COUNT(*) AS cnt
                    FROM {TABLE_NAME}
                    WHERE sales_date = %s
                      AND response_statuses <> 'success'
                      AND COALESCE(NULLIF(issue_sources,''),'(null)') = %s
                      AND COALESCE(NULLIF(issue_reasons,''),'(null)') = %s
                    GROUP BY sitecode
                    ORDER BY cnt DESC NULLS LAST
                    LIMIT 5
                    """,
                    (target_date, issue_sources, issue_reasons),
                )
                top_sites_rows = cursor.fetchall()
                top_sites = [
                    {"sitecode": r[0], "failed_count": int(r[1])} for r in top_sites_rows
                ]

                share_of_day = round(((failed_count or 0) / total_rows) * 100.0, 4) if total_rows else 0.0
                issues.append(
                    {
                        "issue_sources": issue_sources,
                        "issue_reasons": issue_reasons,
                        "failed_count": int(failed_count or 0),
                        "share_of_day_percent": share_of_day,
                        "providers_impacted": int(prov_cnt or 0),
                        "sites_impacted": int(site_cnt or 0),
                        "pos_impacted": int(pos_cnt or 0),
                        "top_providers": top_providers,
                        "top_sites": top_sites,
                    }
                )

            return json.dumps(
                {
                    "target_date": target_date,
                    "total_rows": total_rows,
                    "total_success": total_success,
                    "total_failed": total_failed,
                    "failure_rate_percent": failure_rate,
                    "issues": issues,
                },
                indent=2,
            )
    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)


def summarize_issue_impact(issue_sources: str, issue_reasons: str, sales_date: int | None = None, top_n: int = 20) -> str:
    """
    Deep-dive impact summary for a specific issue pair on a given day.

    Args:
        issue_sources: Issue source value (use '(null)' for null/empty)
        issue_reasons: Issue reason value (use '(null)' for null/empty)
        sales_date: Optional YYYYMMDD; defaults to latest available
        top_n: Number of top providers/sites to return (<= 50)
    """
    try:
        top_n = max(1, min(int(top_n), 50))
    except Exception:
        top_n = 20

    try:
        conn = _get_connector().get_connection()
        with conn.cursor() as cursor:
            try:
                cursor.execute("ROLLBACK;")
            except Exception:
                pass

            if sales_date is None:
                cursor.execute(f"SELECT MAX(sales_date) FROM {TABLE_NAME}")
                target_date = cursor.fetchone()[0]
            else:
                target_date = int(sales_date)

            # Totals for the day
            cursor.execute(
                f"""
                WITH base AS (
                    SELECT response_statuses
                    FROM {TABLE_NAME}
                    WHERE sales_date = %s
                )
                SELECT
                    COUNT(*) AS total_rows,
                    SUM(CASE WHEN response_statuses='success' THEN 1 ELSE 0 END) AS total_success,
                    SUM(CASE WHEN response_statuses<>'success' THEN 1 ELSE 0 END) AS total_failed
                FROM base
                """,
                (target_date,),
            )
            totals_row = cursor.fetchone()
            total_rows = int(totals_row[0] or 0)

            # Failed rows for the target issue
            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM {TABLE_NAME}
                WHERE sales_date = %s
                  AND response_statuses <> 'success'
                  AND COALESCE(NULLIF(issue_sources,''),'(null)') = %s
                  AND COALESCE(NULLIF(issue_reasons,''),'(null)') = %s
                """,
                (target_date, issue_sources, issue_reasons),
            )
            failed_for_issue = int(cursor.fetchone()[0] or 0)
            share_of_day = round(((failed_for_issue or 0) / total_rows) * 100.0, 4) if total_rows else 0.0

            # Impacted distinct counts
            cursor.execute(
                f"""
                SELECT COUNT(DISTINCT providercode) AS providers_impacted,
                       COUNT(DISTINCT sitecode)      AS sites_impacted,
                       COUNT(DISTINCT pos)           AS pos_impacted
                FROM {TABLE_NAME}
                WHERE sales_date = %s
                  AND response_statuses <> 'success'
                  AND COALESCE(NULLIF(issue_sources,''),'(null)') = %s
                  AND COALESCE(NULLIF(issue_reasons,''),'(null)') = %s
                """,
                (target_date, issue_sources, issue_reasons),
            )
            prov_cnt, site_cnt, pos_cnt = cursor.fetchone()

            # Top providers
            cursor.execute(
                f"""
                SELECT providercode, COUNT(*) AS cnt
                FROM {TABLE_NAME}
                WHERE sales_date = %s
                  AND response_statuses <> 'success'
                  AND COALESCE(NULLIF(issue_sources,''),'(null)') = %s
                  AND COALESCE(NULLIF(issue_reasons,''),'(null)') = %s
                GROUP BY providercode
                ORDER BY cnt DESC NULLS LAST
                LIMIT {top_n}
                """,
                (target_date, issue_sources, issue_reasons),
            )
            prov_rows = cursor.fetchall()
            top_providers = [
                {"providercode": r[0], "failed_count": int(r[1])} for r in prov_rows
            ]

            # Top sites
            cursor.execute(
                f"""
                SELECT sitecode, COUNT(*) AS cnt
                FROM {TABLE_NAME}
                WHERE sales_date = %s
                  AND response_statuses <> 'success'
                  AND COALESCE(NULLIF(issue_sources,''),'(null)') = %s
                  AND COALESCE(NULLIF(issue_reasons,''),'(null)') = %s
                GROUP BY sitecode
                ORDER BY cnt DESC NULLS LAST
                LIMIT {top_n}
                """,
                (target_date, issue_sources, issue_reasons),
            )
            site_rows = cursor.fetchall()
            top_sites = [
                {"sitecode": r[0], "failed_count": int(r[1])} for r in site_rows
            ]

            # Status distribution among failures for this issue (if multiple failure statuses exist)
            cursor.execute(
                f"""
                SELECT response_statuses, COUNT(*) AS cnt
                FROM {TABLE_NAME}
                WHERE sales_date = %s
                  AND response_statuses <> 'success'
                  AND COALESCE(NULLIF(issue_sources,''),'(null)') = %s
                  AND COALESCE(NULLIF(issue_reasons,''),'(null)') = %s
                GROUP BY response_statuses
                ORDER BY cnt DESC NULLS LAST
                LIMIT {top_n}
                """,
                (target_date, issue_sources, issue_reasons),
            )
            status_rows = cursor.fetchall()
            failure_statuses = [
                {"response_statuses": r[0], "cnt": int(r[1])} for r in status_rows
            ]

            return json.dumps(
                {
                    "target_date": target_date,
                    "issue_sources": issue_sources,
                    "issue_reasons": issue_reasons,
                    "failed_count": failed_for_issue,
                    "share_of_day_percent": share_of_day,
                    "providers_impacted": int(prov_cnt or 0),
                    "sites_impacted": int(site_cnt or 0),
                    "pos_impacted": int(pos_cnt or 0),
                    "top_providers": top_providers,
                    "top_sites": top_sites,
                    "failure_statuses": failure_statuses,
                },
                indent=2,
            )
    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)
