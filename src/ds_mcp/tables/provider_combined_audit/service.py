"""
Provider Combined Audit service layer.

Encapsulates SQL helpers and tool implementations behind a small, testable API.
Tools delegate to this class to keep module-level MCP functions small and clear.

Best practices:
- Read-only SELECT/WITH queries only; enforce with simple checks.
- Never print to stdout; log via caller to stderr if needed.
- Fail fast with clear errors (JSON string payloads).
"""

from __future__ import annotations

import json
import logging
import re
from typing import List, Optional

from ds_mcp.core.connectors import DatabaseConnectorFactory


log = logging.getLogger(__name__)


class ProviderCombinedAuditService:
    SCHEMA_NAME = "monitoring_prod"
    TABLE_BASE = "provider_combined_audit"
    TABLE_NAME = f"{SCHEMA_NAME}.{TABLE_BASE}"

    PROVIDER_COL = "providercode"
    SITE_COL = "sitecode"
    DATE_COL = "sales_date"
    POS_COL = "pos"
    CABIN_COL = "cabin"
    TRIPTYPE_COL = "triptype"
    LOS_COL = "los"
    DEPARTDATE_COL = "departdate"

    # ---------------------------------------------------------------------
    # SQL helpers
    # ---------------------------------------------------------------------
    @staticmethod
    def _build_event_ts(alias: Optional[str] = None) -> str:
        expr = "COALESCE(observationtimestamp, actualscheduletimestamp)"
        return f"{expr} AS {alias}" if alias else expr

    @classmethod
    def _sales_date_bound(cls, days: int) -> str:
        return f"{cls.DATE_COL} >= CAST(TO_CHAR(CURRENT_DATE - {days}, 'YYYYMMDD') AS INT)"

    @staticmethod
    def _today_int() -> str:
        return "CAST(TO_CHAR(CURRENT_DATE, 'YYYYMMDD') AS INT)"

    @classmethod
    def _expand_macros(cls, sql: str) -> str:
        result = sql

        def expand_event_ts(match):
            alias = match.group(1)
            return cls._build_event_ts(alias)

        result = re.sub(r"\{\{EVENT_TS(?::([A-Za-z_][A-Za-z0-9_]*))?\}\}", expand_event_ts, result)
        result = result.replace("{{PCA}}", cls.TABLE_NAME)
        result = result.replace("{{OD}}", "(originairportcode || '-' || destinationairportcode)")
        result = result.replace("{{OBS_HOUR}}", f"DATE_TRUNC('hour', {cls._build_event_ts()})")

        # ISSUE_TYPE = non-empty reasons or sources
        result = result.replace(
            "{{ISSUE_TYPE}}",
            "COALESCE(NULLIF(TRIM(issue_reasons::VARCHAR), ''), NULLIF(TRIM(issue_sources::VARCHAR), ''))",
        )
        result = result.replace("{{IS_SITE}}", "NULLIF(TRIM(sitecode::VARCHAR), '') IS NOT NULL")
        result = result.replace("{{IS_INVALID}}", "FALSE")
        return result

    @staticmethod
    def _date_expr(col: str, dtype: str) -> str:
        dtype_lower = (dtype or "").lower()
        if "timestamp" in dtype_lower or "date" in dtype_lower:
            return col
        return f"TO_DATE({col}::VARCHAR, 'YYYYMMDD')"

    # ---------------------------------------------------------------------
    # DB execution
    # ---------------------------------------------------------------------
    @classmethod
    def _execute_select(cls, sql_query: str, params=None, max_rows: int = 100) -> str:
        expanded_sql = cls._expand_macros(sql_query)
        sql_upper = expanded_sql.upper().strip()

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
        if not re.search(r"\blimit\b\s+\d+", expanded_sql, flags=re.IGNORECASE):
            expanded_sql = expanded_sql.rstrip(";") + f" LIMIT {max_rows}"
            truncated = True

        try:
            conn = DatabaseConnectorFactory.get_connector("analytics").get_connection()
            try:
                conn.autocommit = True
            except Exception:
                pass
            with conn.cursor() as cur:
                try:
                    cur.execute("ROLLBACK;")
                except Exception:
                    pass
                log.info(f"SQL[execute]: {expanded_sql.strip()} | params={params}")
                cur.execute(expanded_sql, tuple(params) if params else None)
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

    # ---------------------------------------------------------------------
    # Tool implementations
    # ---------------------------------------------------------------------
    @classmethod
    def query_audit(cls, sql_query: str, params: Optional[List] = None) -> str:
        return cls._execute_select(sql_query, params=params, max_rows=100)

    @classmethod
    def get_table_schema(cls) -> str:
        return cls._execute_select(
            f"""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM svv_columns
            WHERE table_schema = '{cls.SCHEMA_NAME}' AND table_name = '{cls.TABLE_BASE}'
            ORDER BY ordinal_position
            """
        )

    @classmethod
    def top_site_issues(cls, provider: str, lookback_days: int = 7, limit: int = 10) -> str:
        limit = min(max(1, limit), 50)
        date_expr = cls._date_expr(cls.DATE_COL, "bigint")
        sql = (
            "SELECT {{ISSUE_TYPE}} AS issue_key, COUNT(*) AS cnt "
            "FROM {{PCA}} "
            f"WHERE {cls.PROVIDER_COL} ILIKE %s "
            f"AND {cls._sales_date_bound(lookback_days)} "
            f"AND {date_expr} >= CURRENT_DATE - {lookback_days} "
            "AND COALESCE(NULLIF(TRIM(issue_reasons::VARCHAR), ''), NULLIF(TRIM(issue_sources::VARCHAR), '')) IS NOT NULL "
            "GROUP BY 1 ORDER BY 2 DESC "
            f"LIMIT {limit}"
        )
        return cls._execute_select(sql, [f"%{provider}%"])

    @classmethod
    def list_provider_sites(cls, provider: str, lookback_days: int = 7, limit: int = 10) -> str:
        limit = min(max(1, limit), 50)
        date_expr = cls._date_expr(cls.DATE_COL, "bigint")
        sql = (
            f"SELECT NULLIF(TRIM({cls.SITE_COL}::VARCHAR), '') AS site, COUNT(*) AS cnt "
            "FROM {{PCA}} "
            f"WHERE {cls.PROVIDER_COL} ILIKE %s "
            f"AND {cls._sales_date_bound(lookback_days)} "
            f"AND {date_expr} >= CURRENT_DATE - {lookback_days} "
            "AND COALESCE(NULLIF(TRIM(issue_reasons::VARCHAR), ''), NULLIF(TRIM(issue_sources::VARCHAR), '')) IS NOT NULL "
            f"AND NULLIF(TRIM({cls.SITE_COL}::VARCHAR), '') IS NOT NULL "
            "GROUP BY 1 ORDER BY 2 DESC "
            f"LIMIT {limit}"
        )
        return cls._execute_select(sql, [f"%{provider}%"])

    @classmethod
    def issue_scope_combined(
        cls,
        provider: str,
        site: str,
        dims: List[str],
        lookback_days: int = 7,
        limit: int = 200,
    ) -> str:
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
        date_expr = cls._date_expr(cls.DATE_COL, "bigint")

        select_map = {
            "obs_hour": "{{OBS_HOUR}} AS obs_hour",
            "pos": f"NULLIF(TRIM({cls.POS_COL}::VARCHAR), '') AS pos",
            "od": "(originairportcode || '-' || destinationairportcode) AS od",
            "cabin": f"NULLIF(TRIM({cls.CABIN_COL}::VARCHAR), '') AS cabin",
            "triptype": f"NULLIF(TRIM({cls.TRIPTYPE_COL}::VARCHAR), '') AS triptype",
            "los": f"{cls.LOS_COL}::VARCHAR AS los",
            "depart_week": f"DATE_TRUNC('week', TO_DATE({cls.DEPARTDATE_COL}::VARCHAR, 'YYYYMMDD')) AS depart_week",
            "depart_dow": f"EXTRACT(DOW FROM TO_DATE({cls.DEPARTDATE_COL}::VARCHAR, 'YYYYMMDD'))::INT AS depart_dow",
        }

        select_cols = [select_map[d] for d in dims_req]
        select_list = ", ".join(select_cols) + ", COUNT(*) AS cnt"
        group_by = ", ".join(str(i) for i in range(1, len(dims_req) + 1))

        base_where = (
            f"WHERE {cls.PROVIDER_COL} ILIKE %s "
            f"AND {cls.SITE_COL} ILIKE %s "
            f"AND {cls._sales_date_bound(lookback_days)} "
            f"AND {date_expr} >= CURRENT_DATE - {lookback_days} "
            "AND COALESCE(NULLIF(TRIM(issue_reasons::VARCHAR), ''), NULLIF(TRIM(issue_sources::VARCHAR), '')) IS NOT NULL "
        )

        extra_filters = []
        if "pos" in dims_req:
            extra_filters.append(f"{cls.POS_COL} IS NOT NULL")
        if "cabin" in dims_req:
            extra_filters.append(f"{cls.CABIN_COL} IS NOT NULL")
        if "triptype" in dims_req:
            extra_filters.append(f"{cls.TRIPTYPE_COL} IS NOT NULL")
        if "los" in dims_req:
            extra_filters.append(f"{cls.LOS_COL} IS NOT NULL")
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
        return cls._execute_select(sql, params)

    @classmethod
    def overview_site_issues_today(cls, per_dim_limit: int = 50) -> str:
        per_dim_limit = min(max(1, per_dim_limit), 500)
        date_expr = cls._date_expr(cls.DATE_COL, "bigint")
        sql = (
            "SELECT "
            "  NULLIF(TRIM(LOWER(COALESCE(issue_reasons::VARCHAR, issue_sources::VARCHAR))), '') AS issue_key, "
            f"  NULLIF(TRIM({cls.PROVIDER_COL}::VARCHAR), '') AS provider, "
            f"  NULLIF(TRIM({cls.POS_COL}::VARCHAR), '') AS pos, "
            f"  DATE_TRUNC('hour', {cls._build_event_ts()}) AS obs_hour, "
            "  COUNT(*) AS cnt "
            "FROM {{PCA}} "
            f"WHERE {cls.DATE_COL} = {cls._today_int()} "
            f"  AND {date_expr} >= CURRENT_DATE "
            "  AND COALESCE(NULLIF(TRIM(issue_reasons::VARCHAR), ''), NULLIF(TRIM(issue_sources::VARCHAR), '')) IS NOT NULL "
            "GROUP BY 1, 2, 3, 4 "
            "ORDER BY 5 DESC "
            f"LIMIT {per_dim_limit}"
        )
        return cls._execute_select(sql)


