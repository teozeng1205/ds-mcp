"""
Table metadata for Market Level Anomalies V3.

The heavy lifting is handled by :mod:`ds_mcp.tables.base`. This module only
declares macros and ready-made SQL tool specifications so new tools stay
declarative.
"""

from __future__ import annotations

from typing import Any

from ds_mcp.tables.base import ParameterSpec, SQLToolSpec

TABLE_NAME = "analytics.market_level_anomalies_v3"

MACROS = {
    "MLA": TABLE_NAME,
}


def _coerce_optional_customer(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value).strip().upper()


def _prepare_overview_today(values: dict[str, Any]) -> str:
    filters = [
        "any_anomaly = 1",
        "sales_date = {{TODAY}}",
    ]
    if values.get("customer"):
        filters.append("customer = :customer")
    where_clause = " AND ".join(filters)

    return (
        "WITH base AS ("
        "    SELECT customer, cp"
        "    FROM {{MLA}}"
        f"    WHERE {where_clause}"
        "),"
        " customers AS ("
        "    SELECT 'customer' AS bucket,"
        "           NULLIF(TRIM(customer::VARCHAR), '') AS label,"
        "           COUNT(*) AS anomaly_count"
        "    FROM base"
        "    GROUP BY 1, 2"
        "    ORDER BY anomaly_count DESC"
        "    LIMIT :per_dim_limit"
        "),"
        " cps AS ("
        "    SELECT 'cp' AS bucket,"
        "           NULLIF(TRIM(cp::VARCHAR), '') AS label,"
        "           COUNT(*) AS anomaly_count"
        "    FROM base"
        "    GROUP BY 1, 2"
        "    ORDER BY anomaly_count DESC"
        "    LIMIT :per_dim_limit"
        ")"
        " SELECT bucket, label, anomaly_count"
        " FROM ("
        "    SELECT * FROM customers"
        "    UNION ALL"
        "    SELECT * FROM cps"
        " ) AS combined"
        " ORDER BY bucket, anomaly_count DESC"
    )


SQL_TOOL_SPECS = (
    SQLToolSpec(
        name="get_available_customers",
        doc="Return customers with record counts and anomaly totals.",
        sql=(
            "SELECT customer, "
            "       COUNT(*) AS total_records, "
            "       SUM(CASE WHEN any_anomaly = 1 THEN 1 ELSE 0 END) AS anomaly_records, "
            "       MIN(sales_date) AS first_date, "
            "       MAX(sales_date) AS last_date "
            "FROM {{MLA}} "
            "GROUP BY customer "
            "ORDER BY customer "
            "LIMIT :limit"
        ),
        params=(
            ParameterSpec(
                name="limit",
                description="Maximum customers to return",
                default=100,
                coerce=int,
                min_value=1,
                max_value=500,
                as_literal=True,
            ),
        ),
        enforce_limit=False,
        max_rows_param="limit",
    ),
    SQLToolSpec(
        name="overview_anomalies_today",
        doc=(
            "Quick overview of today's anomalies with counts grouped by customer"
            " and competitive position."
        ),
        sql="",  # Built dynamically.
        params=(
            ParameterSpec(
                name="customer",
                description="Optional customer code filter (defaults to all)",
                default=None,
                coerce=_coerce_optional_customer,
            ),
            ParameterSpec(
                name="per_dim_limit",
                description="Maximum rows per bucket",
                default=25,
                coerce=int,
                min_value=1,
                max_value=200,
                as_literal=True,
            ),
        ),
        enforce_limit=False,
        prepare=_prepare_overview_today,
    ),
)
