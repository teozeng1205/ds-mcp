"""Minimal table registry for DS-MCP."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple

from ds_mcp.core.registry import TableRegistry
from ds_mcp.tables.base import ParameterSpec, SQLToolSpec, Table, build_table


# --------------------------------------------------------------------------------------
# Provider Combined Audit helpers


def _build_provider_table() -> Table:
    import re

    macros = {
        "PCA": "prod.monitoring.provider_combined_audit",
        "EVENT_TS": lambda alias=None: (
            "COALESCE(observationtimestamp, actualscheduletimestamp)"
            + (f" AS {alias}" if alias else "")
        ),
        "ISSUE_TYPE": (
            "COALESCE(NULLIF(TRIM(issue_reasons::VARCHAR), ''), "
            "NULLIF(TRIM(issue_sources::VARCHAR), ''))"
        ),
        "OBS_HOUR": "DATE_TRUNC('hour', COALESCE(observationtimestamp, actualscheduletimestamp))",
    }

    travel_expr = "TO_DATE(CAST(scheduledate AS VARCHAR), 'YYYYMMDD')"

    base_dims = {
        "obs_hour": "{{EVENT_TS:obs_hour}}",
        "pos": "NULLIF(TRIM(pos::VARCHAR), '') AS pos",
        "od": "(NULLIF(TRIM(originairportcode::VARCHAR), '') || '-' || NULLIF(TRIM(destinationairportcode::VARCHAR), '')) AS od",
        "origin": "NULLIF(TRIM(originairportcode::VARCHAR), '') AS origin",
        "destination": "NULLIF(TRIM(destinationairportcode::VARCHAR), '') AS destination",
        "cabin": "NULLIF(TRIM(cabin::VARCHAR), '') AS cabin",
        "triptype": "NULLIF(TRIM(triptype::VARCHAR), '') AS triptype",
        "los": "los::VARCHAR AS los",
        "issue_label": "{{ISSUE_TYPE}} AS issue_label",
        "depart_period": f"DATE_TRUNC('month', {travel_expr}) AS depart_period",
        "depart_date": f"{travel_expr} AS depart_date",
        "travel_dow": f"DATE_PART('dow', {travel_expr}) AS travel_dow",
    }

    dim_aliases = {
        "origin_code": "origin",
        "destination_code": "destination",
        "o_and_d": "od",
        "o_d": "od",
        "origin_destination": "od",
        "issue_labels": "issue_label",
        "depart_periods": "depart_period",
        "departure_period": "depart_period",
        "depart_month": "depart_period",
        "depart_months": "depart_period",
        "travel_day_of_week": "travel_dow",
        "depart_dow": "travel_dow",
        "departure_dow": "travel_dow",
    }

    dimension_sql = dict(base_dims)
    for alias, target in dim_aliases.items():
        dimension_sql[alias] = base_dims[target]
    dimension_choices = tuple(dimension_sql.keys())
    full_scope_dims = (
        "obs_hour",
        "pos",
        "triptype",
        "los",
        "od",
        "cabin",
        "depart_period",
        "travel_dow",
        "issue_label",
    )

    PROVIDER_PATTERN = re.compile(r"provider\s+([A-Z0-9]{2,6})", re.IGNORECASE)
    SITE_PATTERN = re.compile(r"site\s+([A-Z0-9]{2,6})", re.IGNORECASE)
    PROVIDER_SITE_PATTERN = re.compile(r"([A-Z0-9]{2,6})\|([A-Z0-9]{2,6})", re.IGNORECASE)

    def _infer_code(request: str, pattern: re.Pattern[str]) -> str | None:
        match = pattern.search(request or "")
        if match:
            return match.group(1).upper()
        return None

    def _infer_pair(request: str) -> tuple[str | None, str | None]:
        match = PROVIDER_SITE_PATTERN.search(request or "")
        if match:
            return match.group(1).upper(), match.group(2).upper()
        return None, None

    def _prepare_issue_scope(values: Dict[str, Any]) -> str:
        dims = list(values.get("dims") or ["obs_hour"])
        select_parts = [dimension_sql[dim] for dim in dims]
        group_indices = ", ".join(str(i + 1) for i in range(len(dims)))
        select_clause = ", ".join(select_parts)
        return (
            f"SELECT {select_clause}, COUNT(*) AS issue_count "
            "FROM {{PCA}} "
            "WHERE providercode ILIKE :provider "
            "  AND sitecode ILIKE :site "
            "  AND sales_date >= CAST(TO_CHAR(CURRENT_DATE - :lookback_days, 'YYYYMMDD') AS INT) "
            f"GROUP BY {group_indices} "
            "ORDER BY issue_count DESC "
            "LIMIT :limit"
        )

    def _prepare_issue_scope_full(values: Dict[str, Any]) -> str:
        scoped = dict(values)
        scoped["dims"] = list(full_scope_dims)
        return _prepare_issue_scope(scoped)

    def _prepare_top_site_flex(values: Dict[str, Any]) -> str:
        provider = (values.get("provider") or "").upper()
        request = values.get("request", "")
        if not provider:
            provider = _infer_code(request, PROVIDER_PATTERN) or ""
        if not provider:
            inferred, _ = _infer_pair(request)
            if inferred:
                provider = inferred
        if not provider:
            raise ValueError("Provider code required (e.g., 'provider QL2').")
        values["provider"] = provider
        return (
            "SELECT NULLIF(TRIM(sitecode::VARCHAR), '') AS site, COUNT(*) AS issue_count "
            "FROM {{PCA}} "
            "WHERE providercode ILIKE :provider "
            "  AND sales_date >= CAST(TO_CHAR(CURRENT_DATE - :lookback_days, 'YYYYMMDD') AS INT) "
            "  AND NULLIF(TRIM(sitecode::VARCHAR), '') IS NOT NULL "
            "GROUP BY 1 "
            "ORDER BY issue_count DESC "
            "LIMIT :limit"
        )

    def _prepare_issue_scope_flex(values: Dict[str, Any]) -> str:
        provider = (values.get("provider") or "").upper()
        site = (values.get("site") or "").upper()
        request = values.get("request", "")
        inferred_provider, inferred_site = _infer_pair(request)
        if not provider:
            provider = _infer_code(request, PROVIDER_PATTERN) or (inferred_provider or "")
        if not site:
            site = _infer_code(request, SITE_PATTERN) or (inferred_site or "")
        if not provider or not site:
            raise ValueError("Provider and site codes are required (e.g., 'provider QL2' and 'site QF').")
        values["provider"] = provider
        values["site"] = site
        return _prepare_issue_scope(values)

    tools = (
        SQLToolSpec(
            name="top_site_issues",
            doc="Top site issues for a provider within a recent window.",
            sql=(
                "SELECT NULLIF(TRIM(sitecode::VARCHAR), '') AS site, COUNT(*) AS issue_count "
                "FROM {{PCA}} "
                "WHERE providercode ILIKE :provider "
                "  AND sales_date >= CAST(TO_CHAR(CURRENT_DATE - :lookback_days, 'YYYYMMDD') AS INT) "
                "  AND NULLIF(TRIM(sitecode::VARCHAR), '') IS NOT NULL "
                "GROUP BY 1 "
                "ORDER BY issue_count DESC "
                "LIMIT :limit"
            ),
            params=(
                ParameterSpec(name="provider", coerce=lambda v: str(v).strip().upper()),
                ParameterSpec(name="lookback_days", default=3, coerce=int, min_value=1, max_value=30, as_literal=True),
                ParameterSpec(name="limit", default=20, coerce=int, min_value=1, max_value=200, as_literal=True),
            ),
            enforce_limit=False,
        ),
        SQLToolSpec(
            name="list_provider_sites",
            doc="Most active sites for a provider across a recent window.",
            sql=(
                "SELECT NULLIF(TRIM(sitecode::VARCHAR), '') AS site, COUNT(*) AS issue_count "
                "FROM {{PCA}} "
                "WHERE providercode ILIKE :provider "
                "  AND sales_date >= CAST(TO_CHAR(CURRENT_DATE - :lookback_days, 'YYYYMMDD') AS INT) "
                "GROUP BY 1 "
                "ORDER BY issue_count DESC "
                "LIMIT :limit"
            ),
            params=(
                ParameterSpec(name="provider", coerce=lambda v: str(v).strip().upper()),
                ParameterSpec(name="lookback_days", default=7, coerce=int, min_value=1, max_value=60, as_literal=True),
                ParameterSpec(name="limit", default=20, coerce=int, min_value=1, max_value=200, as_literal=True),
            ),
            enforce_limit=False,
        ),
        SQLToolSpec(
            name="overview_site_issues_today",
            doc="Overview of today's issues grouped by label, provider, and POS.",
            sql=(
                "SELECT LOWER({{ISSUE_TYPE}})::VARCHAR AS issue_key,"
                "       NULLIF(TRIM(providercode::VARCHAR), '') AS provider,"
                "       NULLIF(TRIM(pos::VARCHAR), '') AS pos,"
                "       {{OBS_HOUR}} AS obs_hour,"
                "       COUNT(*) AS issue_count"
                "  FROM {{PCA}}"
                " WHERE sales_date = {{TODAY}}"
                " GROUP BY 1, 2, 3, 4"
                " ORDER BY issue_count DESC"
                " LIMIT :limit"
            ),
            params=(
                ParameterSpec(name="limit", default=50, coerce=int, min_value=1, max_value=200, as_literal=True),
            ),
            enforce_limit=False,
        ),
        SQLToolSpec(
            name="issue_scope_combined",
            doc="Aggregate provider/site issues by selected dimensions.",
            sql="",
            params=(
                ParameterSpec(name="provider", coerce=lambda v: str(v).strip().upper()),
                ParameterSpec(name="site", coerce=lambda v: str(v).strip().upper()),
                ParameterSpec(name="dims", default=("obs_hour", "pos", "od"), kind="list", choices=dimension_choices, include_in_sql=False),
                ParameterSpec(name="lookback_days", default=3, coerce=int, min_value=1, max_value=30, as_literal=True),
                ParameterSpec(name="limit", default=50, coerce=int, min_value=1, max_value=500, as_literal=True),
            ),
            enforce_limit=False,
            prepare=_prepare_issue_scope,
        ),
        SQLToolSpec(
            name="issue_scope_combined_all",
            doc="Aggregate provider/site issues across the standard scope dimensions.",
            sql="",
            params=(
                ParameterSpec(name="provider", coerce=lambda v: str(v).strip().upper()),
                ParameterSpec(name="site", coerce=lambda v: str(v).strip().upper()),
                ParameterSpec(name="lookback_days", default=7, coerce=int, min_value=1, max_value=30, as_literal=True),
                ParameterSpec(name="limit", default=100, coerce=int, min_value=1, max_value=500, as_literal=True),
            ),
            enforce_limit=False,
            prepare=_prepare_issue_scope_full,
        ),
        SQLToolSpec(
            name="top_site_issues_flex",
            doc="Top site issues with provider inferred from the natural-language request if omitted.",
            sql="",
            params=(
                ParameterSpec(name="provider", default="", coerce=lambda v: str(v).strip().upper()),
                ParameterSpec(name="lookback_days", default=3, coerce=int, min_value=1, max_value=30, as_literal=True),
                ParameterSpec(name="limit", default=20, coerce=int, min_value=1, max_value=200, as_literal=True),
                ParameterSpec(name="request", default="", coerce=str, include_in_sql=False),
            ),
            enforce_limit=False,
            prepare=_prepare_top_site_flex,
        ),
        SQLToolSpec(
            name="issue_scope_combined_flex",
            doc="Scope issues for a provider/site with inference from the request text.",
            sql="",
            params=(
                ParameterSpec(name="provider", default="", coerce=lambda v: str(v).strip().upper()),
                ParameterSpec(name="site", default="", coerce=lambda v: str(v).strip().upper()),
                ParameterSpec(name="dims", default=("obs_hour", "pos", "od"), kind="list", choices=dimension_choices, include_in_sql=False),
                ParameterSpec(name="lookback_days", default=3, coerce=int, min_value=1, max_value=30, as_literal=True),
                ParameterSpec(name="limit", default=50, coerce=int, min_value=1, max_value=500, as_literal=True),
                ParameterSpec(name="request", default="", coerce=str, include_in_sql=False),
            ),
            enforce_limit=False,
            prepare=_prepare_issue_scope_flex,
        ),
    )

    return build_table(
        slug="provider",
        schema_name="monitoring",
        table_name="provider_combined_audit",
        database_name="prod",
        description="Provider-level monitoring feed combining issue signals and context.",
        key_columns=("providercode", "sitecode", "sales_date"),
        partition_columns=("sales_date",),
        macros=macros,
        custom_tools=tools,
        query_aliases=("query_audit",),
    )


# --------------------------------------------------------------------------------------
# Market Level Anomalies helpers


def _build_anomalies_table() -> Table:
    macros = {"MLA": "analytics.market_level_anomalies_v3"}

    def _coerce_customer(value: Any) -> str | None:
        if value in (None, ""):
            return None
        return str(value).strip().upper()

    def _prepare_overview_today(values: Dict[str, Any]) -> str:
        filters = ["any_anomaly = 1", "sales_date = {{TODAY}}"]
        if values.get("customer"):
            filters.append("customer = :customer")
        where_clause = " AND ".join(filters)
        return (
            "WITH base AS ("
            "    SELECT customer, cp FROM {{MLA}} WHERE "
            f"{where_clause}"
            "), customers AS ("
            "    SELECT 'customer' AS bucket, NULLIF(TRIM(customer::VARCHAR), '') AS label, COUNT(*) AS anomaly_count"
            "    FROM base GROUP BY 1, 2 ORDER BY anomaly_count DESC LIMIT :per_dim_limit"
            "), cps AS ("
            "    SELECT 'cp' AS bucket, NULLIF(TRIM(cp::VARCHAR), '') AS label, COUNT(*) AS anomaly_count"
            "    FROM base GROUP BY 1, 2 ORDER BY anomaly_count DESC LIMIT :per_dim_limit"
            ")"
            " SELECT bucket, label, anomaly_count FROM ("
            "    SELECT * FROM customers"
            "    UNION ALL"
            "    SELECT * FROM cps"
            " ) AS combined"
            " ORDER BY bucket, anomaly_count DESC"
        )

    tools = (
        SQLToolSpec(
            name="get_available_customers",
            doc="Return customers with total/anomaly record counts and date spans.",
            sql=(
                "SELECT customer, COUNT(*) AS total_records,"
                "       SUM(CASE WHEN any_anomaly = 1 THEN 1 ELSE 0 END) AS anomaly_records,"
                "       MIN(sales_date) AS first_date, MAX(sales_date) AS last_date"
                "  FROM {{MLA}}"
                " GROUP BY customer"
                " ORDER BY customer"
                " LIMIT :limit"
            ),
            params=(
                ParameterSpec(name="limit", default=100, coerce=int, min_value=1, max_value=500, as_literal=True),
            ),
            enforce_limit=False,
        ),
        SQLToolSpec(
            name="overview_anomalies_today",
            doc="Buckets anomalies for today by customer and competitive position.",
            sql="",
            params=(
                ParameterSpec(name="customer", default=None, coerce=_coerce_customer),
                ParameterSpec(name="per_dim_limit", default=25, coerce=int, min_value=1, max_value=200, as_literal=True),
            ),
            enforce_limit=False,
            prepare=_prepare_overview_today,
        ),
    )

    return build_table(
        slug="anomalies",
        schema_name="analytics",
        table_name="market_level_anomalies_v3",
        description="Market-level pricing anomalies with impact scores and CP context.",
        key_columns=("customer", "mkt", "sales_date"),
        partition_columns=("sales_date",),
        default_limit=100,
        head_order_by=("sales_date DESC",),
        macros=macros,
        custom_tools=tools,
        query_aliases=("query_anomalies",),
    )


# --------------------------------------------------------------------------------------
# Table registry utilities

TABLES: Dict[str, Table] = {
    "provider": _build_provider_table(),
    "anomalies": _build_anomalies_table(),
}

_DYNAMIC_TABLES: Dict[str, Table] = {}


def list_available_tables() -> List[Tuple[str, str]]:
    tables = {**TABLES, **_DYNAMIC_TABLES}
    return sorted((slug, tbl.table_name) for slug, tbl in tables.items())


def get_table(slug_or_name: str) -> Table:
    tables = {**TABLES, **_DYNAMIC_TABLES}
    if slug_or_name in tables:
        return tables[slug_or_name]
    table = _create_generic_table(slug_or_name)
    _DYNAMIC_TABLES[table.slug] = table
    return table


def register_dynamic_table(table: Table) -> None:
    _DYNAMIC_TABLES[table.slug] = table


def register_all_tables(registry: TableRegistry, only: Iterable[str] | None = None) -> List[str]:
    target_slugs = list(only) if only else [slug for slug, _ in list_available_tables()]
    registered: List[str] = []
    for identifier in target_slugs:
        table = get_table(identifier)
        table.register(registry)
        registered.append(table.slug)
    return registered


def _create_generic_table(identifier: str) -> Table:
    parts = identifier.split(".")
    database = None
    if len(parts) == 2:
        schema, table = parts
    elif len(parts) == 3:
        database, schema, table = parts
    else:
        raise KeyError(f"Invalid table identifier '{identifier}'. Use schema.table or database.schema.table")
    slug = identifier.replace(".", "_").lower()
    description = f"Generic table wrapper for {identifier}."
    return build_table(
        slug=slug,
        schema_name=schema,
        table_name=table,
        database_name=database,
        description=description,
    )


__all__ = [
    "Table",
    "build_table",
    "get_table",
    "list_available_tables",
    "register_all_tables",
    "register_dynamic_table",
]
