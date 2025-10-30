"""
Table metadata for Provider Combined Audit.

All heavy lifting runs through :mod:`ds_mcp.tables.base`. This module declares
macros and SQL tool specifications so new tools stay declarative.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from ds_mcp.tables.base import ParameterSpec, SQLToolSpec, SimpleTableExecutor

TABLE_NAME = "prod.monitoring.provider_combined_audit"

MACROS = {
    "PCA": TABLE_NAME,
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

_DIMENSION_SQL: Dict[str, str] = {
    "obs_hour": "{{EVENT_TS:obs_hour}}",
    "pos": "NULLIF(TRIM(pos::VARCHAR), '') AS pos",
    "od": "(NULLIF(TRIM(originairportcode::VARCHAR), '') || '-' || NULLIF(TRIM(destinationairportcode::VARCHAR), '')) AS od",
    "cabin": "NULLIF(TRIM(cabin::VARCHAR), '') AS cabin",
    "triptype": "NULLIF(TRIM(triptype::VARCHAR), '') AS triptype",
    "los": "los::VARCHAR AS los",
}


_PROVIDER_PATTERN = re.compile(r"provider\s+([A-Z0-9]{2,6})", re.IGNORECASE)
_SITE_PATTERN = re.compile(r"site\s+([A-Z0-9]{2,6})", re.IGNORECASE)


def _infer_code(request: str, pattern: re.Pattern[str]) -> Optional[str]:
    match = pattern.search(request)
    if match:
        return match.group(1).upper()
    return None


def _prepare_issue_scope(values: Dict[str, List[str]]) -> str:
    dims = values.get("dims") or ["obs_hour"]
    select_parts = [_DIMENSION_SQL[dim] for dim in dims]
    group_indices = ", ".join(str(i + 1) for i in range(len(dims)))
    select_clause = ", ".join(select_parts)
    return (
        f"SELECT {select_clause}, COUNT(*) AS issue_count "
        "FROM {{PCA}} "
        "WHERE providercode ILIKE :provider "
        "  AND sitecode ILIKE :site "
        "  AND sales_date >= CAST(TO_CHAR(CURRENT_DATE - :lookback_days, 'YYYYMMDD') AS INT) "
        "  AND {{ISSUE_TYPE}} IS NOT NULL "
        f"GROUP BY {group_indices} "
        "ORDER BY issue_count DESC "
        "LIMIT :limit"
    )


def _prepare_top_site_flex(values: Dict[str, str]) -> str:
    provider = values.get("provider", "").upper()
    if not provider:
        provider = _infer_code(values.get("request", ""), _PROVIDER_PATTERN) or ""
    if not provider:
        raise ValueError(f"Provider code required (e.g., 'provider QL2'). Received args: {values}")
    values["provider"] = provider
    return (
        "SELECT NULLIF(TRIM(sitecode::VARCHAR), '') AS site, COUNT(*) AS issue_count "
        "FROM {{PCA}} "
        "WHERE providercode ILIKE :provider "
        "  AND sales_date >= CAST(TO_CHAR(CURRENT_DATE - :lookback_days, 'YYYYMMDD') AS INT) "
        "  AND {{ISSUE_TYPE}} IS NOT NULL "
        "  AND NULLIF(TRIM(sitecode::VARCHAR), '') IS NOT NULL "
        "GROUP BY 1 "
        "ORDER BY issue_count DESC "
        "LIMIT :limit"
    )


def _prepare_issue_scope_flex(values: Dict[str, Any]) -> str:
    provider = values.get("provider", "").upper()
    site = values.get("site", "").upper()
    request = values.get("request", "")
    if not provider:
        provider = _infer_code(request, _PROVIDER_PATTERN) or ""
    if not site:
        site = _infer_code(request, _SITE_PATTERN) or ""
    if not provider or not site:
        raise ValueError(f"Provider and site codes are required (e.g., 'provider QL2' and 'site QF'). Received args: {values}")
    values["provider"] = provider
    values["site"] = site
    dims = values.get("dims") or ["obs_hour"]
    select_parts = [_DIMENSION_SQL[dim] for dim in dims]
    group_indices = ", ".join(str(i + 1) for i in range(len(dims)))
    select_clause = ", ".join(select_parts)
    return (
        f"SELECT {select_clause}, COUNT(*) AS issue_count "
        "FROM {{PCA}} "
        "WHERE providercode ILIKE :provider "
        "  AND sitecode ILIKE :site "
        "  AND sales_date >= CAST(TO_CHAR(CURRENT_DATE - :lookback_days, 'YYYYMMDD') AS INT) "
        "  AND {{ISSUE_TYPE}} IS NOT NULL "
        f"GROUP BY {group_indices} "
        "ORDER BY issue_count DESC "
        "LIMIT :limit"
    )


SQL_TOOL_SPECS = (
    SQLToolSpec(
        name="top_site_issues",
        doc="Top site issues for a provider within a recent window.",
        sql=(
            "SELECT NULLIF(TRIM(sitecode::VARCHAR), '') AS site, COUNT(*) AS issue_count "
            "FROM {{PCA}} "
            "WHERE providercode ILIKE :provider "
            "  AND sales_date >= CAST(TO_CHAR(CURRENT_DATE - :lookback_days, 'YYYYMMDD') AS INT) "
            "  AND {{ISSUE_TYPE}} IS NOT NULL "
            "  AND NULLIF(TRIM(sitecode::VARCHAR), '') IS NOT NULL "
            "GROUP BY 1 "
            "ORDER BY issue_count DESC "
            "LIMIT :limit"
        ),
        params=(
            ParameterSpec(
                name="provider",
                description="Provider code (case-insensitive)",
                coerce=str,
                transform=lambda v: v.strip(),
            ),
            ParameterSpec(
                name="lookback_days",
                description="Days to look back",
                default=3,
                coerce=int,
                min_value=1,
                max_value=30,
                as_literal=True,
            ),
            ParameterSpec(
                name="limit",
                description="Maximum rows to return",
                default=20,
                coerce=int,
                min_value=1,
                max_value=200,
                as_literal=True,
            ),
        ),
        enforce_limit=False,
        max_rows_param="limit",
    ),
    SQLToolSpec(
        name="list_provider_sites",
        doc="Most active sites for a provider across a recent window.",
        sql=(
            "SELECT NULLIF(TRIM(sitecode::VARCHAR), '') AS site, COUNT(*) AS issue_count "
            "FROM {{PCA}} "
            "WHERE providercode ILIKE :provider "
            "  AND sales_date >= CAST(TO_CHAR(CURRENT_DATE - :lookback_days, 'YYYYMMDD') AS INT) "
            "  AND {{ISSUE_TYPE}} IS NOT NULL "
            "GROUP BY 1 "
            "ORDER BY issue_count DESC "
            "LIMIT :limit"
        ),
        params=(
            ParameterSpec(
                name="provider",
                description="Provider code (case-insensitive)",
                coerce=str,
                transform=lambda v: v.strip(),
            ),
            ParameterSpec(
                name="lookback_days",
                description="Days to look back",
                default=7,
                coerce=int,
                min_value=1,
                max_value=60,
                as_literal=True,
            ),
            ParameterSpec(
                name="limit",
                description="Maximum rows to return",
                default=20,
                coerce=int,
                min_value=1,
                max_value=200,
                as_literal=True,
            ),
        ),
        enforce_limit=False,
        max_rows_param="limit",
    ),
    SQLToolSpec(
        name="overview_site_issues_today",
        doc="Overview of today's issues grouped by label, provider, and POS.",
        sql=(
            "SELECT "
            "  LOWER({{ISSUE_TYPE}})::VARCHAR AS issue_key, "
            "  NULLIF(TRIM(providercode::VARCHAR), '') AS provider, "
            "  NULLIF(TRIM(pos::VARCHAR), '') AS pos, "
            "  {{OBS_HOUR}} AS obs_hour, "
            "  COUNT(*) AS issue_count "
            "FROM {{PCA}} "
            "WHERE sales_date = {{TODAY}} "
            "  AND {{ISSUE_TYPE}} IS NOT NULL "
            "GROUP BY 1, 2, 3, 4 "
            "ORDER BY issue_count DESC "
            "LIMIT :limit"
        ),
        params=(
            ParameterSpec(
                name="limit",
                description="Maximum rows to return",
                default=50,
                coerce=int,
                min_value=1,
                max_value=200,
                as_literal=True,
            ),
        ),
        enforce_limit=False,
        max_rows_param="limit",
    ),
    SQLToolSpec(
        name="issue_scope_combined",
        doc="Aggregate provider/site issues by selected dimensions.",
        sql="",  # Provided dynamically via prepare().
        params=(
            ParameterSpec(
                name="provider",
                description="Provider code (case-insensitive)",
                coerce=str,
                transform=lambda v: v.strip(),
            ),
            ParameterSpec(
                name="site",
                description="Site code (case-insensitive)",
                coerce=str,
                transform=lambda v: v.strip(),
            ),
            ParameterSpec(
                name="dims",
                description="Dimensions to aggregate (comma-separated)",
                default=("obs_hour", "pos", "od"),
                kind="list",
                include_in_sql=False,
                choices=tuple(_DIMENSION_SQL.keys()),
            ),
            ParameterSpec(
                name="lookback_days",
                description="Days to look back",
                default=3,
                coerce=int,
                min_value=1,
                max_value=30,
                as_literal=True,
            ),
            ParameterSpec(
                name="limit",
                description="Maximum rows to return",
                default=50,
                coerce=int,
                min_value=1,
                max_value=500,
                as_literal=True,
            ),
        ),
        enforce_limit=False,
        max_rows_param="limit",
        prepare=_prepare_issue_scope,
    ),
    SQLToolSpec(
        name="top_site_issues_flex",
        doc=(
            "Top site issues for a provider; accepts either explicit provider parameter "
            "or infers it from the request text (e.g., 'provider QL2')."
        ),
        sql=(
            "SELECT NULLIF(TRIM(sitecode::VARCHAR), '') AS site, COUNT(*) AS issue_count "
            "FROM {{PCA}} "
            "WHERE providercode ILIKE :provider "
            "  AND sales_date >= CAST(TO_CHAR(CURRENT_DATE - :lookback_days, 'YYYYMMDD') AS INT) "
            "  AND {{ISSUE_TYPE}} IS NOT NULL "
            "  AND NULLIF(TRIM(sitecode::VARCHAR), '') IS NOT NULL "
            "GROUP BY 1 "
            "ORDER BY issue_count DESC "
            "LIMIT :limit"
        ),
        params=(
            ParameterSpec(
                name="provider",
                description="Provider code (case-insensitive); leave blank to infer from request",
                default="",
                coerce=str,
                transform=lambda v: v.strip().upper(),
            ),
            ParameterSpec(
                name="lookback_days",
                description="Days to look back",
                default=3,
                coerce=int,
                min_value=1,
                max_value=30,
                as_literal=True,
            ),
            ParameterSpec(
                name="limit",
                description="Maximum rows to return",
                default=20,
                coerce=int,
                min_value=1,
                max_value=200,
                as_literal=True,
            ),
            ParameterSpec(
                name="request",
                description="Original natural-language request (for inference fallback)",
                default="",
                coerce=str,
                include_in_sql=False,
            ),
        ),
        enforce_limit=False,
        max_rows_param="limit",
        prepare=_prepare_top_site_flex,
    ),
    SQLToolSpec(
        name="issue_scope_combined_flex",
        doc=(
            "Scope issues for a provider/site; infers provider/site from request if omitted "
            "and accepts comma-separated dims."
        ),
        sql="",
        params=(
            ParameterSpec(
                name="provider",
                description="Provider code (case-insensitive); leave blank to infer",
                default="",
                coerce=str,
                transform=lambda v: v.strip().upper(),
            ),
            ParameterSpec(
                name="site",
                description="Site code (case-insensitive); leave blank to infer",
                default="",
                coerce=str,
                transform=lambda v: v.strip().upper(),
            ),
            ParameterSpec(
                name="dims",
                description="Dimensions to aggregate (comma-separated)",
                default=("obs_hour", "pos", "od"),
                kind="list",
                include_in_sql=False,
                choices=tuple(_DIMENSION_SQL.keys()),
            ),
            ParameterSpec(
                name="lookback_days",
                description="Days to look back",
                default=3,
                coerce=int,
                min_value=1,
                max_value=30,
                as_literal=True,
            ),
            ParameterSpec(
                name="limit",
                description="Maximum rows to return",
                default=50,
                coerce=int,
                min_value=1,
                max_value=500,
                as_literal=True,
            ),
            ParameterSpec(
                name="request",
                description="Original natural-language request",
                default="",
                coerce=str,
                include_in_sql=False,
            ),
        ),
        enforce_limit=False,
        max_rows_param="limit",
        prepare=_prepare_issue_scope_flex,
    ),
)


def make_issue_scope_combined(executor: SimpleTableExecutor):
    # Backwards-compatible accessor if needed elsewhere.
    return executor.make_sql_tool(next(spec for spec in SQL_TOOL_SPECS if spec.name == "issue_scope_combined"))
