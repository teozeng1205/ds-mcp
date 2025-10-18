"""
MCP tools for monitoring_prod.provider_combined_audit.

Thin wrappers that delegate to ProviderCombinedAuditService for implementation.
"""

from __future__ import annotations

from typing import List, Optional

from ds_mcp.tables.provider_combined_audit.service import ProviderCombinedAuditService as Svc


def query_audit(sql_query: str, params: Optional[List] = None) -> str:
    return Svc.query_audit(sql_query, params=params)

def issue_scope_quick_by_site(provider: str, site: str, lookback_days: int = 3, per_dim_limit: int = 5) -> str:
    """
    DEPRECATED: Quick scope for provider+site.

    This wrapper now calls issue_scope_combined with dims ['obs_hour','pos'] and returns
    a single table to replace the former multi-call behavior.
    """
    limit = min(max(1, per_dim_limit), 50)
    return issue_scope_combined(provider=provider, site=site, dims=["obs_hour", "pos"], lookback_days=lookback_days, limit=limit)


def issue_scope_by_site_dimensions(provider: str, site: str, dims: List[str], lookback_days: int = 3, per_dim_limit: int = 5) -> str:
    limit = min(max(1, per_dim_limit), 1000)
    return issue_scope_combined(provider=provider, site=site, dims=dims, lookback_days=lookback_days, limit=limit)


def get_table_schema() -> str:
    return Svc.get_table_schema()

def top_site_issues(provider: str, lookback_days: int = 7, limit: int = 10) -> str:
    return Svc.top_site_issues(provider, lookback_days=lookback_days, limit=limit)

def issue_scope_combined(provider: str, site: str, dims: List[str], lookback_days: int = 7, limit: int = 200) -> str:
    return Svc.issue_scope_combined(provider, site, dims=dims, lookback_days=lookback_days, limit=limit)


def overview_site_issues_today(per_dim_limit: int = 50) -> str:
    return Svc.overview_site_issues_today(per_dim_limit=per_dim_limit)


def list_provider_sites(provider: str, lookback_days: int = 7, limit: int = 10) -> str:
    return Svc.list_provider_sites(provider, lookback_days=lookback_days, limit=limit)
