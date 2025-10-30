"""
Table discovery utilities for DS-MCP.

Table packages expose a ``TABLE`` constant containing a
:class:`ds_mcp.tables.base.TableBundle`.
"""

from __future__ import annotations

import importlib
import pkgutil
from functools import lru_cache
from typing import Dict, Iterable, List, Tuple

from ds_mcp.core.registry import TableRegistry

from .base import SimpleTableDefinition, TableBundle

__all__ = [
    "get_table_bundle",
    "get_table_definition",
    "list_available_tables",
    "register_all_tables",
]


@lru_cache
def _discover_tables() -> Dict[str, TableBundle]:
    """Import available table packages and collect their bundles."""
    tables: dict[str, TableBundle] = {}
    for module_info in pkgutil.iter_modules(__path__):
        if not module_info.ispkg or module_info.name.startswith("_"):
            continue

        module = importlib.import_module(f"{__name__}.{module_info.name}")
        bundle = getattr(module, "TABLE", None)
        if bundle is None:
            continue
        if isinstance(bundle, TableBundle):
            tables[bundle.slug] = bundle
    return tables


def list_available_tables() -> List[Tuple[str, str]]:
    """Return ``(slug, display_name)`` pairs for discovered tables."""
    tables = _discover_tables()
    return sorted((slug, bundle.display_name) for slug, bundle in tables.items())


def get_table_bundle(slug: str) -> TableBundle:
    """Return the table bundle for *slug*."""
    tables = _discover_tables()
    try:
        return tables[slug]
    except KeyError as exc:  # pragma: no cover - defensive
        raise KeyError(f"Unknown table slug: {slug}") from exc


def get_table_definition(slug: str) -> SimpleTableDefinition:
    """Return the ``SimpleTableDefinition`` for *slug*."""
    return get_table_bundle(slug).definition


def register_all_tables(registry: TableRegistry, only: Iterable[str] | None = None) -> List[str]:
    """
    Register discovered tables with *registry*.

    Args:
        registry: Target table registry.
        only: Optional iterable of table slugs to register. When omitted all
            discovered tables are registered.

    Returns:
        List of slugs that were registered.
    """
    tables = _discover_tables()

    if only is not None:
        slugs = [slug for slug in only]
        missing = [slug for slug in slugs if slug not in tables]
        if missing:
            raise KeyError(f"Unknown table slug(s): {', '.join(missing)}")
    else:
        slugs = sorted(tables.keys())

    for slug in slugs:
        tables[slug].register(registry)

    return slugs
