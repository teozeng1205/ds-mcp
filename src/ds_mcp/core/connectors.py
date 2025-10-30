"""
Lightweight connector registry for DS-MCP.

The previous implementation hard-coded a single Redshift connector inside a
factory class. To add an additional database you had to edit this module
directly, which scaled poorly. The new registry keeps things simple: register a
callable (class or factory function) under a name and the registry will
instantiate it on demand while caching the live connector for reuse.
"""

from __future__ import annotations

import inspect
import logging
from typing import Any, Callable, Dict, Protocol

from threevictors.dao import redshift_connector

log = logging.getLogger(__name__)


class DatabaseConnector(Protocol):
    """Minimal protocol shared by database connectors."""

    def get_connection(self) -> Any: ...

    def close(self) -> None: ...  # pragma: no cover - optional hook


ConnectorFactory = Callable[[], DatabaseConnector]


def _normalise_factory(factory: Callable[..., Any]) -> ConnectorFactory:
    """Accept both callables and classes as connector factories."""
    if inspect.isclass(factory):
        return lambda: factory()  # type: ignore[misc]
    if callable(factory):
        return factory  # type: ignore[return-value]
    raise TypeError("Connector factory must be callable")


class ConnectorRegistry:
    """Runtime registry that keeps connector factories and live instances."""

    _factories: Dict[str, ConnectorFactory] = {}
    _instances: Dict[str, DatabaseConnector] = {}

    @classmethod
    def register(cls, name: str, factory: Callable[..., Any], *, replace: bool = False) -> None:
        """
        Register *factory* under *name*.

        Args:
            name: Symbolic connector identifier (e.g. ``\"analytics\"``).
            factory: Callable or class returning a connector.
            replace: When ``True`` an existing connector is replaced.
        """
        if not name:
            raise ValueError("Connector name cannot be empty")

        if name in cls._factories:
            if not replace:
                raise ValueError(f"Connector '{name}' already registered")
            cls.remove(name)

        cls._factories[name] = _normalise_factory(factory)
        log.info("Registered database connector '%s'", name)

    @classmethod
    def get(cls, name: str) -> DatabaseConnector:
        """Return an active connector instance for *name*."""
        try:
            factory = cls._factories[name]
        except KeyError as exc:
            raise ValueError(f"Unknown connector '{name}'") from exc

        if name not in cls._instances:
            cls._instances[name] = factory()
            log.debug("Created connector '%s'", name)
        return cls._instances[name]

    @classmethod
    def remove(cls, name: str) -> None:
        """Remove a connector factory and close the cached instance if any."""
        instance = cls._instances.pop(name, None)
        cls._factories.pop(name, None)
        if instance is not None:
            cls._safe_close(name, instance)

    @classmethod
    def close_all(cls) -> None:
        """Close all cached connector instances and clear the registry."""
        for name, connector in list(cls._instances.items()):
            cls._safe_close(name, connector)
        cls._instances.clear()

    @staticmethod
    def _safe_close(name: str, connector: DatabaseConnector) -> None:
        close = getattr(connector, "close", None)
        if callable(close):
            try:
                close()
                log.info("Closed connector '%s'", name)
            except Exception as exc:  # pragma: no cover - defensive
                log.warning("Failed to close connector '%s': %s", name, exc)


class AnalyticsReader(redshift_connector.RedshiftConnector):
    """Default connector pointing at the Analytics Redshift cluster."""

    def get_properties_filename(self) -> str:
        return "database-analytics-redshift-serverless-reader.properties"


# Register default connector on import so existing code keeps working.
ConnectorRegistry.register("analytics", AnalyticsReader)


def get_connector(name: str = "analytics") -> DatabaseConnector:
    """Convenience helper mirroring the old factory interface."""
    return ConnectorRegistry.get(name)


def register_connector(name: str, factory: Callable[..., Any], *, replace: bool = False) -> None:
    """Public helper to register additional connectors."""
    ConnectorRegistry.register(name, factory, replace=replace)
