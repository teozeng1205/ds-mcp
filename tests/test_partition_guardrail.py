#!/usr/bin/env python3
from __future__ import annotations

import json

from ds_mcp.tables.base import (
    PartitionGuardrail,
    SimpleTableExecutor,
    build_table,
)


def _fake_run_select(self, sql, params=None, limit=None):
    return {"columns": ["id"], "rows": [{"id": 1}], "row_count": 1, "truncated": False}


def test_guardrail_blocks_unpartitioned_queries(monkeypatch):
    table = build_table(
        slug="test_guardrail",
        schema_name="analytics",
        table_name="dummy",
        display_name="Dummy",
        description="",
        partition_guardrail=PartitionGuardrail(column="sales_date"),
    )
    executor = SimpleTableExecutor(table.definition)
    monkeypatch.setattr(SimpleTableExecutor, "_run_select", _fake_run_select, raising=False)
    result = executor.fetch("SELECT * FROM dummy", enforce_limit=False)
    assert "error" in result
    assert "sales_date" in result["error"]


def test_guardrail_warn_mode(monkeypatch, caplog):
    table = build_table(
        slug="test_guardrail_warn",
        schema_name="analytics",
        table_name="dummy_warn",
        display_name="Dummy Warn",
        description="",
        partition_guardrail=PartitionGuardrail(column="sales_date", behavior="warn"),
    )
    executor = SimpleTableExecutor(table.definition)
    monkeypatch.setattr(SimpleTableExecutor, "_run_select", _fake_run_select, raising=False)
    caplog.clear()
    payload = executor.fetch("SELECT * FROM dummy_warn", enforce_limit=False)
    assert json.dumps(payload)
    assert any("Partition guardrail" in message for message in caplog.text.splitlines())
