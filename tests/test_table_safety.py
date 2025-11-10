from __future__ import annotations

import json
import pytest

from ds_mcp.tables.base import Table


def _stub_table() -> Table:
    return Table(
        slug="stub",
        schema_name="analytics",
        table_name="dummy",
        description="Stub table for tests",
        key_columns=("id", "snapshot_ts"),
        partition_columns=("snapshot_ts",),
    )


def test_describe_table_includes_intro_and_keys():
    table = _stub_table()
    describe = table.make_describe_tool()
    payload = json.loads(describe())
    assert payload["table"] == table.full_name
    assert "Stub table" in payload["introduction"]
    assert "id" in payload["key_columns"]


def test_query_tool_rejects_non_select():
    table = _stub_table()
    query = table.make_query_tool()
    with pytest.raises(ValueError):
        query("DELETE FROM dummy")


def test_query_tool_expands_macros(monkeypatch):
    table = _stub_table()

    def fake_run(sql, params=None):
        return {"columns": ["a"], "rows": [{"a": 1}], "row_count": 1, "sql": sql, "truncated": False}

    monkeypatch.setattr(table, "_run_query", fake_run)
    query = table.make_query_tool()
    payload = json.loads(query("SELECT * FROM {{TABLE}}", max_rows=5))
    assert table.full_name in payload["sql"]
    assert "LIMIT 5" in payload["sql"].upper()
