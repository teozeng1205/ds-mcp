# Guide: Adding New Tables to DS-MCP

This guide walks you through adding a new database table to the DS-MCP server.

## Overview

Adding a new table involves:
1. Creating a table module directory
2. Defining table configuration
3. Implementing MCP tools
4. Registering the table
5. Testing

The entire process takes about 15-30 minutes and requires no changes to core framework code!

## Step-by-Step Guide

### Step 1: Create Table Module

```bash
cd src/ds_mcp/tables
mkdir my_new_table
cd my_new_table
touch __init__.py config.py tools.py
```

**File structure:**
```
src/ds_mcp/tables/my_new_table/
├── __init__.py       # Module initialization
├── config.py         # Table configuration
├── tools.py          # MCP tools implementation
└── queries.py        # (Optional) SQL query templates
```

### Step 2: Define Table Configuration

Edit `config.py`:

```python
"""
Configuration for my_new_table.
"""

from ds_mcp.core.registry import TableConfig


def get_table_config() -> TableConfig:
    """
    Get the configuration for my_new_table.

    Returns:
        TableConfig instance with table metadata and tools
    """
    from ds_mcp.tables.my_new_table import tools

    config = TableConfig(
        name="analytics.my_new_table",
        display_name="My New Table",
        description="Description of what this table contains",
        schema_name="analytics",
        table_name="my_new_table",
        connector_type="analytics",  # or "mysql", etc.
        tools=[
            tools.query_data,
            tools.get_schema,
            tools.get_summary,
        ],
        metadata={
            "version": "1.0",
            "primary_key": ["id", "date"],
            "key_metrics": ["metric1", "metric2"],
            "dimensions": ["dimension1", "dimension2"]
        }
    )

    return config
```

### Step 3: Implement Tools

Edit `tools.py`:

```python
"""
MCP tools for my_new_table.
"""

import json
import logging
from ds_mcp.core.connectors import DatabaseConnectorFactory

log = logging.getLogger(__name__)

TABLE_NAME = "analytics.my_new_table"


def _get_connector():
    """Get the database connector for this table."""
    return DatabaseConnectorFactory.get_connector("analytics")


def query_data(sql_query: str) -> str:
    """
    Execute a SQL query against my_new_table.

    This tool allows querying the table with custom SQL.

    Args:
        sql_query: SQL SELECT query to execute

    Returns:
        JSON string with query results

    Example:
        "SELECT * FROM analytics.my_new_table WHERE date = 20251014 LIMIT 10"
    """
    try:
        # Validate query
        query_upper = sql_query.upper().strip()

        if not query_upper.startswith('SELECT'):
            return json.dumps({"error": "Only SELECT queries allowed"}, indent=2)

        if TABLE_NAME.upper().replace('.', '') not in query_upper.replace('.', ''):
            return json.dumps({"error": f"Query must reference {TABLE_NAME}"}, indent=2)

        # Execute query
        connector = _get_connector()

        with connector.get_connection().cursor() as cursor:
            cursor.execute(sql_query)
            colnames = [desc[0] for desc in cursor.description]
            records = cursor.fetchmany(100)

            results = []
            for record in records:
                row_dict = {col: record[i] for i, col in enumerate(colnames)}
                results.append(row_dict)

            return json.dumps({
                "columns": colnames,
                "rows": results,
                "row_count": len(results),
                "truncated": len(results) == 100
            }, indent=2)

    except Exception as e:
        log.error(f"Error in query_data: {e}")
        return json.dumps({"error": str(e)}, indent=2)


def get_schema() -> str:
    """
    Get schema information for my_new_table.

    Returns:
        JSON string with column names and types
    """
    try:
        query = f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'analytics'
          AND table_name = 'my_new_table'
        ORDER BY ordinal_position;
        """

        connector = _get_connector()

        with connector.get_connection().cursor() as cursor:
            cursor.execute(query)
            colnames = [desc[0] for desc in cursor.description]
            records = cursor.fetchall()

            results = [
                {col: record[i] for i, col in enumerate(colnames)}
                for record in records
            ]

            return json.dumps({
                "table": TABLE_NAME,
                "columns": results
            }, indent=2)

    except Exception as e:
        log.error(f"Error in get_schema: {e}")
        return json.dumps({"error": str(e)}, indent=2)


def get_summary() -> str:
    """
    Get summary statistics for my_new_table.

    Returns:
        JSON with row counts, date ranges, etc.
    """
    try:
        query = f"""
        SELECT
            COUNT(*) as total_rows,
            MIN(date_column) as min_date,
            MAX(date_column) as max_date
        FROM {TABLE_NAME};
        """

        connector = _get_connector()

        with connector.get_connection().cursor() as cursor:
            cursor.execute(query)
            colnames = [desc[0] for desc in cursor.description]
            record = cursor.fetchone()

            result = {col: record[i] for i, col in enumerate(colnames)}

            return json.dumps(result, indent=2)

    except Exception as e:
        log.error(f"Error in get_summary: {e}")
        return json.dumps({"error": str(e)}, indent=2)
```

### Step 4: Create Module Init

Edit `__init__.py`:

```python
"""
My New Table module.
"""

from ds_mcp.core.registry import TableRegistry
from ds_mcp.tables.my_new_table.config import get_table_config

__all__ = ["register_table"]


def register_table(registry: TableRegistry) -> None:
    """
    Register my_new_table with the registry.

    Args:
        registry: TableRegistry instance
    """
    config = get_table_config()
    registry.register_table(config)
```

### Step 5: Register in Tables Init

Edit `src/ds_mcp/tables/__init__.py` and add your table:

```python
def register_all_tables(registry: TableRegistry) -> None:
    """Register all available tables."""

    # Existing tables
    from ds_mcp.tables.market_anomalies_v3 import register_table as register_market
    register_market(registry)

    # Add your new table here!
    from ds_mcp.tables.my_new_table import register_table as register_my_table
    register_my_table(registry)
```

### Step 6: Test Your Table

Create a test file `tests/test_my_new_table.py`:

```python
"""
Test for my_new_table tools.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from ds_mcp.tables.my_new_table import tools


def test_get_schema():
    """Test schema retrieval."""
    result = tools.get_schema()
    print("Schema:", result)
    assert "error" not in result.lower()


def test_query_data():
    """Test data query."""
    query = "SELECT * FROM analytics.my_new_table LIMIT 5"
    result = tools.query_data(query)
    print("Query result:", result)
    assert "error" not in result.lower()


if __name__ == "__main__":
    test_get_schema()
    test_query_data()
    print("All tests passed!")
```

Run the test:
```bash
python tests/test_my_new_table.py
```

### Step 7: Run the Server

```bash
cd scripts
python -m ds_mcp.servers.your_table_server
```

Your new table tools should now be available!

## Best Practices

### Tool Design

1. **Clear docstrings**: Include description, parameters, returns, and examples
2. **Error handling**: Always return JSON with `{"error": "message"}` on errors
3. **Validation**: Validate SQL queries to prevent injection and ensure correct table
4. **Row limits**: Cap results at reasonable limits (e.g., 100 rows)
5. **Logging**: Use `log.info()` and `log.error()` for debugging

### Configuration

1. **Metadata**: Include helpful metadata like primary keys, key metrics
2. **Display names**: Use clear, human-readable names
3. **Descriptions**: Explain what the table contains and its purpose

### Testing

1. **Test each tool**: Verify all tools work independently
2. **Test edge cases**: Empty results, errors, large queries
3. **Integration tests**: Test with real MCP client if possible

## Examples

### Example 1: Simple Lookup Table

For a simple reference table with few columns:

```python
# Minimal tools needed
tools=[
    tools.query_data,      # Generic query
    tools.get_all_values,  # Get all reference data
]
```

### Example 2: Time-Series Table

For tables with date-based data:

```python
tools=[
    tools.query_data,
    tools.get_schema,
    tools.get_date_range,        # Min/max dates
    tools.get_data_by_date,      # Filter by date
    tools.get_latest_data,       # Most recent
]
```

### Example 3: Aggregation Table

For pre-aggregated metrics:

```python
tools=[
    tools.query_data,
    tools.get_schema,
    tools.get_metrics_summary,   # Available metrics
    tools.get_by_dimension,      # Filter by dimension
    tools.get_top_n,             # Top N by metric
]
```

## Common Patterns

### Pattern 1: Helper Function for Queries

```python
def _execute_query(sql: str, max_rows: int = 100) -> str:
    """Common query execution logic."""
    try:
        connector = _get_connector()
        with connector.get_connection().cursor() as cursor:
            cursor.execute(sql)
            # ... common processing ...
            return json.dumps(results, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)
```

### Pattern 2: Query Templates

Create `queries.py` for complex queries:

```python
GET_SUMMARY = """
SELECT
    dimension,
    COUNT(*) as count,
    AVG(metric) as avg_metric
FROM {table_name}
WHERE date >= {start_date}
GROUP BY dimension
ORDER BY count DESC
LIMIT {limit};
"""

def get_summary_query(start_date: int, limit: int = 100) -> str:
    return GET_SUMMARY.format(
        table_name=TABLE_NAME,
        start_date=start_date,
        limit=limit
    )
```

### Pattern 3: Parameter Validation

```python
def validate_date(date: int) -> bool:
    """Validate YYYYMMDD format."""
    if not isinstance(date, int):
        return False
    s = str(date)
    if len(s) != 8:
        return False
    year = int(s[:4])
    month = int(s[4:6])
    day = int(s[6:8])
    return 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31
```

## Troubleshooting

### Issue: Tools not appearing

**Solution**: Check that:
1. Table is registered in `register_all_tables()`
2. Tools are listed in `config.py`
3. No import errors (check server logs)

### Issue: Database connection errors

**Solution**: Verify:
1. AWS credentials are set
2. Connector type matches your database
3. Table name is correct (schema.table)

### Issue: Query errors

**Solution**: Test queries directly:
```python
from ds_mcp.tables.my_table import tools
result = tools.query_data("SELECT * FROM ... LIMIT 1")
print(result)
```

## Next Steps

1. Add more sophisticated tools for your specific use case
2. Add query templates for common queries
3. Create examples showing how to use your table
4. Document your table in `docs/README.md`

## Resources

- [MCP Documentation](https://modelcontextprotocol.io)
- [FastMCP Guide](https://github.com/modelcontextprotocol/python-sdk)
- [Example: market_anomalies_v3](../src/ds_mcp/tables/market_anomalies_v3/)
