# MCP Server for Market Level Anomalies V3

A Model Context Protocol (MCP) server for querying the `analytics.market_level_anomalies_v3` table using the ds-threevictors library.

## Overview

This MCP server provides 14 specialized tools for accessing and analyzing market-level pricing anomalies data. The tools are designed to support various use cases including anomaly detection, impact analysis, trend identification, and revenue optimization.

### Dataset Summary

- **Table**: `analytics.market_level_anomalies_v3`
- **Total Records**: ~9.4 million
- **Date Range**: 20250914 - 20251014 (31 days)
- **Customers**: AS, SK, B6, INS
- **Anomaly Rate**: 4.52% of records

## Available MCP Tools

### 1. Discovery Tools

#### `get_available_customers()`
Get list of all customers with their record counts and date ranges.

**Returns**: Customer codes, total/anomaly record counts, first/last dates

#### `get_date_range(customer)`
Get available date range for a specific customer.

**Parameters**:
- `customer` (str): Customer code (e.g., 'AS', 'SK', 'B6', 'INS')

**Returns**: Min/max dates, distinct date count, total records

### 2. Summary & Analytics Tools

#### `get_anomaly_summary_by_date(customer, sales_date)`
Get comprehensive summary statistics for a specific date.

**Parameters**:
- `customer` (str): Customer code
- `sales_date` (int): Date in YYYYMMDD format (e.g., 20251003)

**Returns**: Total records, anomaly counts by type (frequency/magnitude), average/max impact scores, breakdown by competitive position

**Use Case**: Daily anomaly overview, trend analysis

### 3. Impact-Based Retrieval

#### `get_anomalies_by_customer(customer, start_date, end_date, max_rows=100)`
Get all anomalies for a customer within date range, sorted by impact.

**Parameters**:
- `customer` (str): Customer code
- `start_date` (int): Start date YYYYMMDD
- `end_date` (int): End date YYYYMMDD
- `max_rows` (int): Max rows to return (default 100)

**Returns**: All fields for anomaly records

#### `get_top_anomalies_by_impact(customer, sales_date, min_impact_score=10.0, max_rows=50)`
Get highest impact anomalies for a specific date.

**Parameters**:
- `customer` (str): Customer code
- `sales_date` (int): Date YYYYMMDD
- `min_impact_score` (float): Minimum impact_score_v2 threshold (default 10.0)
- `max_rows` (int): Max rows (default 50)

**Returns**: Key fields including seg_mkt, cp, impact scores, anomaly types, revenue metrics

**Use Case**: Focus on most critical anomalies requiring immediate attention

### 4. Dimension-Based Filters

#### `get_anomalies_by_market(customer, market, start_date, end_date, max_rows=100)`
Get anomalies for a specific market (e.g., 'BOS-ATL', 'LAX-SYD').

**Parameters**:
- `customer` (str): Customer code
- `market` (str): Market code (e.g., 'BOS-ATL')
- `start_date` (int): Start date YYYYMMDD
- `end_date` (int): End date YYYYMMDD
- `max_rows` (int): Max rows (default 100)

**Returns**: Market-specific anomaly details with trend info

**Use Case**: Deep dive into specific route performance

#### `get_anomalies_by_competitive_position(customer, competitive_position, sales_date, max_rows=100)`
Filter anomalies by competitive position.

**Parameters**:
- `customer` (str): Customer code
- `competitive_position` (str): One of 'Undercut', 'Overpriced', 'Match', 'N/A'
- `sales_date` (int): Date YYYYMMDD
- `max_rows` (int): Max rows (default 100)

**Returns**: Anomalies with frequency/magnitude values, directions, and scores

**Use Case**: Analyze pricing strategy performance (e.g., all "Overpriced" anomalies)

#### `get_anomalies_by_region(customer, region_name, start_date, end_date, max_rows=100)`
Get anomalies for a specific region.

**Parameters**:
- `customer` (str): Customer code
- `region_name` (str): Region (e.g., 'Domestic', 'IC-Europe', 'North America - NYC')
- `start_date` (int): Start date YYYYMMDD
- `end_date` (int): End date YYYYMMDD
- `max_rows` (int): Max rows (default 100)

**Returns**: Region-specific anomaly data

**Use Case**: Regional performance analysis

### 5. Anomaly Type Filters

#### `get_frequency_anomalies(customer, sales_date, min_freq_pcnt=0.1, max_rows=50)`
Get records with frequency percentage anomalies.

**Parameters**:
- `customer` (str): Customer code
- `sales_date` (int): Date YYYYMMDD
- `min_freq_pcnt` (float): Minimum frequency % threshold (default 0.1 = 10%)
- `max_rows` (int): Max rows (default 50)

**Returns**: Frequency anomaly details including bounds, 7-day average, itinerary count

**Use Case**: Identify sudden changes in booking frequency

#### `get_magnitude_anomalies(customer, sales_date, min_mag_pcnt=10.0, max_rows=50)`
Get records with magnitude (price) percentage anomalies.

**Parameters**:
- `customer` (str): Customer code
- `sales_date` (int): Date YYYYMMDD
- `min_mag_pcnt` (float): Minimum magnitude % threshold (default 10.0%)
- `max_rows` (int): Max rows (default 50)

**Returns**: Magnitude anomaly details including nominal values and bounds

**Use Case**: Identify significant price changes

### 6. Trend Analysis Tools

#### `get_trending_anomalies(customer, sales_date, direction='up', max_rows=50)`
Get anomalies with specific directional trends.

**Parameters**:
- `customer` (str): Customer code
- `sales_date` (int): Date YYYYMMDD
- `direction` (str): 'up' or 'down' (default 'up')
- `max_rows` (int): Max rows (default 50)

**Returns**: Anomalies with direction indicators, direction scores, 7-day differences

**Use Case**: Track upward or downward trending anomalies

### 7. Revenue-Based Filtering

#### `get_high_revenue_anomalies(customer, sales_date, min_revenue_score=0.5, max_rows=50)`
Get anomalies for high-revenue markets.

**Parameters**:
- `customer` (str): Customer code
- `sales_date` (int): Date YYYYMMDD
- `min_revenue_score` (float): Min revenue score 0.0-1.0 (default 0.5)
- `max_rows` (int): Max rows (default 50)

**Returns**: Revenue score, estimated revenue, passenger count, impact scores

**Use Case**: Prioritize anomalies in revenue-critical markets

### 8. Search Tool

#### `search_anomalies(customer, sales_date, search_text, max_rows=50)`
Search for anomalies by text in market, segment, or region.

**Parameters**:
- `customer` (str): Customer code
- `sales_date` (int): Date YYYYMMDD
- `search_text` (str): Text to search (e.g., 'BOS', 'Europe', 'Premium')
- `max_rows` (int): Max rows (default 50)

**Returns**: Matching records with anomaly indicators

**Use Case**: Quick lookup of specific markets/regions

## Installation

### Prerequisites

1. Python 3.10 or higher
2. AWS credentials configured (via `.env.sh`)
3. Access to the configuration S3 bucket with database properties
4. The `ds-threevictors` package installed

### Setup Steps

```bash
# 1. Source environment variables
source ../.env.sh

# 2. Install dependencies
pip install -r requirements.txt

# 3. Install ds-threevictors
cd ../ds-threevictors && pip install -e . && cd ../ds-mcp

# 4. Test imports
python test_imports.py

# 5. Test tools (optional)
python test_tools.py
```

## Running the Server

### Development Mode (one-table servers)

```bash
# Provider Combined Audit (stdio)
bash scripts/run_mcp_server.sh provider

# Market Anomalies V3 (stdio)
bash scripts/run_mcp_server.sh anomalies

# Python module entrypoint (single or multiple tables)
python -m ds_mcp.server --table provider
python -m ds_mcp.server --table anomalies
```

### Connecting from an MCP client

Use your preferred MCP client to launch or connect to the server. Configure environment variables as needed for AWS and credentials.

## Usage Examples

### Example 1: Daily Anomaly Report

```python
# Get summary for today
summary = get_anomaly_summary_by_date("AS", 20251013)

# Get top 10 highest impact
top_anomalies = get_top_anomalies_by_impact("AS", 20251013, min_impact_score=15.0, max_rows=10)

# Focus on high-revenue markets
revenue_anomalies = get_high_revenue_anomalies("AS", 20251013, min_revenue_score=0.7, max_rows=10)
```

### Example 2: Regional Analysis

```python
# Get all anomalies in Domestic region for past week
domestic = get_anomalies_by_region("AS", "Domestic", 20251007, 20251013, max_rows=100)

# Search for Boston-related anomalies
boston = search_anomalies("AS", 20251013, "BOS", max_rows=50)
```

### Example 3: Competitive Position Analysis

```python
# Analyze "Overpriced" anomalies
overpriced = get_anomalies_by_competitive_position("AS", "Overpriced", 20251013)

# Get "Undercut" anomalies
undercut = get_anomalies_by_competitive_position("AS", "Undercut", 20251013)
```

### Example 4: Trend Detection

```python
# Find upward trending anomalies
up_trends = get_trending_anomalies("AS", 20251013, direction="up")

# Find downward trending anomalies
down_trends = get_trending_anomalies("AS", 20251013, direction="down")
```

## Data Dictionary

### Key Fields

| Field | Description | Type |
|-------|-------------|------|
| `customer` | Customer code (AS, SK, B6, INS) | String |
| `sales_date` | Date in YYYYMMDD format | Integer |
| `mkt` | Market (e.g., BOS-ATL) | String |
| `seg` | Segment description | String |
| `seg_mkt` | Combined segment:market | String |
| `cp` | Competitive position (Undercut/Overpriced/Match/N/A) | String |
| `region_name` | Geographic region | String |
| `cabin_group` | Economy or Premium | String |
| `any_anomaly` | 1 if any anomaly detected, 0 otherwise | Integer |
| `freq_pcnt_anomaly` | 1 if frequency anomaly | Integer |
| `mag_pcnt_anomaly` | 1 if magnitude % anomaly | Integer |
| `mag_nominal_anomaly` | 1 if magnitude nominal anomaly | Integer |
| `freq_pcnt_val` | Frequency percentage value (0-1) | Float |
| `mag_pcnt_val` | Magnitude percentage value | Float |
| `mag_nominal_val` | Magnitude nominal value (dollars) | Float |
| `freq_pcnt_direction` | Direction: 'up', 'down', or '' | String |
| `mag_pcnt_direction` | Direction: 'UP', 'DOWN', or '' | String |
| `impact_score_v2` | Primary impact score (-3.95 to 80.35) | Float |
| `impact_score` | Secondary impact score (-3.95 to 100.0) | Float |
| `direction_score` | Trend direction score (0-10) | Float |
| `oag_score` | OAG capacity score | Float |
| `revenue_score` | Revenue importance (0-1) | Float |
| `estimated_revenue` | Estimated revenue in dollars | Float |
| `midt_pax` | Passenger count | Integer |

## Architecture

```
ds-mcp/
├── server.py           # Main MCP server (14 tools)
├── test_tools.py       # Tool testing script
├── test_imports.py     # Import validation
├── requirements.txt    # Dependencies
├── run_server.sh       # Launch script
└── README.md          # This file

Dependencies:
└── ds-threevictors/   # Database utilities library
    ├── dao/           # Redshift connector
    ├── config_reader/ # S3 config reader
    └── secrets_manager/ # AWS Secrets Manager
```

## Security

- Database credentials retrieved from AWS Secrets Manager
- AWS credentials required via environment variables
- Never commit `.env.sh` or credentials to version control
- SQL queries use parameterized values where possible

## Error Handling

All tools return JSON with either:
- **Success**: `{"columns": [...], "rows": [...], "row_count": N, "truncated": boolean}`
- **Error**: `{"error": "error message"}`

Errors are logged to stderr to maintain MCP protocol compliance.

## Performance Notes

- Database connection is initialized once and reused across tool invocations
- Default `max_rows` limits prevent excessive data transfer
- Queries use appropriate indexes on `customer`, `sales_date`, and `any_anomaly`
- Results include `truncated` flag when hitting max_rows limit

## Development

### Adding New Tools

1. Define function with `@mcp.tool()` decorator
2. Add type hints for all parameters
3. Write comprehensive docstring
4. Return JSON string via `execute_query()`
5. Test with `test_tools.py`

Example:
```python
@mcp.tool()
def my_new_tool(customer: str, sales_date: int) -> str:
    """
    Tool description here.

    Args:
        customer: Customer code
        sales_date: Date in YYYYMMDD format

    Returns:
        JSON string with results
    """
    query = f"SELECT * FROM ... WHERE customer = '{customer}' ..."
    return execute_query(query, max_rows=100)
```

## Troubleshooting

### Connection Issues

```bash
# Verify AWS credentials
echo $AWS_ACCESS_KEY_ID

# Test database connection

# Check S3 config access
aws s3 ls s3://config-server-*/
```

### Import Errors

```bash
# Reinstall ds-threevictors
cd ../ds-threevictors && pip install -e . && cd ../ds-mcp

# Verify imports
python test_imports.py
```

### MCP Server Issues

- Ensure no stdout pollution (use stderr for logging)
- Test with `mcp dev server.py` first
- Check client logs
- Verify JSON response format

## Testing

```bash
# Test all imports
python test_imports.py

# Test database exploration

# Test MCP tools
python test_tools.py

# Syntax check
python -m py_compile server.py
```

## References

- [Model Context Protocol](https://github.com/modelcontextprotocol)
- [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)
- [FastMCP Documentation](https://github.com/modelcontextprotocol/python-sdk)
- ds-threevictors library (internal)

## Version History

- **v1.0** - Initial release with 14 tools for market_level_anomalies_v3
- Focused on anomaly detection, impact analysis, and revenue optimization
- Supports 4 customers: AS, SK, B6, INS
- Date range: 2025-09-14 to 2025-10-14
