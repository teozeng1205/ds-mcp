#!/bin/bash

# Template for Table-Specific Server Launch Script
# Copy this file and customize for each table server
#
# Example usage:
#   cp run_table_template.sh run_my_table.sh
#   Edit run_my_table.sh to point to your server file

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$SCRIPT_DIR/.."
ENV_FILE="$ROOT_DIR/../.env.sh"

# Source environment variables
if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
else
    echo "Error: .env.sh not found at $ENV_FILE" >&2
    exit 1
fi

# Export PYTHONPATH to include src directory and parent for threevictors
export PYTHONPATH="$ROOT_DIR/src:$ROOT_DIR/..:$PYTHONPATH"

# Use the virtual environment's Python if available, otherwise use system python
if [ -f "$ROOT_DIR/../.venv/bin/python3" ]; then
    PYTHON="$ROOT_DIR/../.venv/bin/python3"
else
    PYTHON="python3"
fi

# TODO: Replace with your server file name
# Run the table server
# exec "$PYTHON" "$ROOT_DIR/src/ds_mcp/servers/[YOUR_TABLE]_server.py"
echo "Error: This is a template. Copy and customize for your table." >&2
exit 1
