#!/bin/bash

# Wrapper script to run MCP server with environment variables
# This script sources .env.sh and then runs the MCP server

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

# Run the MCP server
exec "$PYTHON" "$ROOT_DIR/src/ds_mcp/server.py"
