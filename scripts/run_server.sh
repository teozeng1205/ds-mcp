#!/bin/bash

# Quick start script for MCP Database Server
# This script sources the environment variables and runs the server

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$SCRIPT_DIR/.."

# Source environment variables
if [ -f "$ROOT_DIR/../.env.sh" ]; then
    echo "Loading environment variables from .env.sh..."
    source "$ROOT_DIR/../.env.sh"
    echo "✓ Environment variables loaded"
else
    echo "Error: .env.sh not found at $ROOT_DIR/../.env.sh"
    exit 1
fi

# Check if AWS credentials are set
if [ -z "$AWS_ACCESS_KEY_ID" ]; then
    echo "Error: AWS_ACCESS_KEY_ID not set"
    exit 1
fi

# Export PYTHONPATH to include src directory and parent for threevictors
export PYTHONPATH="$ROOT_DIR/src:$ROOT_DIR/..:$PYTHONPATH"

echo "Starting DS-MCP Server..."
echo "Press Ctrl+C to stop"
echo ""

# Run the server
python "$ROOT_DIR/src/ds_mcp/server.py"
