#!/bin/bash

# Launch script for Market Anomalies V3 server
# This script sources environment variables and runs only the market anomalies server

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$SCRIPT_DIR/.."
ENV_FILE="$ROOT_DIR/.env.sh"
if [ ! -f "$ENV_FILE" ] && [ -f "$ROOT_DIR/../.env.sh" ]; then
    ENV_FILE="$ROOT_DIR/../.env.sh"
fi

# Source environment variables
if [ -f "$ENV_FILE" ]; then
    # shellcheck disable=SC1090
    source "$ENV_FILE"
else
    echo "Error: .env.sh not found at $ENV_FILE" >&2
    exit 1
fi

# Setup AWS authentication (validates existing session or triggers SSO login)
source "$SCRIPT_DIR/common_aws_setup.sh"

# Export PYTHONPATH to include src directory and parent for threevictors
export PYTHONPATH="$ROOT_DIR/src:$ROOT_DIR/..:$PYTHONPATH"

# Require a repo-local or parent .venv Python
if [ -f "$ROOT_DIR/.venv/bin/python3" ]; then
    PYTHON="$ROOT_DIR/.venv/bin/python3"
elif [ -f "$ROOT_DIR/../.venv/bin/python3" ]; then
    PYTHON="$ROOT_DIR/../.venv/bin/python3"
else
    echo "Error: .venv not found at $ROOT_DIR/.venv or parent. Please create a virtualenv and install deps." >&2
    echo "Hint: python3 -m venv ../.venv && source ../.venv/bin/activate && pip install -r requirements.txt && (cd ../ds-threevictors && pip install -e .)" >&2
    exit 1
fi

# Run the market anomalies server
exec "$PYTHON" "$ROOT_DIR/src/ds_mcp/servers/market_anomalies_server.py"
