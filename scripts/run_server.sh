#!/bin/bash

# Quick start script for MCP Database Server
# This script sources the environment variables and runs the server

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$SCRIPT_DIR/.."

# Source environment variables (prefer env.sh, fallback to .env.sh)
if [ -f "$ROOT_DIR/../env.sh" ]; then
  echo "Loading environment variables from ../env.sh..." >&2
  # shellcheck disable=SC1090
  source "$ROOT_DIR/../env.sh"
  echo "✓ Environment variables loaded" >&2
elif [ -f "$ROOT_DIR/env.sh" ]; then
  echo "Loading environment variables from env.sh..." >&2
  # shellcheck disable=SC1090
  source "$ROOT_DIR/env.sh"
  echo "✓ Environment variables loaded" >&2
elif [ -f "$ROOT_DIR/.env.sh" ]; then
  echo "Loading environment variables from .env.sh..." >&2
  # shellcheck disable=SC1090
  source "$ROOT_DIR/.env.sh"
  echo "✓ Environment variables loaded" >&2
elif [ -f "$ROOT_DIR/../.env.sh" ]; then
  echo "Loading environment variables from parent .env.sh..." >&2
  # shellcheck disable=SC1090
  source "$ROOT_DIR/../.env.sh"
  echo "✓ Environment variables loaded" >&2
else
  echo "Error: env.sh/.env.sh not found near $ROOT_DIR. Please create env.sh at repo root." >&2
  exit 1
fi

# Setup AWS authentication (validates existing session or triggers SSO login) unless bypassed
if [ "${DS_MCP_SKIP_AWS:-}" = "1" ]; then
  echo "Skipping AWS setup due to DS_MCP_SKIP_AWS=1" >&2
else
  # shellcheck disable=SC1090
  source "$SCRIPT_DIR/common_aws_setup.sh"
fi

# Export PYTHONPATH to include ds-mcp src only (threevictors should be installed via pip)
export PYTHONPATH="$ROOT_DIR/src:${PYTHONPATH:-}"

echo "Starting DS-MCP Server..." >&2
echo "Press Ctrl+C to stop" >&2
echo "" >&2

# Choose Python from local .venv if available
if [ -f "$ROOT_DIR/.venv/bin/python3" ]; then
    PYTHON="$ROOT_DIR/.venv/bin/python3"
elif [ -f "$ROOT_DIR/../.venv/bin/python3" ]; then
    PYTHON="$ROOT_DIR/../.venv/bin/python3"
else
    echo "Error: .venv not found at $ROOT_DIR/.venv or parent. Please create a virtualenv and install deps." >&2
    echo "Hint: python3 -m venv ../.venv && source ../.venv/bin/activate && pip install -r requirements.txt && (cd ../ds-threevictors && pip install -e .)" >&2
    exit 1
fi

# Run the server
"$PYTHON" "$ROOT_DIR/src/ds_mcp/server.py"
