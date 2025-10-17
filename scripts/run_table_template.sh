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

# Default AWS profile if none provided
export AWS_PROFILE="${AWS_PROFILE:-3vdev}"

# If an AWS profile is selected, prefer it over static creds from .env.sh
if [ -n "$AWS_PROFILE" ]; then
    unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
    export AWS_SDK_LOAD_CONFIG=1
    if command -v aws >/dev/null 2>&1; then
        if ! aws sts get-caller-identity --profile "$AWS_PROFILE" >/dev/null 2>&1; then
            echo "AWS SSO login for profile $AWS_PROFILE..." >&2
            aws sso login --profile "$AWS_PROFILE" || {
                echo "ERROR: aws sso login failed for $AWS_PROFILE" >&2; exit 1; }
        fi
    else
        echo "Warning: aws CLI not found; cannot auto-login SSO." >&2
    fi
fi

# Export PYTHONPATH to include src directory and parent for threevictors
export PYTHONPATH="$ROOT_DIR/src:$ROOT_DIR/..:$PYTHONPATH"

# Use the virtual environment's Python if available, otherwise use system python
if [ -f "$ROOT_DIR/.venv/bin/python3" ]; then
    PYTHON="$ROOT_DIR/.venv/bin/python3"
elif [ -f "$ROOT_DIR/../.venv/bin/python3" ]; then
    PYTHON="$ROOT_DIR/../.venv/bin/python3"
else
    PYTHON="python3"
fi

# TODO: Replace with your server file name
# Run the table server
# exec "$PYTHON" "$ROOT_DIR/src/ds_mcp/servers/[YOUR_TABLE]_server.py"
echo "Error: This is a template. Copy and customize for your table." >&2
exit 1
