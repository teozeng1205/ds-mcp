#!/bin/bash

# Launch script for Provider Combined Audit server

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

# Run the provider combined audit server
exec "$PYTHON" "$ROOT_DIR/src/ds_mcp/servers/provider_combined_audit_server.py"
