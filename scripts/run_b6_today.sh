#!/bin/bash

# Wrapper to run the B6-today validation script with a guaranteed environment

set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$SCRIPT_DIR/.."

# Locate and source env
ENV_FILE="$ROOT_DIR/.env.sh"
if [ ! -f "$ENV_FILE" ] && [ -f "$ROOT_DIR/../.env.sh" ]; then
  ENV_FILE="$ROOT_DIR/../.env.sh"
fi
if [ ! -f "$ENV_FILE" ]; then
  echo "Error: .env.sh not found at $ROOT_DIR/.env.sh or parent" >&2
  exit 1
fi
set -a; source "$ENV_FILE"; set +a

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

# Require a .venv python (repo or parent)
if [ -f "$ROOT_DIR/.venv/bin/python3" ]; then
  PY="$ROOT_DIR/.venv/bin/python3"
elif [ -f "$ROOT_DIR/../.venv/bin/python3" ]; then
  PY="$ROOT_DIR/../.venv/bin/python3"
else
  echo "Error: .venv not found at $ROOT_DIR/.venv or parent. Create it and install deps." >&2
  echo "Hint: python3 -m venv ../.venv && source ../.venv/bin/activate && pip install -r requirements.txt && (cd ../ds-threevictors && pip install -e .)" >&2
  exit 1
fi

export PYTHONPATH="$ROOT_DIR/src:$ROOT_DIR/..:$PYTHONPATH"

exec "$PY" "$ROOT_DIR/scripts/run_b6_today.py"
