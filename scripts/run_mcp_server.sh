#!/usr/bin/env bash
set -euo pipefail

# Unified launcher for ds-mcp MCP servers.
# Usage: run_mcp_server.sh [provider|anomalies]

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
MCP_DIR="$SCRIPT_DIR/.."
REPO_ROOT="$MCP_DIR/.."

SERVER_KIND="${1:-}"
if [ -z "$SERVER_KIND" ]; then
  echo "Usage: $0 [provider|anomalies]" >&2
  exit 2
fi

# Required: repo-level env.sh
ENV_FILE="$REPO_ROOT/env.sh"
if [ ! -f "$ENV_FILE" ]; then
  echo "Error: env.sh not found at $ENV_FILE. Create it at the repo root." >&2
  exit 1
fi
# shellcheck disable=SC1090
source "$ENV_FILE"

# Required: repo-level .venv Python
PY="$REPO_ROOT/.venv/bin/python3"
if [ ! -x "$PY" ]; then
  echo "Error: repo .venv not found at $REPO_ROOT/.venv. Create it and install deps." >&2
  echo "Hint: python3 -m venv .venv && source .venv/bin/activate && pip install -e openai-agents-python -e ds-threevictors -r ds-mcp/requirements.txt -e ds-mcp" >&2
  exit 1
fi

# Ensure ds-mcp sources are importable
export PYTHONPATH="$MCP_DIR/src"

# Ensure AWS auth (serialized SSO if needed)
# shellcheck disable=SC1090
source "$SCRIPT_DIR/common_aws_setup.sh"

case "$SERVER_KIND" in
  provider)
    exec "$PY" "$MCP_DIR/src/ds_mcp/servers/provider_combined_audit_server.py"
    ;;
  anomalies)
    exec "$PY" "$MCP_DIR/src/ds_mcp/servers/market_anomalies_server.py"
    ;;
  *)
    echo "Error: unknown server kind: $SERVER_KIND (expected: provider|anomalies)" >&2
    exit 2
    ;;
esac

