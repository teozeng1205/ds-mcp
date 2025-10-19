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

PY="python3"

# AWS credentials are expected to be provided via environment or mounted
# ~/.aws config. Interactive SSO setup has been removed for simplicity.

case "$SERVER_KIND" in
  provider)
    exec "$PY" -m ds_mcp.servers.provider_combined_audit_server
    ;;
  anomalies)
    exec "$PY" -m ds_mcp.servers.market_anomalies_server
    ;;
  *)
    echo "Error: unknown server kind: $SERVER_KIND (expected: provider|anomalies)" >&2
    exit 2
    ;;
esac
