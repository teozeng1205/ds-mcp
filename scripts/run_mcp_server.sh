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

# Skip interactive AWS SSO in containers; rely on mounted ~/.aws or env creds.
# To enable serialized SSO login, set DS_MCP_AWS_SETUP=1.
if [ "${DS_MCP_AWS_SETUP:-}" = "1" ]; then
  # shellcheck disable=SC1090
  source "$SCRIPT_DIR/common_aws_setup.sh"
fi

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
