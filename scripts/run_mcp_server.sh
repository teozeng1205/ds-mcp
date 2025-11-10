#!/usr/bin/env bash
set -euo pipefail

# Unified launcher for ds-mcp MCP servers.
# Usage:
#   run_mcp_server.sh            # run all tables
#   run_mcp_server.sh provider   # run specific table by slug
#   run_mcp_server.sh --list     # list available table slugs

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
MCP_DIR="$SCRIPT_DIR/.."
REPO_ROOT="$MCP_DIR/.."

SERVER_KIND="${1:-}"

# Required: repo-level env.sh
ENV_FILE="$REPO_ROOT/env.sh"
if [ ! -f "$ENV_FILE" ]; then
  echo "Error: env.sh not found at $ENV_FILE. Create it at the repo root." >&2
  exit 1
fi
# shellcheck disable=SC1090
source "$ENV_FILE"

PY="${PYTHON:-python}"

# AWS credentials are expected to be provided via environment or mounted
# ~/.aws config. Interactive SSO setup has been removed for simplicity.

if [ -z "$SERVER_KIND" ]; then
  exec "$PY" -m ds_mcp.server
fi

if [ "$SERVER_KIND" = "--list" ] || [ "$SERVER_KIND" = "-l" ]; then
  shift
  exec "$PY" -m ds_mcp.server --list
fi

TABLE_ARGS=()
for slug in "$@"; do
  TABLE_ARGS+=("--table" "$slug")
done

exec "$PY" -m ds_mcp.server "${TABLE_ARGS[@]}"
