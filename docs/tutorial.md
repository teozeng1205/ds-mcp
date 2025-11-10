# Tutorial

This tutorial walks you through running the DS‑MCP servers and connecting from any MCP‑compatible client.

## Prerequisites

- Python 3.10+
- AWS CLI installed (for SSO flows if needed)
- Access to Redshift and an SSO profile (e.g., `3VDEV`)

## Setup

```
# In repo root
python -m venv .venv
source .venv/bin/activate
pip install -U openai-agents
pip install -e ds-threevictors -r ds-mcp/requirements.txt -e ds-mcp

# Create env.sh (repo root)
cat > env.sh <<'EOF'
export AWS_PROFILE=3VDEV
export AWS_DEFAULT_REGION=us-east-1
# export OPENAI_API_KEY=sk-...
echo "Environment variables loaded from env.sh" >&2
EOF
```

## Run servers

```
# One-table servers
bash ds-mcp/scripts/run_mcp_server.sh provider
bash ds-mcp/scripts/run_mcp_server.sh anomalies

# Multi-table server (pass as many identifiers as you like)
bash ds-mcp/scripts/run_mcp_server.sh provider analytics.some_other_table
```

You should see the server start, register tools, and log to stderr.

## Connect from a client

Use any MCP client (e.g., `mcp` CLI, your own app) to connect to the servers you started. For tool exploration without a client, you can call the tool modules directly from Python.

## Debugging

### Checking server status

Confirm your server starts and registers tools (logs include "Total tools registered").

### Working directory

When launching MCP servers from external hosts, the working directory may vary. Prefer absolute paths in scripts and `.env` files for reliability.

### Concurrent AWS SSO logins

When multiple servers start simultaneously, avoid multiple SSO windows:

- Pre‑authenticate once: `aws sso login --profile 3VDEV` (or your profile)
- Ensure `aws` CLI is available on PATH where servers run

### Logging

Servers log to stderr. Avoid printing to stdout in stdio mode to prevent protocol interference.

## Next steps

- Add new tables under `src/ds_mcp/tables/`
- Register them in a custom server or the multi‑table server
- Use your MCP client to exercise tools end‑to‑end
