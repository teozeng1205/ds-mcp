ds-mcp

- What: Minimal MCP servers for Provider Combined Audit and Market Anomalies.

- Quick Start (repo root)
  - python -m venv .venv && source .venv/bin/activate
  - pip install -U openai-agents
  - pip install -e ds-threevictors -r ds-mcp/requirements.txt -e ds-mcp
  - Create `env.sh` at repo root with `AWS_PROFILE`, `AWS_DEFAULT_REGION`, `OPENAI_API_KEY`
- Run server: `bash ds-mcp/scripts/run_mcp_server.sh [slug]` (use `--list` to see options)

- Connect from a client
  - Use any MCP client to connect to the servers started above.

- More
  - Details and table guides: `docs/`
