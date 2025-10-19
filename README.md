ds-mcp

- What: Minimal MCP servers for Provider Combined Audit and Market Anomalies.

- Quick Start (repo root)
  - python -m venv .venv && source .venv/bin/activate
  - pip install -U openai-agents
  - pip install -e ds-threevictors -r ds-mcp/requirements.txt -e ds-mcp
  - Create `env.sh` at repo root with `AWS_PROFILE`, `AWS_DEFAULT_REGION`, `OPENAI_API_KEY`
  - Run server: `bash ds-mcp/scripts/run_mcp_server.sh provider` (or `anomalies`)

- Claude Desktop (absolute paths)
  - Provider: `bash /ABS/PATH/TO/agents/ds-mcp/scripts/run_mcp_server.sh provider`
  - Anomalies: `bash /ABS/PATH/TO/agents/ds-mcp/scripts/run_mcp_server.sh anomalies`
  - Tip: `aws sso login --profile 3VDEV` before launch

- More
  - Details and table guides: `docs/`
