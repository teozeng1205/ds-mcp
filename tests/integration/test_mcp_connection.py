#!/usr/bin/env python3
"""
Test MCP connection by listing available tools
"""

import subprocess
import json
import sys

def test_mcp_tools():
    """Test that the MCP server can list its tools"""

    script_path = "/Users/weichengzeng/Library/CloudStorage/OneDrive-ATPCO/Desktop/agents/ds-mcp/run_with_env.sh"

    print("Testing MCP server connection...")
    print(f"Script: {script_path}")
    print("-" * 80)

    # Send a tools/list request via JSON-RPC
    request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/list"
    }

    try:
        proc = subprocess.Popen(
            [script_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        # Send the request
        stdout, stderr = proc.communicate(
            input=json.dumps(request) + "\n",
            timeout=10
        )

        print("STDERR (logs):")
        print(stderr[:500] if stderr else "(none)")
        print()

        print("STDOUT (response):")
        print(stdout[:1000] if stdout else "(none)")
        print()

        if stdout:
            # Try to parse response
            for line in stdout.split('\n'):
                if line.strip():
                    try:
                        response = json.loads(line)
                        if 'result' in response and 'tools' in response['result']:
                            tools = response['result']['tools']
                            print(f"✓ Found {len(tools)} tools:")
                            for tool in tools[:5]:  # Show first 5
                                print(f"  - {tool.get('name', 'unknown')}")
                            if len(tools) > 5:
                                print(f"  ... and {len(tools) - 5} more")
                            return True
                    except json.JSONDecodeError:
                        continue

        print("✗ Could not parse tool list from response")
        return False

    except subprocess.TimeoutExpired:
        print("✗ Timeout waiting for response")
        proc.kill()
        return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False

if __name__ == "__main__":
    success = test_mcp_tools()
    sys.exit(0 if success else 1)
