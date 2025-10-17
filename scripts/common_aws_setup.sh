#!/bin/bash
#!/bin/bash
# Common AWS setup for all MCP servers
# Source this from run scripts to handle AWS authentication
#
# Improvements:
# - Serializes AWS SSO login across multiple concurrently-started servers using
#   a simple filesystem lock, so only one browser window opens.
# - Other servers wait for credentials to become valid instead of racing.

# Default AWS profile if none provided (use 3VDEV to match SSO profile)
export AWS_PROFILE="${AWS_PROFILE:-3VDEV}"
export AWS_SDK_LOAD_CONFIG=1

# Locking configuration (per-profile)
LOCK_BASE_DIR="${XDG_CACHE_HOME:-$HOME/.cache}/ds-mcp/locks"
LOCK_DIR="$LOCK_BASE_DIR/aws-sso-${AWS_PROFILE}.lock"
mkdir -p "$LOCK_BASE_DIR" 2>/dev/null || true

# Helper: wait for valid AWS credentials with timeout
wait_for_valid_credentials() {
  local timeout_secs=${1:-180}
  local interval=2
  local waited=0
  while (( waited < timeout_secs )); do
    if aws sts get-caller-identity >/dev/null 2>&1; then
      return 0
    fi
    sleep "$interval"
    waited=$(( waited + interval ))
  done
  return 1
}

# Check if we have valid AWS credentials (from existing session or profile)
if command -v aws >/dev/null 2>&1; then
  if aws sts get-caller-identity >/dev/null 2>&1; then
    echo "✓ AWS credentials are valid" >&2
    CALLER_INFO=$(aws sts get-caller-identity 2>/dev/null || echo "{}")
    ACCOUNT=$(echo "$CALLER_INFO" | grep -o '"Account": "[^"]*"' | cut -d'"' -f4)
    if [ -n "$ACCOUNT" ]; then
      echo "  Account: $ACCOUNT" >&2
    fi
  else
    # If another process is already logging in, wait for it to finish
    if [ -d "$LOCK_DIR" ]; then
      echo "⏳ Another server is performing AWS SSO login (profile: $AWS_PROFILE). Waiting for credentials..." >&2
      # Wait up to 3 minutes for credentials to become valid
      if wait_for_valid_credentials 180; then
        echo "✓ AWS credentials became valid after waiting" >&2
        return 0
      fi
      # If we timed out, fall through and try to acquire the lock ourselves
    fi

    # Try to acquire the lock (mkdir is atomic)
    if mkdir "$LOCK_DIR" 2>/dev/null; then
      # We own the lock; ensure cleanup on exit
      trap 'rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT INT TERM
      echo "⚠ AWS credentials not found or expired" >&2
      echo "Attempting AWS SSO login for profile: $AWS_PROFILE" >&2
      if aws sso login --profile "$AWS_PROFILE" 2>&1; then
        echo "✓ AWS SSO login successful" >&2
      else
        echo "" >&2
        echo "ERROR: AWS SSO login failed for profile: $AWS_PROFILE" >&2
        echo "" >&2
        echo "Please run in your terminal:" >&2
        echo "  aws sso login --profile $AWS_PROFILE" >&2
        echo "" >&2
        echo "Or if using a different profile name, update .env.sh with:" >&2
        echo "  export AWS_PROFILE=\"your-profile-name\"" >&2
        exit 1
      fi

      # After login, double-check credentials are usable (allow a short settle time)
      if ! wait_for_valid_credentials 60; then
        echo "ERROR: AWS credentials still not valid after SSO login" >&2
        exit 1
      fi
    else
      # Could not acquire lock; another process likely started login slightly earlier. Wait.
      echo "⏳ Waiting for parallel AWS SSO login to complete (profile: $AWS_PROFILE)..." >&2
      if ! wait_for_valid_credentials 240; then
        echo "ERROR: Timed out waiting for AWS credentials to become valid. Please run 'aws sso login --profile $AWS_PROFILE' manually and retry." >&2
        exit 1
      fi
      echo "✓ AWS credentials are valid after waiting" >&2
    fi
  fi
else
  echo "Warning: aws CLI not found; assuming credentials are set via environment" >&2
fi
