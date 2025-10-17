#!/bin/bash
# Common AWS setup for all MCP servers
# Source this from run scripts to handle AWS authentication

# Default AWS profile if none provided (use 3VDEV to match SSO profile)
export AWS_PROFILE="${AWS_PROFILE:-3VDEV}"
export AWS_SDK_LOAD_CONFIG=1

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
        # Try SSO login only if credentials are not valid
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
    fi
else
    echo "Warning: aws CLI not found; assuming credentials are set via environment" >&2
fi
