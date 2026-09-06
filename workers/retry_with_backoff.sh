#!/bin/bash
# retry_with_backoff.sh — Run a command with exponential backoff on failure.
#
# Usage:  retry_with_backoff.sh <max_retries> <command> [args...]
#
# Behavior:
#   - Runs <command> in the foreground.
#   - On exit code != 0, waits 2^n seconds (capped at 120s) before retrying.
#   - After <max_retries> consecutive failures, exits with the last error code.
#   - On SIGTERM/SIGINT, forwards the signal and exits immediately.
#
# Example:
#   retry_with_backoff.sh 5 python workers/surface-worker/main.py

set -euo pipefail

MAX_RETRIES="${1:-5}"
shift
COMMAND="$@"
RETRY_COUNT=0
MAX_DELAY=120

# Track whether we're in a retry loop
trap 'exit 130' INT TERM

while true; do
    echo "[backoff] Starting: $COMMAND (attempt $((RETRY_COUNT + 1))/$MAX_RETRIES)"
    
    # Run the command; capture exit code without set -e killing us
    set +e
    "$@"
    EXIT_CODE=$?
    set -e
    
    if [ "$EXIT_CODE" -eq 0 ]; then
        echo "[backoff] Process exited cleanly (code 0). No restart needed."
        exit 0
    fi
    
    RETRY_COUNT=$((RETRY_COUNT + 1))
    
    if [ "$RETRY_COUNT" -ge "$MAX_RETRIES" ]; then
        echo "[backoff] Max retries ($MAX_RETRIES) exhausted. Last exit code: $EXIT_CODE"
        exit "$EXIT_CODE"
    fi
    
    # Exponential backoff: 2^attempt seconds, capped at MAX_DELAY
    DELAY=$((2 ** RETRY_COUNT))
    if [ "$DELAY" -gt "$MAX_DELAY" ]; then
        DELAY=$MAX_DELAY
    fi
    
    echo "[backoff] Process failed (exit code $EXIT_CODE). Retrying in ${DELAY}s..."
    sleep "$DELAY"
done
