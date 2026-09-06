#!/bin/sh
set -eu

export DISPLAY="${DISPLAY:-:99}"
Xvfb "$DISPLAY" -screen 0 1920x1080x24 -ac +extension GLX +render -noreset >/tmp/xvfb.log 2>&1 &
xvfb_pid=$!

cleanup() {
    kill "$xvfb_pid" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "Xvfb started on ${DISPLAY} (pid=${xvfb_pid})"
exec /wait_for_kafka.sh python workers/deep-worker/main.py