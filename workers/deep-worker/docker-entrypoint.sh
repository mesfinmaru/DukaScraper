#!/bin/sh
set -eu

# ---------------------------------------------------------------------------
# Xvfb: headful Camoufox (Firefox) needs a real X display. A previous
# container lifecycle can leave /tmp/.X99-lock behind; Xvfb then refuses to
# start and every headful launch fails with "cannot open display: :99".
# Clear stale locks first, then start Xvfb and wait until the socket is live.
# ---------------------------------------------------------------------------
export DISPLAY="${DISPLAY:-:99}"
rm -f /tmp/.X99-lock /tmp/.X11-unix/X99 2>/dev/null || true
mkdir -p /tmp/.X11-unix 2>/dev/null || true
chmod 1777 /tmp/.X11-unix 2>/dev/null || true

Xvfb "$DISPLAY" -screen 0 1920x1080x24 -ac +extension GLX +render -noreset >/tmp/xvfb.log 2>&1 &
xvfb_pid=$!

# Wait (up to ~10s) for the X socket so Camoufox never races Xvfb startup.
_x_wait=0
while [ "$_x_wait" -lt 50 ]; do
    if [ -S "/tmp/.X11-unix/X${DISPLAY#:}" ]; then
        break
    fi
    _x_wait=$((_x_wait + 1))
    sleep 0.2
done
echo "Xvfb started on ${DISPLAY} (pid=${xvfb_pid}, waited=${_x_wait})"

# ---------------------------------------------------------------------------
# D-Bus session bus: Firefox aborts at startup when it can't reach a bus on
# some images ("Failed to connect to socket /run/user/0/bus"). Start one if
# it isn't already listening.
# ---------------------------------------------------------------------------
export DBUS_SESSION_BUS_ADDRESS="${DBUS_SESSION_BUS_ADDRESS:-unix:path=/run/user/0/bus}"
if ! dbus-daemon --version >/dev/null 2>&1; then
    echo "dbus-daemon not installed — skipping session bus (Camoufox may log warnings)"
else
    if [ ! -S /run/user/0/bus ] 2>/dev/null || ! dbus-send --session --dest=org.freedesktop.DBus --type=method_call --print-reply / org.freedesktop.DBus.ListNames >/dev/null 2>&1; then
        mkdir -p /run/user/0 2>/dev/null || true
        dbus-daemon --session --fork --address="$DBUS_SESSION_BUS_ADDRESS" 2>/dev/null \
            && echo "D-Bus session bus started at $DBUS_SESSION_BUS_ADDRESS" \
            || echo "D-Bus session bus start failed (continuing without)"
    fi
fi

# ---------------------------------------------------------------------------
# Camoufox browser self-heal: the deep worker falls back to Camoufox when
# Patchright cannot clear a Cloudflare challenge. The image bakes the browser
# in, so this is only a repair path for an image that predates that fix (or a
# container whose cache layer was lost). Probe with the package's own resolver:
# `camoufox version` prints a rich table and `camoufox fetch` exits 0 even when
# it installs nothing, so neither is safe to gate on. Bound the repair so a
# slow download cannot hold worker startup forever, and re-probe afterwards.
# ---------------------------------------------------------------------------
camoufox_ready() {
    python -c 'from camoufox.pkgman import camoufox_path; camoufox_path(download_if_missing=False)' >/dev/null 2>&1
}

if command -v camoufox >/dev/null 2>&1; then
    if camoufox_ready; then
        echo "Camoufox browser ready: $(camoufox active 2>/dev/null || echo 'unknown')"
    else
        echo "Camoufox browser missing — repairing before worker start (bounded to ${CAMOUFOX_FETCH_TIMEOUT:-600}s) …"
        if timeout "${CAMOUFOX_FETCH_TIMEOUT:-600}" camoufox fetch >/tmp/camoufox-fetch.log 2>&1; then
            if camoufox_ready; then
                echo "Camoufox browser installed: $(camoufox active 2>/dev/null || echo 'unknown')"
            else
                echo "WARNING: camoufox fetch reported success but no browser resolved — Camoufox fallback unavailable"
                tail -n 20 /tmp/camoufox-fetch.log 2>/dev/null || true
            fi
        else
            echo "WARNING: camoufox fetch failed or timed out (see /tmp/camoufox-fetch.log) — Camoufox fallback unavailable; rebuild the image to bake the browser in"
            tail -n 20 /tmp/camoufox-fetch.log 2>/dev/null || true
        fi
    fi
fi

cleanup() {
    kill "$xvfb_pid" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

exec /wait_for_kafka.sh python workers/deep-worker/main.py
