FROM python:3.13-slim

WORKDIR /app
ENV DEBIAN_FRONTEND=noninteractive

# System deps required by headless Chromium (Playwright) AND headful
# Camoufox (Firefox) running under Xvfb. Firefox additionally needs the
# GTK3 "private" helper libraries (libgtk-3-0 pulls only the public ones),
# D-Bus, and libcanberra, otherwise camoufox-bin exits during startup with
# "Failed to launch the browser process".
#
# Why headful-over-Xvfb instead of native headless: native headless Firefox is
# what anti-bot engines flag, so the worker paints into a virtual framebuffer
# (Xvfb, started by docker-entrypoint.sh on DISPLAY=:99) instead of a real
# monitor. That needs a software GL stack — libgl1/libglx-mesa0 plus the Mesa
# DRI drivers — because Firefox has no usable GL context without libGL, and
# Camoufox can only spoof a WebGL vendor/renderer once one exists. The GL layer
# underneath runs on llvmpipe; the page still sees Camoufox's spoofed GPU. Note
# these packages already arrive transitively via the GTK/Firefox deps, but they
# are pinned here so the GL stack cannot silently vanish. mesa-utils ships
# glxinfo so the pipeline can be probed inside the container:
#     glxinfo -B      # renderer: llvmpipe (LLVM ...) — expected, host-side only
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    ca-certificates \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    fonts-liberation \
    fonts-unifont \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcairo2 \
    libcairo-gobject2 \
    libdbus-1-3 \
    libdbus-glib-1-2 \
    libdrm2 \
    libevent-2.1-7 \
    libfontconfig1 \
    libgdk-pixbuf-2.0-0 \
    libgbm1 \
    libgtk-3-0 \
    libgtk-3-common \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libpangoft2-1.0-0 \
    libx11-xcb1 \
    libxcb-dri3-0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libxshmfence1 \
    libxkbcommon0 \
    libxss1 \
    libxtst6 \
    dbus \
    libgl1 \
    libgl1-mesa-dri \
    libglx-mesa0 \
    mesa-utils \
    wget \
    xdg-utils \
    xvfb \
    && (apt-get install -y --no-install-recommends libgl1-mesa-glx \
        || echo "libgl1-mesa-glx is a transitional package absent from this base image; libgl1 + libglx-mesa0 supply the GL stack") \
    && rm -rf /var/lib/apt/lists/*

COPY workers/requirements.txt .
RUN pip install --no-cache-dir --requirement requirements.txt

# Camoufox is a Python package PLUS a separately managed browser binary:
# `pip install camoufox` downloads no browser, and a browser download takes
# minutes. It is baked in here so the challenge fallback never has to fetch a
# browser at runtime, where it would blow the worker's per-page budget and
# could never converge on a container that gets restarted mid-download.
#
# `camoufox fetch official/stable` does NOT work — the specifier is
# repo/version-build (or repo/channel/version-build), so the two-part form
# resolves to nothing, prints "Version 'official/stable' not found in cache",
# and still exits 0. The image then shipped without a browser and every
# fallback launch failed with
#   "official/stable is not installed. Please run `camoufox fetch` to install."
# Bare `camoufox fetch` installs the newest supported official/stable build
# (it syncs the release index itself) and records it as the active version.
# Probe with the package's own resolver because fetch exits 0 even when it
# installs nothing — a failure here must break the build, not ship.
#
# The 663 MB browser zip comes from GitHub releases, which routinely drops
# connections mid-transfer (IncompleteRead). fetch has no retry and no
# resume, so loop up to 5 attempts; Docker's layer cache keeps every other
# step warm, so only this step re-runs on retry.
RUN set -eux; \
    for i in 1 2 3 4 5; do \
        camoufox fetch && break; \
        echo "camoufox fetch attempt $i failed; retrying in 10s..."; \
        sleep 10; \
    done; \
    python -c "from camoufox.pkgman import camoufox_path; print('Camoufox browser ready at', camoufox_path(download_if_missing=False))"

# GeoIP databases, for resolving the browser's timezone/locale from the egress
# IP (DEEP_BROWSER_GEOIP). They are ~45 MB and camoufox downloads them on first
# use, which would happen inside a challenge attempt and eat its budget. Fetch
# them at build time instead, where a failure is a slow build rather than a
# failed crawl. Non-fatal on purpose: a deployment without access to the
# MaxMind mirror still works, it just falls back to the per-profile timezone.
RUN python -c "from camoufox.geolocation import download_mmdb; download_mmdb()" \
        || echo "WARNING: GeoIP database pre-fetch failed; it will download on first use (falling back to the per-profile timezone until then)"

# Patchright uses its own browser cache and executable revision. Installing
# Playwright's Chromium binary does not satisfy patchright.async_api.
RUN patchright install chromium

COPY . .
COPY workers/wait_for_kafka.sh /wait_for_kafka.sh
RUN chmod +x /wait_for_kafka.sh
COPY workers/deep-worker/docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Virtual framebuffer for headful Camoufox. docker-entrypoint.sh starts
# `Xvfb :99 -screen 0 1920x1080x24` before the worker, so the browser always has
# a display to attach to. This is also why the worker launches Camoufox with
# headless=False rather than headless="virtual": Camoufox's own virtual display
# spawns a 1x1 px framebuffer on Linux (daijro/camoufox#458), so the real window,
# compositor and GL surface are degenerate. A real 1920x1080 Xvfb keeps them
# honest, and running headful is what lets Camoufox clear the challenge.
# docker-compose exports DISPLAY=:99 for this service too, so a plain
# `docker run` of this image behaves identically.
ENV DISPLAY=:99

# Force Mesa's llvmpipe software rasterizer. There is no GPU in the container,
# so without this Firefox probes for a DRI device and can end up with no usable
# WebGL context at all — and Camoufox can only spoof a WebGL vendor/renderer if
# the browser created a context in the first place. This only affects the
# host-side GL layer; the page still reads Camoufox's spoofed GPU string.
# Pinning it keeps that layer identical on every host (verify with `glxinfo -B`).
ENV LIBGL_ALWAYS_SOFTWARE=1

# Firefox/Camoufox wants a session bus; point it at the Xvfb session.
# The entrypoint starts dbus-daemon if one isn't already running.
ENV DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/0/bus

CMD ["/docker-entrypoint.sh"]
