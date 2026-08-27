FROM python:3.12-slim

WORKDIR /app

# System deps required by headless Chromium (Playwright)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    ca-certificates \
    fonts-liberation \
    fonts-unifont \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
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
    wget \
    xdg-utils \
    xvfb \
    && rm -rf /var/lib/apt/lists/*

COPY workers/requirements.txt .
RUN pip install --no-cache-dir --requirement requirements.txt

# Install Patchright's patched Chromium binary at BUILD time.
# Patchright strips CDP signatures, C++ automation markers, and TLS/JA3
# fingerprints that Cloudflare/Akamai use to detect headless browsers.
# NOTE: --with-deps is a Playwright flag, NOT supported by Patchright.
RUN python -m patchright install chromium

# Verify patchright is importable after install
RUN python -c "from patchright.sync_api import sync_playwright; print('✅ patchright importable')"

COPY . .
COPY workers/wait_for_kafka.sh /wait_for_kafka.sh
RUN chmod +x /wait_for_kafka.sh

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["/wait_for_kafka.sh", "python", "workers/deep-worker/main.py"]
