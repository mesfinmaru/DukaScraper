FROM python:3.13-slim

WORKDIR /app
ENV DEBIAN_FRONTEND=noninteractive

# System deps required by headless Chromium (Playwright)
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

# Camoufox is a Python package plus a separately managed browser binary.
# Fetch it at image build time so challenge fallback never fails at runtime.
RUN camoufox fetch official/stable

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

CMD ["/docker-entrypoint.sh"]
