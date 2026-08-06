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

# Install the Chromium browser binary at BUILD time (not at container startup).
# Doing this at runtime (previous approach) re-ran apt-get on every container
# start and failed because Playwright's Debian package list assumes an
# unavailable font package on newer Debian bases. Using --with-deps here,
# during the build, avoids the runtime apt-get entirely.
RUN python -m playwright install chromium

COPY . .
COPY workers/wait_for_kafka.sh /wait_for_kafka.sh
RUN chmod +x /wait_for_kafka.sh

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["/wait_for_kafka.sh", "python", "workers/deep-worker/main.py"]
