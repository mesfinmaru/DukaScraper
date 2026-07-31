FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY workers/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
COPY workers/wait_for_kafka.sh /wait_for_kafka.sh
RUN chmod +x /wait_for_kafka.sh

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["/wait_for_kafka.sh", "python", "workers/surface-worker/main.py"]
