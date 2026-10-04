FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY workers/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Transcription backend. Baked in because "audio" is a first-class crawl
# datatype: without it the worker receives every audio hand-off and rejects it
# with "faster-whisper is not installed", so the job completes with nothing
# stored. Set TRANSCRIBE_ENABLED=false to skip audio entirely instead.
#
# The CPU build pulls ctranslate2 (~50 MB) and the 'base' model is downloaded
# on first use into the container's HF cache, so the first transcription is
# slower than the rest. GPU images should pin the CUDA build instead.
RUN pip install --no-cache-dir faster-whisper

COPY . .
COPY workers/wait_for_kafka.sh /wait_for_kafka.sh
RUN chmod +x /wait_for_kafka.sh

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["/wait_for_kafka.sh", "python", "workers/transcribe-worker/main.py"]
