FROM python:3.12-slim

WORKDIR /app

# Install OS-level dependencies that might be needed for some Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install worker-specific Python dependencies
COPY workers/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared application code (like schemas) and all worker code
COPY app/ ./app/
COPY workers/ ./workers/

# This build argument is passed from docker-compose.yml to specify which worker to run
ARG WORKER_MODULE

CMD ["python", "-m", "workers.${WORKER_MODULE}.main"]