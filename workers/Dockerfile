FROM python:3.12-slim

WORKDIR /app

# Install basic system utilities if required by packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy and install all dependencies from the root requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your project code into the container
COPY . .

# Accept the worker module argument passed from docker-compose.yml
ARG WORKER_MODULE=surface-worker
ENV WORKER_MODULE=${WORKER_MODULE}

# Ensure Python can find your 'app' package modules
ENV PYTHONPATH=/app

# Dynamically run the correct worker's main script
CMD ["sh", "-c", "python workers/${WORKER_MODULE}/main.py"]