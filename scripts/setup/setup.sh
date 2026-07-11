#!/usr/bin/env bash
# One-command setup for new team members (Linux / macOS / Git Bash)
set -euo pipefail

echo "==> Duka Scraper — team dev setup"

if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created .env from .env.example"
fi

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r workers/requirements.txt

echo "Starting shared infrastructure (Postgres, Redis, Kafka, ES, MinIO)..."
docker compose up -d postgres redis kafka elasticsearch minio

echo "Starting crawl workers..."
docker compose up -d surface-worker deep-worker

echo ""
echo "Setup complete."
echo "  API:            http://localhost:8000/health"
echo "  MinIO console:  http://localhost:9001"
echo "  Elasticsearch:  http://localhost:9200"
echo ""
echo "Run API locally:  make dev"
echo "Dark worker:      docker compose --profile dark up -d tor dark-worker"
echo "                  (set DARK_ENABLED=true and DARK_ALLOWED_DOMAINS in .env first)"
