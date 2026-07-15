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

echo "Starting background services (Postgres, Redis, Kafka, Airflow, MinIO, workers...)"
docker compose up -d

echo ""
echo "Setup complete."
echo "The following services are running in Docker:"
echo "To run the API locally with hot-reloading: make dev"
echo "  Airflow UI:     http://localhost:8081"
echo "  Kafka UI:       http://localhost:8080"
echo "  MinIO console:  http://localhost:9001"
echo "  Kibana:         http://localhost:5601"
echo "  Grafana:        http://localhost:3000"
echo ""
echo "Dark worker:      docker compose --profile dark up -d tor dark-worker"
echo "                  (set DARK_ENABLED=true and DARK_ALLOWED_DOMAINS in .env first)"
