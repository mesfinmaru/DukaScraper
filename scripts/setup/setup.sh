#!/usr/bin/env bash
# One-command setup for new team members (Linux / macOS / Git Bash)
set -euo pipefail

echo "==> Duka Scraper — team dev setup"

if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created .env from .env.example"
fi

python -m venv .venv
echo "Installing dependencies into the virtual environment..."
.venv/bin/pip install -r requirements.txt

echo "Starting background services (Postgres, Redis, Kafka, Airflow, MinIO, workers...)"
docker compose up -d

echo ""
echo "Setup complete."
echo "To run the API locally with hot-reloading: make dev"
echo "The following services are running in Docker:"
echo "  Airflow UI:     http://localhost:8081"
echo "  Kafka UI:       http://localhost:8080"
echo "  MinIO console:  http://localhost:9001"
echo "  Kibana:         http://localhost:5601"
echo "  Grafana:        http://localhost:3000"
echo ""
echo "Dark worker:      docker compose --profile dark up -d tor dark-worker"
echo "                  (set DARK_ENABLED=true and DARK_ALLOWED_DOMAINS in .env first)"
