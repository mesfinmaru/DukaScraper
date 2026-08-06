#!/usr/bin/env bash
# ==============================================================================
# Duka Scraper - One-command team setup (Linux / macOS / WSL / Git Bash)
#
# Requires ONLY Docker + Docker Compose. No local Python/venv/pip install
# needed - the API and every worker (surface, deep, dark, parser, exporter)
# run inside containers, built from this repo.
# ==============================================================================
set -euo pipefail

cd "$(dirname "$0")/../.."

echo "=================================================================="
echo " Duka Scraper - Team Setup"
echo "=================================================================="

if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created .env from .env.example"
fi

echo ""
echo "[1/4] Building images (api + all workers)..."
docker compose build

echo ""
echo "[2/4] Starting infrastructure (Postgres, Kafka, MinIO, Elasticsearch, ClickHouse, Redis)..."
docker compose up -d postgres redis kafka elasticsearch clickhouse minio pgadmin kafka-ui kibana prometheus grafana

echo "Waiting for core databases to become healthy..."
sleep 15

echo ""
echo "[3/4] Creating Kafka topics (crawl.requests, crawl.raw, crawl.parsed, ...)..."
docker compose up kafka-topics-init

echo ""
echo "[4/4] Starting API + crawl/render/parse/LLM workers..."
docker compose up -d api surface-worker parser-worker deep-worker llm-worker exporter-worker

echo ""
echo "=================================================================="
echo " Setup complete!"
echo "=================================================================="
echo ""
echo "PostgreSQL databases (duka_system, duka_db) were auto-created from"
echo "database/01_duka_system.sql and database/02_duka_db.sql on first boot."
echo ""
echo "Services:"
echo "  API (Swagger docs): http://localhost:8000/docs"
echo "  Kafka UI:            http://localhost:8088"
echo "  MinIO console:        http://localhost:9001   (minioadmin / minioadmin)"
echo "  pgAdmin:              http://localhost:5050   (admin@example.com / admin)"
echo "  Kibana:               http://localhost:5601"
echo "  Grafana:               http://localhost:3000"
echo "  Prometheus:           http://localhost:9090"
echo "  Kafka external port:   localhost:29092 (WSL/native clients)"
echo ""
echo "Try it: submit a crawl job"
echo '  curl -X POST http://localhost:8000/api/v1/jobs/trigger \'
echo '    -H "Content-Type: application/json" \'
echo '    -d "{\"url\": \"https://www.ena.et/\", \"user_id\": \"USR12345\", \"language\": \"am\"}"'
echo ""
echo "Or run the full demo set from the Airflow crawl targets file:"
echo '  docker compose exec api python -c "from app.airflow.dags.dynamic_crawl_scheduler_dag import load_and_dispatch_targets; load_and_dispatch_targets(None)"'
echo ""
echo "WSL usage: export APP_ENV=wsl before running API/workers natively in Ubuntu WSL."
echo ""
echo "Dark web worker (opt-in, disabled by default):"
echo "  1. Set DARK_ENABLED=true in .env"
echo "  2. docker compose --profile dark up -d tor dark-worker"
echo ""
echo "Check status: docker compose ps"
echo "View logs:    docker compose logs -f surface-worker parser-worker llm-worker exporter-worker"
