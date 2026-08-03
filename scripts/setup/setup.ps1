# One-command setup for Windows PowerShell
$ErrorActionPreference = "Stop"

Set-Location (Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path))

Write-Host "=================================================================="
Write-Host " Duka Scraper - Team Setup"
Write-Host "=================================================================="

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env from .env.example"
}

Write-Host ""
Write-Host "[1/4] Building images (api + all workers)..."
docker compose build

Write-Host ""
Write-Host "[2/4] Starting infrastructure (Postgres, Kafka, MinIO, Elasticsearch, ClickHouse, Redis)..."
docker compose up -d postgres redis kafka elasticsearch clickhouse minio pgadmin kafka-ui kibana prometheus grafana

Write-Host "Waiting for core databases to become healthy..."
Start-Sleep -Seconds 15

Write-Host ""
Write-Host "[3/4] Creating Kafka topics (crawl.requests, crawl.raw, crawl.parsed, ...)..."
docker compose up kafka-topics-init

Write-Host ""
Write-Host "[4/4] Starting API + surface/deep/parser/exporter workers..."
docker compose up -d api surface-worker parser-worker deep-worker exporter-worker

Write-Host ""
Write-Host "=================================================================="
Write-Host " Setup complete!"
Write-Host "=================================================================="
Write-Host ""
Write-Host "PostgreSQL databases (duka_system, duka_db) were auto-created from"
Write-Host "database/01_duka_system.sql and database/02_duka_db.sql on first boot."
Write-Host ""
Write-Host "Services:"
Write-Host "  API (Swagger docs): http://localhost:8000/docs"
Write-Host "  Kafka UI:            http://localhost:8088"
Write-Host "  MinIO console:        http://localhost:9001   (minioadmin / minioadmin)"
Write-Host "  pgAdmin:              http://localhost:5050   (admin@example.com / admin)"
Write-Host "  Kibana:               http://localhost:5601"
Write-Host "  Grafana:              http://localhost:3000"
Write-Host "  Prometheus:           http://localhost:9090"
Write-Host "  Kafka external port:  localhost:29092 (WSL/native clients)"
Write-Host ""
Write-Host "Try it: submit a crawl job"
Write-Host '  curl -X POST http://localhost:8000/api/v1/jobs/trigger -H "Content-Type: application/json" -d ''{"url":"https://example.com","user_id":"USR12345"}'''
Write-Host ""
Write-Host "WSL usage: set APP_ENV=wsl before running API/workers natively in Ubuntu WSL."
Write-Host ""
Write-Host "Dark web worker (opt-in, disabled by default):"
Write-Host "  1. Set DARK_ENABLED=true in .env"
Write-Host "  2. docker compose --profile dark up -d tor dark-worker"
Write-Host ""
Write-Host "Check status: docker compose ps"
Write-Host "View logs:    docker compose logs -f surface-worker parser-worker exporter-worker"
