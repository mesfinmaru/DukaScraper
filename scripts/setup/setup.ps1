# One-command setup for new team members (Windows PowerShell)
$ErrorActionPreference = "Stop"

Write-Host "==> Duka Scraper — team dev setup"

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env from .env.example"
}

python -m venv .venv
& .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -r workers/requirements.txt

Write-Host "Starting shared infrastructure (Postgres, Redis, Kafka, ES, MinIO)..."
docker compose up -d postgres redis kafka elasticsearch minio

Write-Host "Starting crawl workers..."
docker compose up -d surface-worker deep-worker parser-worker

Write-Host ""
Write-Host "Setup complete."
Write-Host "  API:            http://localhost:8000/health"
Write-Host "  MinIO console:  http://localhost:9001"
Write-Host "  Elasticsearch:  http://localhost:9200"
Write-Host ""
Write-Host "Run API locally:  make dev"
Write-Host "Dark worker:      docker compose --profile dark up -d tor dark-worker"
Write-Host "                  (set DARK_ENABLED=true and DARK_ALLOWED_DOMAINS in .env first)"
