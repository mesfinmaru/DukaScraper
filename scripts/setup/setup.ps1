# One-command setup for new team members (Windows PowerShell)
$ErrorActionPreference = "Stop"

Write-Host "==> Duka Scraper — team dev setup"

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env from .env.example"
}

python -m venv .venv
Write-Host "Installing dependencies into the virtual environment..."
.\.venv\Scripts\pip.exe install -r requirements.txt

Write-Host "Starting background services (Postgres, Redis, Kafka, Airflow, MinIO, workers...)"
docker compose up -d

Write-Host ""
Write-Host "Setup complete."
Write-Host "To run the API locally with hot-reloading:"
Write-Host "  uvicorn app.main:app --reload --host 0.0.0.0 --port 8000"
Write-Host "(Or use 'make dev' if you have Make installed)"
Write-Host ""
Write-Host "The following services are running in Docker (use your IP to access from other devices):"
Write-Host "  Airflow UI:     http://<your-ip-or-localhost>:8081"
Write-Host "  Kafka UI:       http://<your-ip-or-localhost>:8080"
Write-Host "  MinIO console:  http://<your-ip-or-localhost>:9001"
Write-Host "  Kibana:         http://<your-ip-or-localhost>:5601"
Write-Host "  Grafana:        http://<your-ip-or-localhost>:3000"
Write-Host ""
Write-Host "Dark worker:      docker compose --profile dark up -d tor dark-worker"
Write-Host "                  (set DARK_ENABLED=true and DARK_ALLOWED_DOMAINS in .env first)"
