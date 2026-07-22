param (
    [string]$Command = "help"
)

switch ($Command) {
    "all" {
        Write-Host "🚀 Starting Docker infrastructure..." -ForegroundColor Green
        docker compose up -d

        Write-Host "⚡ Launching FastAPI Backend in a new window..." -ForegroundColor Yellow
        Start-Process powershell -ArgumentList "-NoExit -Command uvicorn app.main:app --reload --host 0.0.0.0 --port 8000"

        Write-Host "💻 Launching React UI Dashboard in a new window..." -ForegroundColor Cyan
        Start-Process powershell -ArgumentList "-NoExit -Command Set-Location ui; npm run dev"

        Write-Host "✅ Full stack is booting up!" -ForegroundColor Green
    }
    "setup"       { powershell -ExecutionPolicy Bypass -File scripts/setup/setup.ps1 }
    "install"     { pip install -r requirements.txt }
    "docker-up"   { docker compose up -d }
    "docker-down" { docker compose down }
    "api"         { uvicorn app.main:app --reload --host 0.0.0.0 --port 8000 }
    "ui"          { Set-Location ui; npm install; npm run dev }
    "test"        { pytest tests/ }
    "lint"        { ruff check app/ workers/ tests/ }
    default {
        Write-Host "=================================================================" -ForegroundColor Cyan
        Write-Host "                   DUKASCRAPER COMMANDS                          " -ForegroundColor Cyan
        Write-Host "=================================================================" -ForegroundColor Cyan
        Write-Host "  .\run.ps1 all         -> START EVERYTHING (Docker + API + UI)" -ForegroundColor Green
        Write-Host "  .\run.ps1 setup       -> Run initial Windows setup script"
        Write-Host "  .\run.ps1 install     -> Install Python backend dependencies"
        Write-Host "  .\run.ps1 docker-up   -> Start Docker services (Kafka, ES, Redis)"
        Write-Host "  .\run.ps1 docker-down -> Stop all Docker services"
        Write-Host "  .\run.ps1 api         -> Start the FastAPI backend server"
        Write-Host "  .\run.ps1 ui          -> Start the React frontend dashboard"
        Write-Host "  .\run.ps1 test        -> Run automated test suite"
        Write-Host "  .\run.ps1 lint        -> Check code for errors with Ruff"
        Write-Host "=================================================================" -ForegroundColor Cyan
    }
}