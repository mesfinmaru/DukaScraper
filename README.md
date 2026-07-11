# Duka Scraper

Amharic-focused web scraping and content extraction platform.

## Architecture

- **app/** — FastAPI backend (crawler, scheduler, extractors, Amharic NLP, pipeline, storage)
- **ui/** — React frontend
- **workers/** — Independent worker containers (surface, deep, dark, parser)
- **lake/** — Data lake layout (bronze / silver / gold via MinIO)
- **airflow/** — Workflow orchestration
- **spark/** — Batch processing jobs
- **monitoring/** — Prometheus, Grafana, Loki, alerts

## Quick Start

```bash
# For a full team setup including all services, run the setup script:
# Windows: .\scripts\setup\setup.ps1
# Linux/macOS: ./scripts/setup/setup.sh

# After setup, run the API locally with hot-reloading
make dev
```

API health check: http://localhost:8000/health

## Project Structure

```
Duka_Scraper/
├── app/           # Backend application
├── ui/            # React frontend
├── workers/       # Worker containers
├── data/          # Seeds, exports, temp, dictionaries
├── lake/          # Data lake (bronze/silver/gold)
├── airflow/       # Workflow orchestration
├── spark/         # Batch processing
├── monitoring/    # Observability stack
├── deployment/    # Docker, K8s, Helm, Nginx
├── scripts/       # Setup, migration, maintenance
├── tests/         # Unit, integration, performance
├── docs/          # Documentation
└── configs/       # Environment configs
```

## Team Setup (Collaboration)

Everyone on the team runs the **same stack** via Docker:

```bash
# Windows — one command
.\scripts\setup\setup.ps1

# Linux/macOS
./scripts/setup/setup.sh
```

This starts Postgres, Redis, Kafka, Elasticsearch, MinIO, and all workers.

| Command | What it does |
|---------|--------------|
| `make docker-up` | Start full stack (API + infra + workers) |
| `make docker-workers` | Start only crawl workers |
| `make docker-dark` | Enable Tor + dark-worker (opt-in) |
| `make dev` | Run API locally with hot reload |
| `make lint` / `make test` | Pre-push checks (same as CI) |
> **Note for Windows Users**: The `make` command is not available on Windows by default. You can either install it (e.g., via Chocolatey) or use the equivalent commands for PowerShell/CMD listed below.

| Command (Linux/macOS) | Equivalent Command (Windows) | What it does |
|-----------------------|-----------------------------------------------------------------|------------------------------------------|
| `make docker-up` | `docker compose up -d` | Start full stack (API + infra + workers) |
| `make docker-workers` | `docker compose up -d surface-worker deep-worker parser-worker` | Start default crawl workers |
| `make docker-dark` | `docker compose --profile dark up -d tor dark-worker` | Enable Tor + dark-worker (opt-in) |
| `make dev` | `uvicorn app.main:app --reload --host 0.0.0.0 --port 8000` | Run API locally with hot reload |
| `make lint` | `ruff check app/ workers/ tests/` | Run the linter |
| `make test` | `pytest tests/` | Run the test suite |

See [CONTRIBUTING.md](CONTRIBUTING.md) for branch workflow and PR rules.

## Workers

| Worker | Path | Job |
|--------|------|-----|
| surface-worker | `workers/surface-worker/` | Fast HTTP for public pages |
| deep-worker | `workers/deep-worker/` | Auth, forms, pagination |
| dark-worker | `workers/dark-worker/` | Tor/.onion (disabled by default) |
| parser-worker | `workers/parser-worker/` | Extracts content from raw HTML |
