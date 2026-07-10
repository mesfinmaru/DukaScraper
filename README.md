# Duka Scraper

Amharic-focused web scraping and content extraction platform.

## Architecture

- **app/** — FastAPI backend (crawler, scheduler, extractors, Amharic NLP, pipeline, storage)
- **ui/** — React frontend
- **workers/** — Independent worker containers (surface, browser, deep, dark, RSS, parser, exporter)
- **lake/** — Data lake layout (bronze / silver / gold via MinIO)
- **airflow/** — Workflow orchestration
- **spark/** — Batch processing jobs
- **monitoring/** — Prometheus, Grafana, Loki, alerts

## Quick Start

```bash
# Copy environment config
cp .env.example .env

# Install dependencies
pip install -r requirements.txt

# Start infrastructure
docker compose up -d

# Run API
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

See [CONTRIBUTING.md](CONTRIBUTING.md) for branch workflow and PR rules.

## Workers

| Worker | Path | Job |
|--------|------|-----|
| surface-worker | `workers/surface-worker/` | Fast HTTP for public pages |
| browser-worker | `workers/browser-worker/` | Playwright for JS pages |
| deep-worker | `workers/deep-worker/` | Auth, forms, pagination |
| dark-worker | `workers/dark-worker/` | Tor/.onion (disabled by default) |
| rss-worker | `workers/rss-worker/` | Discover URLs from feeds |
| parser-worker | `workers/parser-worker/` | Extract article from HTML |
| exporter-worker | `workers/exporter-worker/` | Save to DB / search / lake |

## Development

```bash
make install   # Install Python dependencies
make dev       # Run API with hot reload
make test      # Run tests
make lint      # Run linter
```
