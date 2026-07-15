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
The platform is a distributed system orchestrated with Docker Compose.

- **`app/`**: The core FastAPI backend, including API endpoints, data pipeline logic, and storage clients.
- **`workers/`**: A set of independent Python services that perform specific tasks like scraping (`surface-worker`, `deep-worker`) and data parsing (`parser-worker`). They communicate via Kafka.
- **`app/airflow/dags/`**: Airflow DAGs for scheduling and orchestrating workflows, such as the `dynamic_crawl_scheduler`.
- **`ui/`**: A React-based frontend for user interaction (work in progress).
- **`monitoring/`**: Configuration for Prometheus and Grafana for system observability.
- **`scripts/`**: Helper scripts for setup and maintenance.

## Quick Start

```bash
# For a full team setup including all services, run the setup script:
# Windows: .\scripts\setup\setup.ps1
# Linux/macOS: ./scripts/setup/setup.sh
This project uses a "local API, containerized infrastructure" development model.

# After setup, run the API locally with hot-reloading
make dev
```
1.  **Start Infrastructure**: Run the setup script for your OS. This starts all background services (Postgres, Kafka, MinIO, etc.) in Docker.
1.  **Start Infrastructure**: Run the setup script for your OS. This starts all background services (Postgres, Kafka, MinIO, Airflow, etc.) in Docker.
    -   **Windows (PowerShell):** `.\scripts\setup\setup.ps1`
    -   **Linux/macOS:** `./scripts/setup/setup.sh`

API health check: http://localhost:8000/health
2.  **Run API Locally**: In a **new terminal**, activate the virtual environment and run the FastAPI server with hot-reloading.
    -   **Activate (Windows):** `.\.venv\Scripts\Activate.ps1`
    -   **Activate (Linux/macOS):** `source .venv/bin/activate`
    -   **Run:** `uvicorn app.main:app --reload --host 0.0.0.0 --port 8000`

## Project Structure
The API is now running at `http://localhost:8000`.
The API is now running at `http://localhost:8000`. You can access the other service UIs at the ports listed by the setup script.

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
### Development Workflow

## Team Setup (Collaboration)
For detailed instructions on the development workflow, branching strategy, and available `make` commands, please see **CONTRIBUTING.md**.
For detailed instructions on the development workflow, branching strategy, and available `make` commands, please see CONTRIBUTING.md.

Everyone on the team runs the **same stack** via Docker:
### Available Workers

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
