# Contributing to Duka Scraper

## Team workflow

1. **Clone** the repo and run setup:
   - Windows: `.\scripts\setup\setup.ps1`
   - Linux/macOS: `./scripts/setup/setup.sh`
2. **Create a branch** from `develop`: `git checkout -b feature/your-name`
3. **Make changes** — keep worker logic in `workers/`, API logic in `app/`
4. **Run checks** before pushing:
   ```bash
   make lint
   make test
   ```
5. **Open a PR** to `develop` — CI runs lint, tests, and Docker builds for all workers

## What runs for everyone

| Component | Purpose |
|-----------|---------|
| `docker-compose.yml` | Same Postgres, Redis, Kafka, ES, MinIO + all workers on every machine |
| `.env.example` | Shared env var template — copy to `.env`, never commit `.env` |
| `.github/workflows/ci.yml` | Auto lint/test/build on every PR |
| `.devcontainer/` | Optional: identical VS Code/Cursor dev environment in a container |
| `scripts/setup/` | One-command bootstrap so nobody configures manually |

## Development Dependencies

To ensure a consistent development experience, the setup scripts install the following tools for linting, testing, and code quality. These are defined in `requirements.txt` under the `# dev` section.

- **`pytest` & `pytest-asyncio`**: For running unit and integration tests.
- **`ruff`**: An extremely fast Python linter and code formatter.
- **`playwright`**: For browser-based tests and development (installed via `playwright install`).

## Data Contracts (Schemas)

All shared data structures, such as API schemas and Kafka message payloads, are defined as Pydantic models in `app/pipeline/schemas.py`. This file is the single source of truth for data exchange between services.

## Worker ownership (suggested)

| Worker | Folder | Kafka topic (reads → writes) |
|--------|--------|------------------------------|
| surface-worker | `workers/surface-worker/` | `crawl.requests` → `crawl.raw` |
| deep-worker | `workers/deep-worker/` | `crawl.requests` → `crawl.raw` |
| dark-worker | `workers/dark-worker/` | `crawl.requests` → `crawl.raw` |
| rss-worker | `workers/rss-worker/` | `rss.poll` → `crawl.requests` |
| parser-worker | `workers/parser-worker/` | `crawl.raw` → `crawl.parsed` |
| exporter-worker | `workers/exporter-worker/` | `crawl.parsed` → `crawl.export` |

## Dark worker safety

- **Off by default** (`DARK_ENABLED=false`)
- Requires explicit onion domain allowlist in `DARK_ALLOWED_DOMAINS`
- Start with: `docker compose --profile dark up -d tor dark-worker`
- Never mix dark crawl results with surface/deep without tagging `network: tor`

## Branch strategy

- `main` — production-ready
- `develop` — integration branch for the team
- `feature/*` — your work branches
