# Contributing to Duka Scraper

## Development Workflow

This guide provides the exact steps for contributing code. We follow a standard feature-branch workflow with Pull Requests.

### 1. Setup Your Environment

If this is your first time, clone the repository and run the one-time setup script for your operating system. This will create your `.env` file, install dependencies, and start all the necessary background services in Docker.

- **Windows (PowerShell):** `.\scripts\setup\setup.ps1`
- **Linux/macOS:** `./scripts\setup\setup.sh`

### 2. Create a Feature Branch

All new work should be done in a feature branch created from the `develop` branch.

```bash
# Make sure you have the latest version of the develop branch
git checkout develop
git pull origin develop

# Create your new branch (e.g., feature/add-user-auth)
git checkout -b feature/your-descriptive-name
```

### 3. Write Code

Run the API locally with hot-reloading for a fast development loop. The background services (Kafka, Postgres, etc.) are already running from the setup script.

```bash
# For Linux/macOS
make dev

# For Windows (since 'make' is not standard)
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Now you can make your changes to the code in the `app/` or `workers/` directories.

### 4. Test and Lint Your Changes

Before you commit, run the same quality checks that our CI pipeline runs. This prevents broken builds.

```bash
# Run the linter
make lint
# or on Windows: ruff check app/ workers/ tests/

# Run the test suite
make test
# or on Windows: pytest tests/
```

### 5. Open a Pull Request

Once your changes are complete and tested, commit them and push your branch to the remote repository.

```bash
git add .
git commit -m "feat: A clear, concise commit message"
git push origin feature/your-descriptive-name
```

Finally, go to the project's GitHub page and open a Pull Request (PR) from your branch into the `develop` branch. Write a clear description of what you changed and why, and request a review from your teammates.

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
| deep-worker    | `workers/deep-worker/`    | `crawl.requests` → `crawl.raw` |
| dark-worker    | `workers/dark-worker/`    | `crawl.requests` → `crawl.raw` |
| parser-worker  | `workers/parser-worker/`  | `crawl.raw` → `None` (for now) |

## Dark worker safety

- **Off by default** (`DARK_ENABLED=false`)
- Requires explicit onion domain allowlist in `DARK_ALLOWED_DOMAINS`
- Start with: `docker compose --profile dark up -d tor dark-worker`
- Never mix dark crawl results with surface/deep without tagging `network: tor`

## Branch strategy

- `main` — production-ready
- `develop` — integration branch for the team
- `feature/*` — your work branches
