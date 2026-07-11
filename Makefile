.PHONY: install dev run test lint docker-up docker-down docker-workers docker-dark setup

install:
	pip install -r requirements.txt
	pip install -r workers/requirements.txt

dev:
	uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

run:
	uvicorn app.main:app --host 0.0.0.0 --port 8000

test:
	pytest tests/

lint:
	ruff check app/ workers/ tests/

docker-up:
	docker compose up -d

docker-workers:
	docker compose up -d surface-worker deep-worker parser-worker

docker-dark:
	docker compose --profile dark up -d tor dark-worker

docker-down:
	docker compose down

setup:
	powershell -ExecutionPolicy Bypass -File scripts/setup/setup.ps1
