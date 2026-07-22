.PHONY: help setup install docker-up docker-down api ui test lint clean

help:
	@echo "================================================================="
	@echo "                   DUKASCRAPER COMMANDS                          "
	@echo "================================================================="
	@echo "  make setup       -> Run initial Windows setup script"
	@echo "  make install     -> Install Python backend dependencies"
	@echo "  make docker-up   -> Start Docker services (Kafka, ES, Redis)"
	@echo "  make docker-down -> Stop all Docker services"
	@echo "  make api         -> Start the FastAPI backend server"
	@echo "  make ui          -> Start the React frontend dashboard"
	@echo "  make test        -> Run automated test suite"
	@echo "  make lint        -> Check code for errors with Ruff"
	@echo "================================================================="

setup:
	powershell -ExecutionPolicy Bypass -File scripts/setup/setup.ps1

install:
	pip install -r requirements.txt

docker-up:
	docker compose up -d

docker-down:
	docker compose down

api:
	uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

ui:
	cd ui && npm install && npm run dev

test:
	pytest tests/

lint:
	ruff check app/ workers/ tests/