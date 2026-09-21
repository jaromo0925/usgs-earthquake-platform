.PHONY: up down logs test lint observability

up:
	docker compose up --build

down:
	docker compose down

logs:
	docker compose logs --follow api ingestion airflow

test:
	pytest

lint:
	ruff check .

observability:
	docker compose --profile observability up --build

