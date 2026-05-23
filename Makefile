# ShopFlow Analytics — Makefile
# Usage: make <target>
# Run `make help` to see all commands.

.PHONY: help up down build generate extract process quality full-run logs clean

help:
	@echo ""
	@echo "ShopFlow Analytics — available commands"
	@echo "─────────────────────────────────────────"
	@echo "  make up          Start all Docker services"
	@echo "  make down        Stop all Docker services"
	@echo "  make build       Build the ETL Docker image"
	@echo "  make generate    Generate and seed fake e-commerce data"
	@echo "  make extract     Run the extraction layer (PostgreSQL → raw Parquet)"
	@echo "  make process     Run PySpark transformation jobs"
	@echo "  make quality     Run data quality checks"
	@echo "  make full-run    Run the full pipeline (extract → process → quality)"
	@echo "  make logs        Tail Airflow scheduler logs"
	@echo "  make clean       Remove generated data files"
	@echo ""

up:
	docker compose up -d
	@echo "Services started. Airflow UI → http://localhost:8080  (admin / admin123)"

down:
	docker compose down

build:
	docker compose build etl-worker

generate:
	docker compose exec etl-worker python scripts/generate_data.py

extract:
	docker compose exec etl-worker python -m ingestion.extractor

process:
	docker compose exec etl-worker python -m processing.spark_jobs

quality:
	docker compose exec etl-worker python -m data_quality.checks

snowflake-load:
	docker compose exec etl-worker python -m storage.snowflake_loader

full-run: extract process quality
	@echo "Full pipeline run complete."

logs:
	docker compose logs -f airflow-scheduler

psql:
	docker compose exec postgres-source psql -U shopflow -d shopflow_db

clean:
	rm -rf data/raw data/processed data/marts
	@echo "Data directories cleaned."
