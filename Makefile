# ============================================================================
#  E-Commerce Data Platform — common commands
#  Run `make help` to list targets.
# ============================================================================
.DEFAULT_GOAL := help
SHELL := /bin/bash
PY ?= python
export PYTHONPATH := $(CURDIR)

# ---- data generation -------------------------------------------------------
.PHONY: generate generate-sample
generate: ## Generate the full synthetic dataset (~200k orders) into data/raw
	$(PY) -m generator.generate_data --output data/raw

generate-sample: ## Generate a small sample (~3k orders) into data/sample
	$(PY) -m generator.generate_data --orders 3000 --output data/sample --seed 42

# ---- local (no-Docker) pipeline steps -------------------------------------
.PHONY: quality spark-local warehouse-load
quality: ## Run the data-quality gate on data/raw (report + quarantine)
	$(PY) -m src.quality.run_quality --raw-dir data/raw --quarantine-dir data/quarantine

spark-local: ## Run Bronze->Silver locally (needs pyspark) reading data/raw
	spark-submit --master local[*] spark/jobs/bronze_to_silver.py \
		--input data/raw --output data/silver

warehouse-load: ## Load Silver Parquet into Postgres staging (needs pyspark + jdbc)
	spark-submit --master local[*] --packages org.postgresql:postgresql:42.7.3 \
		spark/jobs/silver_to_warehouse.py --silver data/silver

# ---- dbt -------------------------------------------------------------------
.PHONY: dbt-deps dbt-snapshot dbt-run dbt-test dbt-docs
dbt-deps: ## Install dbt packages (dbt_utils)
	cd dbt && dbt deps --profiles-dir .
dbt-snapshot: ## Capture SCD2 seller snapshot
	cd dbt && dbt snapshot --profiles-dir .
dbt-run: ## Build all dbt models
	cd dbt && dbt run --profiles-dir .
dbt-test: ## Run all dbt data tests
	cd dbt && dbt test --profiles-dir .
dbt-docs: ## Generate + serve dbt docs (lineage)
	cd dbt && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .

# ---- dashboard -------------------------------------------------------------
.PHONY: dashboard
dashboard: ## Run the Streamlit dashboard locally
	streamlit run dashboard/app.py

# ---- docker stack ----------------------------------------------------------
.PHONY: up down logs restart trigger
up: ## Build + start the full stack (postgres, minio, airflow, dashboard)
	docker compose up -d --build
down: ## Stop the stack (keep volumes)
	docker compose down
logs: ## Tail airflow scheduler logs
	docker compose logs -f airflow-scheduler
restart: down up ## Restart the stack
trigger: ## Trigger the pipeline DAG once
	docker compose exec airflow-scheduler airflow dags trigger ecommerce_batch_pipeline

# ---- tests / quality-of-code ----------------------------------------------
.PHONY: test lint
test: ## Run the pytest suite
	$(PY) -m pytest
lint: ## Static checks (ruff if installed)
	@command -v ruff >/dev/null 2>&1 && ruff check . || echo "ruff not installed; skipping"

# ---- meta ------------------------------------------------------------------
.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
