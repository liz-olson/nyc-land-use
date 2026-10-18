# Shortcuts for common commands. Run `make help` to list them.
# Every target runs from the repo root, so relative paths (like data/warehouse.duckdb) resolve the same way everywhere.

DBT = uv run dbt --no-use-colors
DBT_ARGS = --project-dir transform --profiles-dir transform

.PHONY: help setup test lint dbt-debug dbt-parse dbt-build check clean

help:  ## List available commands
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

setup:  ## Install Python and all dependencies into .venv
	uv sync

test:  ## Run Python unit tests
	uv run pytest -q

lint:  ## Check code style and common bugs
	uv run ruff check .
	uv run ruff format --check .

dbt-debug:  ## Confirm dbt can connect to DuckDB
	mkdir -p data
	$(DBT) debug $(DBT_ARGS)

dbt-parse:  ## Confirm the dbt project compiles
	$(DBT) parse $(DBT_ARGS)

dbt-build:  ## Run all dbt models and tests
	$(DBT) build $(DBT_ARGS)

check: lint test dbt-debug dbt-parse  ## Everything CI runs

clean:  ## Delete local build outputs (keeps raw data)
	rm -rf transform/target transform/logs logs .pytest_cache .ruff_cache

ingest-zap:  ## Fetch ZAP project data into bronze
	uv run python -m ingest.sources.zap

ingest-zap-bbl:  ## Fetch the ZAP BBL crosswalk into bronze
	uv run python -m ingest.sources.zap_bbl

ingest-pluto:  ## Fetch PLUTO (~858k tax lots) into bronze -- takes a while
	uv run python -m ingest.sources.pluto
