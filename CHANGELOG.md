# Changelog

Newest first. One entry per phase or notable change.

## 2026-10-10 · Phase 0: Foundations

- Repo structure for bronze / silver / gold pipeline
- Python environment managed by uv (Python 3.12, pinned via `uv.lock`)
- dbt project with DuckDB adapter; schemas `silver` and `gold`
- CI on GitHub Actions: lint, unit tests, dbt debug and parse
- ADR-001: DuckDB + dbt
