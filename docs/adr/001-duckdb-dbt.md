# ADR-001: Use DuckDB + dbt for storage and transformation

Status: accepted · Date: 2026-10-10

## Context

The project needs a warehouse and a transformation layer that:

- cost close to nothing for a personal project,
- run identically on a laptop and in GitHub Actions,
- show modern data engineering practice (layered models, tests, documentation, lineage),
- handle tens of millions of rows at most (ZAP, PLUTO, DOB permits), not billions.

## Decision

Use **DuckDB** as the warehouse engine (a local file during development, MotherDuck once the
pipeline is automated) and **dbt-core** with the dbt-duckdb adapter for all SQL transformations,
organized into bronze (raw Parquet sources), silver (`staging/`, `intermediate/`) and gold (`marts/`).

## Alternatives considered

- **Snowflake / BigQuery / Databricks:** recognizable names, but free tiers are time-limited,
  usage-capped or awkward to automate, and the data volume doesn't need a distributed engine.
  The SQL and dbt patterns used here transfer directly if the project ever moves.
- **Postgres:** solid, but row-oriented and slower for analytical queries; needs a running server.
- **pandas scripts without dbt:** fewer tools, but no built-in tests, documentation or lineage,
  and transformations become hard to follow as sources multiply.

## Consequences

- One SQL dialect everywhere; no server to run or pay for locally.
- dbt tests and docs come built in, which supports the project's documentation goals.
- DuckDB allows one writer at a time, so scheduled runs must not overlap (fine for weekly jobs).
- Revisit if data grows beyond what a single machine handles comfortably, or if a target
  employer's stack makes a Snowflake/BigQuery port worth demonstrating.
