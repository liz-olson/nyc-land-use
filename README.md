# NYC Land Use Pipeline

Tracking New York City land use applications from filing, through City Planning Commission and
City Council review (ULURP), to permits and finished buildings, and presenting the results on a
public interactive map.

**Current status:** Phase 0 complete: repo, tooling and CI set up. No data yet.

## How it works

```
Sources (NYC Open Data, Census, BOE, Legistar)
   │  ingest/      Python fetches each source on a schedule
   ▼
Bronze         raw Parquet, exactly as received, partitioned by ingest date
   │  transform/   dbt models (SQL) + tests
   ▼
Silver         cleaned, typed, deduplicated, joined, geocoded
   ▼
Gold           analysis-ready tables and map layers
   │  export/      gold tables → files the website reads
   ▼
Website        static site with an interactive map (site/)
```

## Run it locally

Requirements: [uv](https://docs.astral.sh/uv/getting-started/installation/) and `make`
(built into macOS and Linux).

```bash
git clone https://github.com/<you>/nyc-land-use.git
cd nyc-land-use
make setup        # installs Python 3.12 and all dependencies into .venv
cp .env.example .env
make check        # lint, unit tests, dbt connection, dbt compile
```

`make help` lists every command.

## Repo layout

| Folder | What lives there |
| --- | --- |
| `ingest/` | Python: one module per source, plus tests |
| `transform/` | dbt project: `staging/` and `intermediate/` (silver), `marts/` (gold) |
| `export/` | Scripts that write gold tables to Parquet/GeoJSON for the site |
| `site/` | The public website |
| `docs/adr/` | Architecture decision records: why things are built the way they are |
| `docs/sources/` | Notes on each data source: URL, cadence, quirks, licence |
| `data/` | Local data files (gitignored) |

## Decisions

See [`docs/adr/`](docs/adr/). Start with [ADR-001](docs/adr/001-duckdb-dbt.md).
