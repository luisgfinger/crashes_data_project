# Crashes Data Project

This repository contains a local, CLI-driven data engineering pipeline for the NYC Motor Vehicle Collisions datasets. It ingests raw records from NYC Open Data, transforms them through Bronze, Silver, and Gold layers, writes the results to a local data lake under `data/`, and can upload those outputs to Amazon S3 after each run.

## Current Scope

- Two source datasets are implemented: `crashes` and `vehicles`.
- Bronze, Silver, and Gold pipelines live under `src/`.
- The public entry point is the Typer CLI in `cli/cli.py`.
- The Socrata client lives in `api/source_nyc/nyc_open_data.py`.
- S3 uploads are handled by `api/aws/s3/upload.py`.
- AWS Glue helper scripts exist under `api/aws/glue/`.
- Exploration notebooks live under `notebooks/`.

## Data Source

The project reads directly from NYC Open Data:

- Crashes: https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95
- Vehicles: https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Vehicles/bm4k-52h4

The API client uses the Socrata JSON endpoint with paging via `$limit` and `$offset`, plus optional `$where` and `$order` clauses.

## Architecture

```text
NYC Open Data API
  -> Bronze ingestion
  -> Silver validation and typing
  -> Gold analytical models
  -> Optional S3 upload
  -> Optional AWS Glue helpers for Gold datasets
```

## Repository Layout

```text
api/
  aws/
    glue/
    s3/
  source_nyc/
cli/
  cli.py
notebooks/
src/
  bronze/
  gold/
  metrics/
  silver/
  utils/
requirements.txt
requirements-dev.txt
README.md
```

## Local Storage Layout

Generated data is written under `data/`, which is gitignored.

### Bronze

Bronze runs download paged JSON from the source API, write a temporary JSON file, convert it to Parquet, and then delete the temporary JSON file.

Final Bronze output:

```text
data/bronze/<dataset>/<variant>/raw_YYYYMMDD.parquet
```

Examples:

```text
data/bronze/crashes/full/raw_20260310.parquet
data/bronze/vehicles/incremental/raw_20260310.parquet
```

### Silver

Clean Silver output is partitioned by `run_date`:

```text
data/silver/<dataset>/<variant>/run_date=YYYY-MM-DD/data.parquet
```

Quarantine output:

```text
data/silver_quarantine/<dataset>/<variant>/run_date=YYYY-MM-DD/data.parquet
```

Metrics output:

```text
data/metrics/silver/<dataset>/<variant>/run_date=YYYY-MM-DD/metrics.json
```

### Gold

Gold currently writes two datasets:

```text
data/gold/dim/dim_vehicle/data.parquet
data/gold/fact/fact_crash/run_date=YYYY-MM-DD/data.parquet
```

## Pipeline Behavior

### Bronze layer

Implemented pipelines:

- `src/bronze/v1/crashes/run.py`
- `src/bronze/v1/vehicles/run.py`

Current behavior:

- Pulls data from the Socrata API.
- Filters source records with `crash_date >= <start_date>T00:00:00.000`.
- Orders extraction by `crash_date`.
- Stores one Bronze Parquet file per dataset, variant, and run date.
- Supports `full`, `incremental`, and `backfill` variants through directory layout.

Default Bronze lower bound:

```text
2023-01-01
```

### Silver layer

Implemented pipelines:

- `src/silver/v1/crashes/run.py`
- `src/silver/v1/vehicles/run.py`

Current behavior:

- Reads the Bronze Parquet file for the exact `run_date` and `variant` requested in the CLI.
- Selects a fixed set of columns for each dataset.
- Trims string fields and casts numeric fields to nullable integer types where applicable.
- Applies rule-based data quality checks.
- Splits records into clean, quarantine, and discard flows.
- Writes clean data, quarantine data, and run metrics.

Crash rules currently include:

- invalid or future `crash_date`
- negative injury or fatality counts
- inconsistent injured and killed subtotal breakdowns
- discard when `collision_id` is missing

Vehicle rules currently include:

- invalid `vehicle_year` range
- missing or negative `vehicle_occupants`
- discard when `unique_id` or `collision_id` is missing

### Gold layer

Implemented pipeline:

- `src/gold/v1/run.py`

Current behavior:

- Reads Silver `vehicles` and `crashes` partitions for the same `run_date` and `variant`.
- Builds `dim_vehicle` from normalized `vehicle_make` and `vehicle_type` combinations.
- Builds `fact_crash` at crash grain, enriched with total vehicles and total occupants.
- Writes `fact_crash` partitioned by `run_date` in the directory layout, not as a persisted `run_date` column in the output table.

## CLI Usage

The main entry point is:

```bash
python -m cli.cli
```

### Install dependencies

Tracked runtime dependencies:

```bash
pip install -r requirements.txt
```

Tracked development dependencies:

```bash
pip install -r requirements-dev.txt
```

The NYC Open Data client imports `requests`, but `requirements.txt` does not currently list it. If it is missing in your environment, install it explicitly:

```bash
pip install requests
```

### Bronze commands

```bash
python -m cli.cli bronze run -d crashes
python -m cli.cli bronze run -d vehicles --variant incremental --run-date 2026-03-10
python -m cli.cli bronze run -d crashes --start-date 2024-01-01
```

Available Bronze options:

- `--dataset` / `-d`: `vehicles` or `crashes`
- `--variant`: `full`, `incremental`, or `backfill`
- `--start-date`: lower bound for `crash_date`, default `2023-01-01`
- `--run-date`: execution date in `YYYY-MM-DD`
- `--dry-run`: validate parameters and skip writes
- `--bucket`: target S3 bucket
- `--prefix-root`: S3 prefix root
- `--profile`: optional AWS profile

### Silver commands

```bash
python -m cli.cli silver run -d crashes --run-date 2026-03-10
python -m cli.cli silver run -d vehicles --variant full --run-date 2026-03-10
```

Available Silver options:

- `--dataset` / `-d`: `vehicles` or `crashes`
- `--variant`: `full`, `incremental`, or `backfill`
- `--run-date`: execution date in `YYYY-MM-DD`
- `--dry-run`: validate parameters and skip writes
- `--bucket`: target S3 bucket
- `--prefix-root`: S3 prefix root
- `--profile`: optional AWS profile

### Gold commands

```bash
python -m cli.cli gold run --run-date 2026-03-10
python -m cli.cli gold run -d data_lake --variant full --run-date 2026-03-10
```

Available Gold options:

- `--dataset` / `-d`: only `data_lake` is currently supported
- `--variant`: `full`, `incremental`, or `backfill`
- `--run-date`: execution date in `YYYY-MM-DD`
- `--dry-run`: validate parameters and skip writes
- `--bucket`: target S3 bucket
- `--prefix-root`: S3 prefix root
- `--profile`: optional AWS profile

Gold requires both Silver datasets for the same `run_date` and `variant` to exist before it can run successfully.

## S3 Upload Behavior

S3 upload is built into the main CLI commands and runs automatically unless `--dry-run` is used.

Defaults from `cli/cli.py`:

- bucket: `crashes-data-luis-007`
- prefix root: `data_lake`

Upload behavior by layer:

- Bronze uploads only `data/bronze/<dataset>`
- Silver uploads only `data/silver/<dataset>`
- Gold uploads the entire `data/gold`

Examples:

```bash
python -m cli.cli bronze run -d crashes --bucket my-bucket --prefix-root data_lake --profile default
python -m cli.cli silver run -d vehicles --bucket my-bucket --profile default
python -m cli.cli gold run --bucket my-bucket --profile default
```

## AWS Glue Helpers

The repository also contains standalone Glue helper scripts under `api/aws/glue/`.

Current files:

- `api/aws/glue/createDb.py`
- `api/aws/glue/crawler/create/dim_vehicles.py`
- `api/aws/glue/crawler/create/fact_crashes.py`
- `api/aws/glue/crawler/run/dim_vehicles.py`
- `api/aws/glue/crawler/run/fact_crashes.py`

Current characteristics:

- They are not integrated into the main Typer CLI.
- They use hard-coded defaults for region, database name, bucket paths, and crawler role.
- They currently target the Gold S3 paths for `dim_vehicle` and `fact_crash`.

## Notebooks

Exploration notebooks are kept under `notebooks/` and currently cover Bronze, Silver, and Gold exploration.

## Development Tooling

`requirements-dev.txt` currently includes:

- `pytest`
- `ruff`
- `black`
- `jupyterlab`
- `ipykernel`

The repository also contains `pytest.ini`, but there is no committed `tests/` directory at the moment.

## Notes and Limitations

- `data/`, `out/`, notebook checkpoints, and virtual environments are gitignored.
- The main supported interface is `python -m cli.cli`.
- S3 upload depends on valid AWS credentials and optional profile configuration in the local environment.
