# Motor Vehicle Collisions – Data Engineering Pipeline

This project implements a modular, production-oriented data engineering pipeline using the **NYC Motor Vehicle Collisions datasets**.

The goal is to move beyond exploratory notebooks and design a **reproducible, scalable, and layered data architecture** following modern data engineering best practices.

---

# 🇺🇸 English Version (Versão em PT-BR abaixo)

## Project Overview

This repository implements a **layered data lake architecture**:

NYC Open Data API  
↓  
Bronze (Raw CSV)  
↓  
Silver (Validated, Typed, Partitioned Parquet)  
↓  
Gold (Dimensional & Analytical Models)

The pipeline is **fully modular and CLI-driven**, allowing multiple datasets and versions to coexist under a unified architecture.

---

# Architecture Overview

```
                ┌───────────────────────┐
                │   NYC Open Data API   │
                └─────────────┬─────────┘
                              │
                              ▼
                     API Client Layer
                        (api/)
                              │
                              ▼
                    Bronze Ingestion Layer
                        (src/bronze)
                              │
                              ▼
                    Silver Processing Layer
                        (src/silver)
                              │
                              ▼
                     Gold Analytics Layer
                        (src/gold)
```

---

# Key Design Principles

- **Layered Data Lake Architecture**
- Dataset-level modular pipelines
- **API ingestion abstraction layer**
- Schema contracts and explicit validation
- Reproducible CLI execution
- Variant-based environments (`full`, `incremental`, `backfill`)
- Partitioned datasets for scalability
- Data quality validation
- Quarantine isolation for invalid records
- Metrics generation
- AI-augmented development workflow

---

# Data Source

Datasets come from **NYC Open Data**:

Motor Vehicle Collisions – Crashes  
https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95

Motor Vehicle Collisions – Vehicles  
https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Vehicles/bm4k-52h4

The Bronze ingestion pipeline retrieves data directly through the **Socrata API**.

To reduce ingestion time and storage footprint, the pipeline **only retrieves records from 2023 onward by default**.

---

# API Layer

The project introduces a dedicated API client layer:

```
api/
  nyc_open_data.py
```

Responsibilities:

- Encapsulate Socrata API access
- Handle pagination (`$limit`, `$offset`)
- Support filtering (`$where`)
- Support ordering (`$order`)
- Return CSV chunks for streaming ingestion

This design prevents **tight coupling between pipelines and external APIs**.

Pipelines interact only with the **Bronze ingestion layer**, not directly with the API.

---

# Bronze Layer – Raw Ingestion

The Bronze layer is responsible for **extracting raw data from the source API and storing it unchanged**.

```
src/bronze/
  ingest_bronze.py
  utils.py
  v1/
    crashes/
      run.py
    vehicles/
      run.py
```

Responsibilities:

- API data extraction
- Raw CSV persistence
- Dataset filtering
- Pagination handling
- Reproducible ingestion runs

---

## Bronze Storage Layout

```
data/bronze/<dataset>/<variant>/
```

Example:

```
data/bronze/vehicles/full/vehicles_raw_20260303.csv
```

Variants allow different ingestion strategies without changing the dataset structure.

---

# Silver Layer – Data Processing

The Silver layer performs **schema enforcement and data quality validation**.

Responsibilities:

1. Bronze file discovery  
2. Schema validation  
3. Column selection & renaming  
4. snake_case normalization  
5. String trimming  
6. Safe type casting  
7. Derived column creation  
8. Data Quality validation  
9. Clean vs. Quarantine separation  
10. Partitioned Parquet writing  
11. Metrics generation  

---

# Gold Layer – Analytics

The Gold layer provides **analytical models and dimensional tables**.

Current implementations:

```
Gold/
 ├── dim_vehicle
 └── fact_crash
```

Outputs:

```
data/gold/v1/
```

---

# Supported Datasets

| Dataset | Description | Grain |
|--------|-------------|-------|
| crashes | collision-level dataset | 1 row = 1 crash |
| vehicles | vehicle-level dataset | 1 row = 1 vehicle |

---

# CLI Execution

The project uses a structured CLI built with **Typer**.

All pipelines are executed through:

```
python -m src.cli
```

---

## Bronze Execution

Run ingestion from the API:

```
python -m src.cli bronze run -d crashes
```

or

```
python -m src.cli bronze run -d vehicles
```

### Options

| Option | Description |
|------|-------------|
| `-d` | dataset |
| `-v` | pipeline version |
| `--variant` | full / incremental / backfill |
| `--start-date` | ingestion lower bound |
| `--run-date` | execution date |
| `--dry-run` | simulate execution |

Default ingestion scope:

```
start_date = 2023-01-01
```

---

## Silver Execution

```
python -m src.cli silver run -d crashes -v v1
```

Example:

```
python -m src.cli silver run -d crashes -v v1 --variant incremental
```

---

## Gold Execution

```
python -m src.cli gold run -d warehouse -v v1
```

---

# Data Quality Strategy

The pipeline implements explicit rule-based validation.

Invalid records are redirected to:

```
data/silver_quarantine/
```

Metrics are written to:

```
data/metrics/
```

Metrics include:

- row counts
- invalid record counts
- validation failures

---

# Partition Strategy

| Layer | Partition |
|------|-----------|
| Bronze | none |
| Silver Vehicles | run_date |
| Silver Crashes | crash_year |
| Gold | dataset dependent |

---

# Project Structure

```
api/
  nyc_open_data.py

src/
  bronze/
  silver/
  gold/
  metrics/
  utils/

data/
  bronze/
  silver/
  gold/
  silver_quarantine/
  metrics/

docs/
notebooks/
tests/
```

---

# Technologies Used

- Python 3.10+
- Pandas
- PyArrow
- Typer
- Pytest
- Socrata Open Data API
- Virtual Environment
- Git
- AI-assisted development

---

# Future Improvements

- Incremental ingestion strategy
- Parallel Bronze ingestion
- Data contracts enforcement
- Metadata tracking
- Pipeline orchestration (Airflow / Prefect)
- Structured logging
- CI/CD automation
- Data catalog integration

---

# Learning Objectives

This project practices:

- Data lake architecture
- API-based ingestion pipelines
- Modular pipeline design
- Schema enforcement
- Data quality frameworks
- Partition strategies
- CLI orchestration
- Reproducible data pipelines
- AI-augmented development

---

# 🇧🇷 Versão em Português

## Visão Geral

Este projeto implementa um pipeline de engenharia de dados modular utilizando os datasets **NYC Motor Vehicle Collisions**.

A arquitetura segue o padrão:

API NYC Open Data  
↓  
Bronze (dados brutos)  
↓  
Silver (dados limpos e validados)  
↓  
Gold (modelos analíticos)

---

## Camada API

A pasta `api/` contém o cliente responsável por acessar a API do NYC Open Data.

Essa camada:

- encapsula a lógica de acesso à API  
- evita acoplamento entre pipelines e serviços externos  
- implementa paginação e filtros  
- fornece dados brutos para a camada Bronze  

---

## Camada Bronze

Responsável por **extrair os dados da API e armazená-los sem transformação**.

Os dados são armazenados em CSV no formato:

```
data/bronze/<dataset>/<variant>/
```

Exemplo:

```
data/bronze/vehicles/full/vehicles_raw_20260303.csv
```

---

## Camada Silver

Realiza:

- validação de schema  
- normalização de colunas  
- validação de qualidade  
- geração de métricas  
- separação de dados inválidos  

---

## Camada Gold

Camada analítica com tabelas dimensionais e fatos.

---

## Execução via CLI

Bronze:

```
python -m src.cli bronze run -d crashes
```

Silver:

```
python -m src.cli silver run -d vehicles
```

Gold:

```
python -m src.cli gold run
```

---

## Objetivos de Aprendizado

- Arquitetura de Data Lake  
- Pipelines baseados em API  
- Validação de schema  
- Estratégias de particionamento  
- Execução reproduzível  
- Arquitetura modular  
- Engenharia de dados moderna  
