# Motor Vehicle Collisions – Data Engineering Pipeline

This project implements a modular, production-oriented data engineering pipeline using the NYC Motor Vehicle Collisions datasets.

The goal is to move beyond exploratory notebooks and design a reproducible, scalable, and layered data architecture following modern data engineering best practices.

---

# 🇺🇸 English Version (Versão em PT-BR mais abaixo)

## Project Overview

This repository implements a layered data lake architecture:

Bronze (Raw CSV)  
        ↓  
Silver (Validated, Typed, Partitioned Parquet)  
        ↓  
Gold (Aggregations & Analytics – Planned)

The pipeline is fully modular and CLI-driven, allowing multiple datasets and versions to coexist under a unified architecture.

### Key Design Principles

- Schema contracts and explicit column validation
- Dataset-level modular pipelines (vehicles, crashes)
- Layered architecture (Bronze → Silver → Gold)
- Partitioning strategy for performance and scalability
- Reproducible execution via CLI
- Variant-based output environments (full, incremental, backfill)
- AI-augmented development workflow

---

## Supported Silver Pipelines

| Dataset  | Version | Description |
|----------|----------|-------------|
| vehicles | v1 | Vehicle-level dataset (1 row = 1 vehicle) |
| crashes  | v1 | Crash-level dataset (1 row = 1 collision event) |

Each dataset has its own independent Silver pipeline module.

---

## CLI Execution

The project uses a structured CLI with enum-based validation and registry-based dispatch.

### Run Silver pipeline

python -m src.cli silver run -d vehicles -v v1

### Available options

-d, --dataset     vehicles | crashes  
-v, --version     v1  
--variant         full | incremental | backfill  
--run-date        YYYY-MM-DD  
--dry-run         simulate execution without writing output  

### Example

python -m src.cli silver run -d crashes -v v1 --variant incremental --run-date 2026-03-01

---

## Dataset Sources

Raw datasets are NOT included in this repository due to GitHub size limits.

Download from NYC Open Data:

Motor Vehicle Collisions – Vehicles  
https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Vehicles/h9gi-nx95

After downloading:

data/bronze/vehicles/full/

Example:

data/bronze/vehicles/full/vehicles_raw_YYYYMMDD.csv

The pipeline automatically detects the most recent file.

---

## Silver Layer – Processing Logic

Each Silver pipeline performs:

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

## Dataset Granularity

### Vehicles (v1)
1 row = 1 vehicle involved in a collision  
Partitioned by: run_date  

### Crashes (v1)
1 row = 1 collision event  

Additional processing:
- crash_date parsing
- crash_time normalization
- crash_year derived column

Partitioned by: crash_year  

---

## Data Quality Strategy

- Invalid records are redirected to Quarantine  
- Metrics generated per run  
- Explicit rule-based validation  
- Date and time normalization checks  
- Type coercion with safe casting  

---

## Execution Variants

Silver output supports execution variants:

silver/<dataset>/<version>/<variant>/

Examples:

silver/vehicles/v1/full/  
silver/vehicles/v1/incremental/  
silver/crashes/v1/backfill/  

This allows environment separation without changing Bronze input.

---

## UML Documentation

UML diagrams are maintained in:

docs/uml/

Includes:
- Component Diagram
- Activity Diagram (Silver execution flow)

These diagrams formalize system architecture and improve maintainability.

---

## Technologies Used

- Python 3.10+
- Pandas
- PyArrow
- Typer (CLI)
- Virtual Environment (venv)
- Git
- AI-assisted development (Copilot & OpenAI)

---

## Future Improvements

- Gold layer aggregations
- Automated testing (pytest)
- CI/CD integration
- Logging framework
- Incremental processing logic
- Data contracts enforcement layer
- Metadata tracking
- Orchestration integration (Prefect/Airflow)

---

## Learning Objectives

This project practices:

- Modular data pipeline architecture
- Data lake layering principles
- Schema contracts
- Partitioning strategies
- Reproducible execution patterns
- CLI-driven orchestration
- AI-augmented development workflows

---

# 🇧🇷 Versão em Português

## Visão Geral do Projeto

Este projeto implementa um pipeline modular e orientado a produção utilizando os datasets NYC Motor Vehicle Collisions.

A arquitetura segue o padrão em camadas:

Bronze (CSV bruto)  
↓  
Silver (Parquet validado, tipado e particionado)  
↓  
Gold (Agregações e métricas – planejado)

O pipeline é modular e executado via CLI, permitindo múltiplos datasets e versões sob uma arquitetura unificada.

---

## Princípios de Arquitetura

- Contrato de schema com validação explícita
- Pipelines modulares por dataset (vehicles, crashes)
- Arquitetura em camadas (Bronze → Silver → Gold)
- Estratégia de particionamento para performance
- Execução reproduzível via CLI
- Separação por variantes (full, incremental, backfill)
- Desenvolvimento assistido por IA

---

## Pipelines Silver Disponíveis

| Dataset  | Versão | Descrição |
|----------|--------|-----------|
| vehicles | v1 | 1 linha = 1 veículo |
| crashes  | v1 | 1 linha = 1 colisão |

---

## Execução via CLI

python -m src.cli silver run -d vehicles -v v1

Opções disponíveis:

-d / --dataset  
-v / --version  
--variant  
--run-date  
--dry-run  

---

## Granularidade

### Vehicles
1 linha = 1 veículo envolvido em colisão  
Particionado por: run_date  

### Crashes
1 linha = 1 evento de colisão  
Particionado por: crash_year  

---

## Estratégia de Qualidade de Dados

- Registros inválidos enviados para Quarantine  
- Geração de métricas por execução  
- Regras explícitas de validação  
- Normalização de datas e horários  
- Conversão segura de tipos  

---

## Objetivos de Aprendizado

- Arquitetura de Data Lake
- Contrato de schema
- Estratégias de particionamento
- Execução reproduzível
- Estrutura modular
- Desenvolvimento assistido por IA
