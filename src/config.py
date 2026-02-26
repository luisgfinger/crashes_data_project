from pathlib import Path

# =====================
# Base Paths
# =====================

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
SILVER_QUARANTINE_DIR = DATA_DIR / "silver_quarantine"
METRICS_DIR = DATA_DIR / "metrics"

# =====================
# Dataset Configuration
# =====================

DATASET_NAME = "vehicles"

BRONZE_VEHICLES_PATH = BRONZE_DIR / DATASET_NAME / "full"

SILVER_VERSION = "v1"

SILVER_VEHICLES_PATH = SILVER_DIR / DATASET_NAME / SILVER_VERSION

SILVER_QUARANTINE_VEHICLES_PATH = SILVER_QUARANTINE_DIR / DATASET_NAME / SILVER_VERSION

SILVER_METRICS_VEHICLES_PATH = METRICS_DIR / "silver" / DATASET_NAME / SILVER_VERSION


# =====================
# Processing Config
# =====================

PARTITION_COLUMN = "run_date"