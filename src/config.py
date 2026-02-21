from pathlib import Path

# =====================
# Base Paths
# =====================

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"

# =====================
# Dataset Configuration
# =====================

DATASET_NAME = "vehicles"

BRONZE_VEHICLES_PATH = BRONZE_DIR / DATASET_NAME / "full"
SILVER_VERSION = "v1"

SILVER_VEHICLES_PATH = SILVER_DIR / DATASET_NAME / SILVER_VERSION

# =====================
# Processing Config
# =====================

PARTITION_COLUMN = "crash_year"