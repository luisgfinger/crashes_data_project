import datetime
from pathlib import Path
from typing import Optional

# =====================
# Base Paths
# =====================
BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
SILVER_QUARANTINE_DIR = DATA_DIR / "silver_quarantine"
GOLD_DIR = DATA_DIR / "gold"

METRICS_DIR = DATA_DIR / "metrics"
LOGS_DIR = BASE_DIR / "logs"


# =====================
# Bronze
# =====================

def bronze_path(dataset: str, variant: str = "full") -> Path:
    return BRONZE_DIR / dataset / variant

def bronze_output_path(dataset: str, variant: str, run_date: Optional[str] = None) -> Path:
    if run_date is None:
        run_date = datetime.now().strftime("%Y%m%d")
    return Path(f"{BRONZE_DIR}/{dataset}/{variant}/{dataset}_raw_{run_date}.json")


# =====================
# Silver
# =====================

def silver_path(dataset: str, variant: str = "full") -> Path:
    return SILVER_DIR / dataset / variant


def silver_quarantine_path(dataset: str, variant: str = "full") -> Path:
    return SILVER_QUARANTINE_DIR / dataset / variant


def silver_metrics_path(dataset: str, variant: str = "full") -> Path:
    return METRICS_DIR / "silver" / dataset / variant


# =====================
# Gold
# =====================

def gold_dim_path(table: str) -> Path:
    return GOLD_DIR / "dim" / table


def gold_fact_path(table: str) -> Path:
    return GOLD_DIR / "fact" / table


def gold_metrics_path(dataset: str, variant: str = "full") -> Path:
    return METRICS_DIR / "gold" / dataset / variant