from pathlib import Path

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


# =====================
# Silver
# =====================

def silver_path(dataset: str, version: str, variant: str = "full") -> Path:
    return SILVER_DIR / version / dataset / variant


def silver_quarantine_path(dataset: str, version: str, variant: str = "full") -> Path:
    return SILVER_QUARANTINE_DIR / version / dataset / variant


def silver_metrics_path(dataset: str, version: str, variant: str = "full") -> Path:
    return METRICS_DIR / "silver" / version / dataset / variant


# =====================
# Gold
# =====================

def gold_dim_path(version: str, table: str) -> Path:
    return GOLD_DIR / version / "dim" / table


def gold_fact_path(version: str, table: str) -> Path:
    return GOLD_DIR / version / "fact" / table


def gold_metrics_path(dataset: str, version: str, variant: str = "full") -> Path:
    return METRICS_DIR / "gold" / dataset / version / variant