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
GOLD_QUARANTINE_DIR = DATA_DIR / "gold_quarantine"
METRICS_DIR = DATA_DIR / "metrics"
LOGS_DIR = BASE_DIR / "logs"


def bronze_path(dataset: str, variant: str = "full") -> Path:
    return BRONZE_DIR / dataset / variant

def silver_path(dataset: str, version: str, variant: str = "full") -> Path:
    return SILVER_DIR / dataset / version / variant

def silver_quarantine_path(dataset: str, version: str, variant: str = "full") -> Path:
    return SILVER_QUARANTINE_DIR / dataset / version / variant

def silver_metrics_path(dataset: str, version: str, variant: str = "full") -> Path:
    return METRICS_DIR / "silver" / dataset / version / variant

def gold_path(dataset: str, version: str, variant: str = "full") -> Path:
    return GOLD_DIR / dataset / version / variant

def gold_metrics_path(dataset: str, version: str, variant: str = "full") -> Path:
    return METRICS_DIR / "gold" / dataset / version / variant
