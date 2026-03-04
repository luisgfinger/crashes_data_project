from __future__ import annotations

from datetime import datetime
from pathlib import Path

def bronze_output_path(dataset: str, variant: str, run_date_str: str) -> Path:

    yyyymmdd = run_date_str.replace("-", "")
    return Path(f"data/bronze/{dataset}/{variant}/{dataset}_raw_{yyyymmdd}.csv")

def parse_start_date_to_where_iso(start_date: str) -> str:

    datetime.strptime(start_date, "%Y-%m-%d")
    return f"{start_date}T00:00:00.000"