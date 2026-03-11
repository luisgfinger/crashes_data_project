from __future__ import annotations

from pathlib import Path
import pandas as pd

from src.config import silver_path, silver_quarantine_path, silver_metrics_path
from src.utils.io_utils import (
    _normalize_time_to_hhmm,
    bronze_parquet_path,
    find_latest_parquet,      
    _assert_columns_exist,
    _write_parquet_overwrite,
    write_partition_overwrite,
)
from src.silver.v1.crashes.dq import apply_quality_rules_crashes
from src.metrics.metrics import _write_metrics_json


BRONZE_DATASET = "crashes"
SILVER_DATASET = "crashes"
PARTITION_COL = "run_date"

TARGET_COLUMNS = [
    "collision_id",
    "crash_date",
    "crash_time",
    "number_of_persons_injured",
    "number_of_persons_killed",
    "number_of_pedestrians_injured",
    "number_of_pedestrians_killed",
    "number_of_cyclist_injured",
    "number_of_cyclist_killed",
    "number_of_motorist_injured",
    "number_of_motorist_killed",
]

NUMERIC_COLUMNS = [
    "collision_id",
    "number_of_persons_injured",
    "number_of_persons_killed",
    "number_of_pedestrians_injured",
    "number_of_pedestrians_killed",
    "number_of_cyclist_injured",
    "number_of_cyclist_killed",
    "number_of_motorist_injured",
    "number_of_motorist_killed",
]


def run(run_date_str: str, variant: str = "full", dry_run: bool = False) -> None:
    silver_dir = silver_path(SILVER_DATASET, variant)
    quarantine_dir = silver_quarantine_path(SILVER_DATASET, variant)
    metrics_dir = silver_metrics_path(SILVER_DATASET, variant)

    bronze_file = bronze_parquet_path(BRONZE_DATASET, variant, run_date_str)                                                                   
    df_raw = pd.read_parquet(bronze_file, engine="pyarrow") 

    _assert_columns_exist(df_raw, TARGET_COLUMNS)
    df = df_raw[TARGET_COLUMNS].copy()

    for col in df.columns:
        if df[col].dtype == "object" or str(df[col].dtype) == "string":
            df[col] = df[col].astype("string").str.strip()

    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    crash_dt = pd.to_datetime(df["crash_date"], errors="coerce")

    df["crash_date"] = crash_dt.dt.date
    df["crash_year"] = crash_dt.dt.year.astype("Int64")
    df["crash_day_of_month"] = crash_dt.dt.day.astype("Int64")
    df["crash_day_of_week"] = crash_dt.dt.dayofweek.astype("Int64")

    dq = apply_quality_rules_crashes(df, run_date_str=run_date_str)

    print("DQ summary:")
    print(dq.metrics_summary.to_string(index=False))

    if dq.metrics_by_reason is not None and not dq.metrics_by_reason.empty:
        print("\nDQ by reason:")
        print(dq.metrics_by_reason.to_string(index=False))

    if dry_run:
        print("[DRY-RUN] Skipping writes.")
        return

    target = silver_dir / f"run_date={dq.run_date}"                                                                                            
    write_partition_overwrite(target, dq.clean_df)  
    print(f"Silver CLEAN written to: {silver_dir}")

    quarantine_run_path = quarantine_dir / f"run_date={dq.run_date}"
    _write_parquet_overwrite(quarantine_run_path, dq.quarantine_df)
    print(f"Silver QUARANTINE written to: {quarantine_run_path}")

    metrics_run_path = metrics_dir / f"run_date={dq.run_date}"
    _write_metrics_json(metrics_run_path, dq.metrics_summary, dq.metrics_by_reason)
    print(f"Metrics written to: {metrics_run_path / 'metrics.json'}")