import pandas as pd

from src.config import bronze_path, silver_path, silver_quarantine_path, silver_metrics_path
from src.utils.io_utils import _normalize_time_to_hhmm, find_latest_csv, _assert_columns_exist, _write_parquet_overwrite
from src.silver.crashes.v1.dq import apply_quality_rules_crashes
from src.metrics.metrics import _write_metrics_csv

BRONZE_DATASET = "crashes"
SILVER_DATASET = "crashes" 
VERSION = "v1"
PARTITION_COL = "run_date"

TARGET_COLUMNS = [
    "COLLISION_ID",
    "CRASH DATE",
    "CRASH TIME",
    "NUMBER OF PERSONS INJURED",
    "NUMBER OF PERSONS KILLED",
    "NUMBER OF PEDESTRIANS INJURED",
    "NUMBER OF PEDESTRIANS KILLED",
    "NUMBER OF CYCLIST INJURED",
    "NUMBER OF CYCLIST KILLED",
    "NUMBER OF MOTORIST INJURED",
    "NUMBER OF MOTORIST KILLED",
]

RENAME_MAP = {
    "COLLISION_ID": "collision_id",
    "CRASH DATE": "crash_date",
    "CRASH TIME": "crash_time",
    "NUMBER OF PERSONS INJURED": "number_of_persons_injured",
    "NUMBER OF PERSONS KILLED": "number_of_persons_killed",
    "NUMBER OF PEDESTRIANS INJURED": "number_of_pedestrians_injured",
    "NUMBER OF PEDESTRIANS KILLED": "number_of_pedestrians_killed",
    "NUMBER OF CYCLIST INJURED": "number_of_cyclist_injured",
    "NUMBER OF CYCLIST KILLED": "number_of_cyclist_killed",
    "NUMBER OF MOTORIST INJURED": "number_of_motorist_injured",
    "NUMBER OF MOTORIST KILLED": "number_of_motorist_killed",
}

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

    bronze_dir = bronze_path(BRONZE_DATASET)
    silver_dir = silver_path(SILVER_DATASET, VERSION, variant)
    quarantine_dir = silver_quarantine_path(SILVER_DATASET, VERSION, variant)
    metrics_dir = silver_metrics_path(SILVER_DATASET, VERSION, variant)

    bronze_file = find_latest_csv(bronze_dir)
    print(f"Reading Bronze file: {bronze_file}")

    df_raw = pd.read_csv(bronze_file, dtype="string", low_memory=False)

    _assert_columns_exist(df_raw, TARGET_COLUMNS)
    df = df_raw[TARGET_COLUMNS].copy().rename(columns=RENAME_MAP)

    for col in df.columns:
        if df[col].dtype == "string":
            df[col] = df[col].str.strip()

    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df["crash_date"] = pd.to_datetime(df["crash_date"], errors="coerce")
    df["crash_time"] = _normalize_time_to_hhmm(df["crash_time"])

    df["crash_year"] = df["crash_date"].dt.year.astype("Int64")
    df["crash_day_of_month"] = df["crash_date"].dt.day.astype("Int64")
    df["crash_day_of_week"] = df["crash_date"].dt.dayofweek.astype("Int64")

    dq = apply_quality_rules_crashes(df, run_date_str=run_date_str)

    print("DQ summary:")
    print(dq.metrics_summary.to_string(index=False))

    if dq.metrics_by_reason is not None and not dq.metrics_by_reason.empty:
        print("\nDQ by reason:")
        print(dq.metrics_by_reason.to_string(index=False))

    if dry_run:
        print("[DRY-RUN] Skipping writes.")
        return

    _write_parquet_overwrite(
        silver_dir,
        dq.clean_df,
        partition_cols=[PARTITION_COL],
    )
    print(f"Silver CLEAN written to: {silver_dir}")

    quarantine_run_path = quarantine_dir / f"run_date={dq.run_date}"
    _write_parquet_overwrite(quarantine_run_path, dq.quarantine_df)
    print(f"Silver QUARANTINE written to: {quarantine_run_path}")

    metrics_run_path = metrics_dir / f"run_date={dq.run_date}"
    _write_metrics_csv(metrics_run_path, dq.metrics_summary, dq.metrics_by_reason)
    print(f"Metrics written to: {metrics_run_path / 'metrics.csv'}")
