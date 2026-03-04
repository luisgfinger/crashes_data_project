import pandas as pd

from src.config import bronze_path, silver_path, silver_quarantine_path, silver_metrics_path
from src.utils.io_utils import find_latest_json, _assert_columns_exist, _write_parquet_overwrite
from src.silver.v1.vehicles.dq import apply_quality_rules_vehicles
from src.metrics.metrics import _write_metrics_json

BRONZE_DATASET = "vehicles"
SILVER_DATASET = "vehicles" 
VERSION = "v1"
PARTITION_COL = "run_date"

TARGET_COLUMNS = [
    "unique_id",
    "collision_id",
    "vehicle_type",
    "vehicle_make",
    "vehicle_year",
    "state_registration",
    "vehicle_occupants",
    "vehicle_damage",
    "vehicle_damage_1",
    "vehicle_damage_2",
    "vehicle_damage_3",
    "pre_crash",
    "travel_direction",
    "point_of_impact",
    "contributing_factor_1",
    "contributing_factor_2",
]


NUMERIC_COLUMNS = [
    "unique_id",
    "collision_id",
    "vehicle_year",
    "vehicle_occupants",
]

def run(run_date_str: str, variant: str = "full", dry_run: bool = False) -> None:
    
    bronze_dir = bronze_path(BRONZE_DATASET) 
    silver_dir = silver_path(SILVER_DATASET, VERSION, variant)
    quarantine_dir = silver_quarantine_path(SILVER_DATASET, VERSION, variant)
    metrics_dir = silver_metrics_path(SILVER_DATASET, VERSION, variant)

    bronze_file = find_latest_json(bronze_dir)
    print(f"Reading Bronze file: {bronze_file}")

    df_raw = pd.read_json(bronze_file, dtype="string", lines=True)

    _assert_columns_exist(df_raw, TARGET_COLUMNS)
    df = df_raw[TARGET_COLUMNS].copy()

    for col in df.columns:
        if df[col].dtype == "string":
            df[col] = df[col].str.strip()

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    dq = apply_quality_rules_vehicles(df, run_date_str=run_date_str)

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
    _write_metrics_json(metrics_run_path, dq.metrics_summary, dq.metrics_by_reason)
    print(f"Metrics written to: {metrics_run_path / 'metrics.json'}")
