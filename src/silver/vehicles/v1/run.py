import pandas as pd
from datetime import date

from src.config import bronze_path, silver_path, quarantine_path, silver_metrics_path

DATASET = "vehicles"
VERSION = "v1"
PARTITION_COL = "run_date"

BRONZE_VEHICLES_PATH = bronze_path(DATASET, "full")
SILVER_VEHICLES_PATH = silver_path(DATASET, VERSION)
SILVER_QUARANTINE_VEHICLES_PATH = quarantine_path(DATASET, VERSION)
SILVER_METRICS_VEHICLES_PATH = silver_metrics_path(DATASET, VERSION)

from src.utils.io_utils import find_latest_csv, _assert_columns_exist, _write_parquet_overwrite
from src.dq.silver.vehicles.v1.dq import apply_quality_rules_vehicles
from src.metrics.metrics import _write_metrics_csv

TARGET_COLUMNS = [
    "UNIQUE_ID",
    "COLLISION_ID",
    "VEHICLE_TYPE",
    "VEHICLE_MAKE",
    "VEHICLE_YEAR",
]

RENAME_MAP = {
    "UNIQUE_ID": "unique_id",
    "COLLISION_ID": "collision_id",
    "VEHICLE_TYPE": "vehicle_type",
    "VEHICLE_MAKE": "vehicle_make",
    "VEHICLE_YEAR": "vehicle_year",
}

def main():

    run_date_str = date.today().isoformat()

    bronze_file = find_latest_csv(BRONZE_VEHICLES_PATH)
    print(f"Reading Bronze file: {bronze_file}")

    df_raw = pd.read_csv(bronze_file, dtype="string", low_memory=False)

    _assert_columns_exist(df_raw, TARGET_COLUMNS)
    df = df_raw[TARGET_COLUMNS].copy()

    df = df.rename(columns=RENAME_MAP)

    for col in df.columns:
        if df[col].dtype == "string":
            df[col] = df[col].str.strip()

    df["vehicle_year"] = pd.to_numeric(df["vehicle_year"], errors="coerce").astype("Int64")
    df["unique_id"] = pd.to_numeric(df["unique_id"], errors="coerce").astype("Int64")

    dq = apply_quality_rules_vehicles(df, run_date_str=run_date_str)

    print("DQ summary:")
    print(dq.metrics_summary.to_string(index=False))

    if dq.metrics_by_reason is not None and not dq.metrics_by_reason.empty:
        print("\nDQ by reason:")
        print(dq.metrics_by_reason.to_string(index=False))

    _write_parquet_overwrite(
        SILVER_VEHICLES_PATH,
        dq.clean_df,
        partition_cols=[PARTITION_COL],
    )
    print(f"Silver CLEAN written to: {SILVER_VEHICLES_PATH}")

    quarantine_run_path = SILVER_QUARANTINE_VEHICLES_PATH / f"run_date={dq.run_date}"
    _write_parquet_overwrite(quarantine_run_path, dq.quarantine_df)
    print(f"Silver QUARANTINE written to: {quarantine_run_path}")

    metrics_run_path = SILVER_METRICS_VEHICLES_PATH / f"run_date={dq.run_date}"
    _write_metrics_csv(metrics_run_path, dq.metrics_summary, dq.metrics_by_reason)
    print(f"Metrics written to: {metrics_run_path / 'metrics.csv'}")


if __name__ == "__main__":
    main()