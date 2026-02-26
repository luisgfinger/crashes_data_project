import pandas as pd
from datetime import date

from src.config import (
    BRONZE_VEHICLES_PATH,
    SILVER_VEHICLES_PATH,
    PARTITION_COLUMN,
    SILVER_QUARANTINE_VEHICLES_PATH,
    SILVER_METRICS_VEHICLES_PATH,
)
from src.utils.io_utils import find_latest_csv, _assert_columns_exist, _normalize_time_to_hhmm, _write_parquet_overwrite
from src.utils.dq import apply_quality_rules_vehicles
from src.metrics.metrics import _write_metrics_csv

TARGET_COLUMNS = [
    "UNIQUE_ID",
    "COLLISION_ID",
    "CRASH_DATE",
    "CRASH_TIME",
    "VEHICLE_TYPE",
    "VEHICLE_MAKE",
    "VEHICLE_MODEL",
    "VEHICLE_YEAR",
    "DRIVER_SEX",
    "DRIVER_LICENSE_STATUS",
    "DRIVER_LICENSE_JURISDICTION",
    "CONTRIBUTING_FACTOR_1",
    "CONTRIBUTING_FACTOR_2",
]

RENAME_MAP = {
    "UNIQUE_ID": "unique_id",
    "COLLISION_ID": "collision_id",
    "CRASH_DATE": "crash_date",
    "CRASH_TIME": "crash_time",
    "VEHICLE_TYPE": "vehicle_type",
    "VEHICLE_MAKE": "vehicle_make",
    "VEHICLE_MODEL": "vehicle_model",
    "VEHICLE_YEAR": "vehicle_year",
    "DRIVER_SEX": "driver_sex",
    "DRIVER_LICENSE_STATUS": "driver_license_status",
    "DRIVER_LICENSE_JURISDICTION": "driver_license_jurisdiction",
    "CONTRIBUTING_FACTOR_1": "contributing_factor_1",
    "CONTRIBUTING_FACTOR_2": "contributing_factor_2",
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

    df["crash_date"] = pd.to_datetime(df["crash_date"], errors="coerce", format="%m/%d/%Y")
    df["crash_time"] = _normalize_time_to_hhmm(df["crash_time"])

    df["vehicle_year"] = pd.to_numeric(df["vehicle_year"], errors="coerce").astype("Int64")
    df["unique_id"] = pd.to_numeric(df["unique_id"], errors="coerce").astype("Int64")
    df["collision_id"] = pd.to_numeric(df["collision_id"], errors="coerce").astype("Int64")

    df["crash_datetime"] = pd.to_datetime(
        df["crash_date"].dt.strftime("%Y-%m-%d") + " " + df["crash_time"].fillna(""),
        errors="coerce",
    )

    df["crash_year"] = df["crash_datetime"].dt.year.astype("Int64")

    dq = apply_quality_rules_vehicles(df, run_date_str=run_date_str)

    print("DQ summary:")
    print(dq.metrics_summary.to_string(index=False))

    if dq.metrics_by_reason is not None and not dq.metrics_by_reason.empty:
        print("\nDQ by reason:")
        print(dq.metrics_by_reason.to_string(index=False))

    _write_parquet_overwrite(
        SILVER_VEHICLES_PATH,
        dq.clean_df,
        partition_cols=[PARTITION_COLUMN],
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