import shutil
import pandas as pd
from pathlib import Path
from datetime import date

from src.config import (
    BRONZE_VEHICLES_PATH,
    SILVER_VEHICLES_PATH,
    PARTITION_COLUMN,
    SILVER_QUARANTINE_VEHICLES_PATH,
    METRICS_VEHICLES_PATH,
)
from src.utils.io_utils import find_latest_csv, _assert_columns_exist, _normalize_time_to_hhmm, _rmtree_force, _write_parquet_overwrite
from src.utils.dq import apply_quality_rules_vehicles


# Data Dictionary columns (Vehicles) - required in Silver v1
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

# Standardize to snake_case for Silver
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


def _write_metrics_csv(metrics_path, metrics_summary: pd.DataFrame, metrics_by_reason: pd.DataFrame) -> None:
    """
    Writes a single metrics.csv with both summary metrics and reason breakdown.
    """
    if metrics_path.exists():
        shutil.rmtree(metrics_path)
    metrics_path.mkdir(parents=True, exist_ok=True)

    # Make a single table: [run_date, metric, value, reason, count]
    summary_csv = metrics_summary.copy()
    if "reason" not in summary_csv.columns:
        summary_csv["reason"] = pd.NA
    if "count" not in summary_csv.columns:
        summary_csv["count"] = pd.NA

    # Normalize reasons table to same shape
    if metrics_by_reason is None or metrics_by_reason.empty:
        reasons_csv = pd.DataFrame(columns=["run_date", "metric", "value", "reason", "count"])
    else:
        reasons_csv = metrics_by_reason.copy()
        # expecting columns: run_date, reason, count
        reasons_csv["metric"] = "dq_reason_count"
        reasons_csv["value"] = reasons_csv["count"]
        reasons_csv = reasons_csv[["run_date", "metric", "value", "reason", "count"]]

    # Ensure summary has same columns order
    if "metric" in summary_csv.columns and "value" in summary_csv.columns:
        summary_csv = summary_csv[["run_date", "metric", "value", "reason", "count"]]
    else:
        # fallback if your dq.py uses different names
        raise ValueError("metrics_summary must contain columns: run_date, metric, value")

    report = pd.concat([summary_csv, reasons_csv], ignore_index=True)
    report.to_csv(metrics_path / "metrics.csv", index=False)


def main():
    # Run date for this pipeline execution (used for quarantine + metrics partition)
    run_date_str = date.today().isoformat()

    # 1) Locate latest Bronze file
    bronze_file = find_latest_csv(BRONZE_VEHICLES_PATH)
    print(f"Reading Bronze file: {bronze_file}")

    # 2) Read raw as strings
    df_raw = pd.read_csv(bronze_file, dtype="string", low_memory=False)

    # 3) Keep only dictionary columns
    _assert_columns_exist(df_raw, TARGET_COLUMNS)
    df = df_raw[TARGET_COLUMNS].copy()

    # 4) Rename to snake_case
    df = df.rename(columns=RENAME_MAP)

    # 5) Trim strings
    for col in df.columns:
        if df[col].dtype == "string":
            df[col] = df[col].str.strip()

    # 6) Parse types
    df["crash_date"] = pd.to_datetime(df["crash_date"], errors="coerce", format="%m/%d/%Y")
    df["crash_time"] = _normalize_time_to_hhmm(df["crash_time"])

    df["vehicle_year"] = pd.to_numeric(df["vehicle_year"], errors="coerce").astype("Int64")
    df["unique_id"] = pd.to_numeric(df["unique_id"], errors="coerce").astype("Int64")
    df["collision_id"] = pd.to_numeric(df["collision_id"], errors="coerce").astype("Int64")

    # 7) Create crash_datetime
    df["crash_datetime"] = pd.to_datetime(
        df["crash_date"].dt.strftime("%Y-%m-%d") + " " + df["crash_time"].fillna(""),
        errors="coerce",
    )

    # 8) Create crash_year
    df["crash_year"] = df["crash_datetime"].dt.year.astype("Int64")

    # 9) Apply Data Quality classification
    dq = apply_quality_rules_vehicles(df, run_date_str=run_date_str)

    print("DQ summary:")
    print(dq.metrics_summary.to_string(index=False))

    if dq.metrics_by_reason is not None and not dq.metrics_by_reason.empty:
        print("\nDQ by reason:")
        print(dq.metrics_by_reason.to_string(index=False))

    # 10) Write CLEAN to Silver (overwrite total)
    _write_parquet_overwrite(
        SILVER_VEHICLES_PATH,
        dq.clean_df,
        partition_cols=[PARTITION_COLUMN],
    )
    print(f"Silver CLEAN written to: {SILVER_VEHICLES_PATH}")

    # 11) Write QUARANTINE (overwrite this run_date)
    quarantine_run_path = SILVER_QUARANTINE_VEHICLES_PATH / f"run_date={dq.run_date}"
    _write_parquet_overwrite(quarantine_run_path, dq.quarantine_df)
    print(f"Silver QUARANTINE written to: {quarantine_run_path}")

    # 12) Write METRICS (CSV) (overwrite this run_date)
    metrics_run_path = METRICS_VEHICLES_PATH / f"run_date={dq.run_date}"
    _write_metrics_csv(metrics_run_path, dq.metrics_summary, dq.metrics_by_reason)
    print(f"Metrics written to: {metrics_run_path / 'metrics.csv'}")


if __name__ == "__main__":
    main()