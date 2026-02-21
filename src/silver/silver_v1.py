import pandas as pd

from src.config import BRONZE_VEHICLES_PATH, SILVER_VEHICLES_PATH, PARTITION_COLUMN
from src.utils.io_utils import find_latest_csv, _assert_columns_exist, _normalize_time_to_hhmm


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


def main():
    # 1) Pick latest Bronze CSV
    bronze_file = find_latest_csv(BRONZE_VEHICLES_PATH)
    print(f"Reading Bronze file: {bronze_file}")

    # 2) Read raw as strings
    df_raw = pd.read_csv(bronze_file, dtype="string", low_memory=False)

    # 3) Keep only dictionary columns
    _assert_columns_exist(df_raw, TARGET_COLUMNS)
    df = df_raw[TARGET_COLUMNS].copy()

    # 4) Rename to snake_case
    df = df.rename(columns=RENAME_MAP)

    # 5) Basic cleanup: trim strings
    for col in df.columns:
        if df[col].dtype == "string":
            df[col] = df[col].str.strip()

    # 6) Parse types
    # crash_date: MM/DD/YYYY
    df["crash_date"] = pd.to_datetime(df["crash_date"], errors="coerce", format="%m/%d/%Y")

    # crash_time: normalize to HH:MM then use to build datetime
    df["crash_time"] = _normalize_time_to_hhmm(df["crash_time"])

    # vehicle_year -> nullable integer
    df["vehicle_year"] = pd.to_numeric(df["vehicle_year"], errors="coerce").astype("Int64")

    # Ids to nullable ints if they are numeric
    df["unique_id"] = pd.to_numeric(df["unique_id"], errors="coerce").astype("Int64")
    df["collision_id"] = pd.to_numeric(df["collision_id"], errors="coerce").astype("Int64")

    # 7) Create crash_datetime (Silver derived)
    # Combine date + time; invalid -> NaT
    df["crash_datetime"] = pd.to_datetime(
        df["crash_date"].dt.strftime("%Y-%m-%d") + " " + df["crash_time"].fillna(""),
        errors="coerce",
    )

    # 8) Create partition column crash_year
    df["crash_year"] = df["crash_datetime"].dt.year.astype("Int64")

    # 9) Drop rows without a valid year
    before = len(df)
    df = df.dropna(subset=["crash_year"])
    after = len(df)
    print(f"Rows total: {before:,} | Rows kept (valid crash_year): {after:,} | Dropped: {before-after:,}")

    # 10) Write partitioned Parquet
    SILVER_VEHICLES_PATH.mkdir(parents=True, exist_ok=True)
    df.to_parquet(
        SILVER_VEHICLES_PATH,
        engine="pyarrow",
        index=False,
        partition_cols=[PARTITION_COLUMN],
    )

    print(f"Silver written to: {SILVER_VEHICLES_PATH}")


if __name__ == "__main__":
    main()