import pandas as pd

from src.config import silver_path, gold_path, gold_metrics_path
from src.utils.io_utils import _write_parquet_overwrite
from src.metrics.metrics import _write_metrics_csv
from src.gold.crash_summary.v1.dq import apply_quality_rules

SILVER_VEHICLES = ("vehicles", "v1")
SILVER_CRASHES = ("crashes", "v1")

GOLD_DATASET = "crash_summary"
VERSION = "v1"
PARTITION_COL = "run_date"


def run(run_date_str: str, variant: str, dry_run: bool = False) -> None:
    vehicles_dir = silver_path(SILVER_VEHICLES[0], SILVER_VEHICLES[1], variant)
    crashes_dir = silver_path(SILVER_CRASHES[0], SILVER_CRASHES[1], variant)
     
    dfv = pd.read_parquet(vehicles_dir, dtype_backend="pyarrow")
    dfc = pd.read_parquet(crashes_dir, dtype_backend="pyarrow")

    for df in (dfv, dfc):
        if "collision_id" in df.columns:
            df["collision_id"] = df["collision_id"].astype("int64", errors="ignore")
        if "crash_year" in dfc.columns:
            dfc["crash_year"] = dfc["crash_year"].astype("int64", errors="ignore")

    total_vehicles = (dfv.groupby("collision_id", dropna=False)
                      .size().rename("total_vehicles").reset_index())
    
    crash_base = (dfc[["collision_id", "crash_year", "contributing_factor_1", "contributing_factor_2"]]
                  .drop_duplicates(subset="collision_id").copy())
    
    f1 = crash_base["contributing_factor_1"].fillna("").astype(str).str.strip()
    f2 = crash_base["contributing_factor_2"].fillna("").astype(str).str.strip()

    crash_base["main_contributing_factor"] = (
        f1.where(f1 != "", f2)
        .replace("", "Unspecified")
    )
     
    out = crash_base.merge(total_vehicles, on="collision_id", how="left")
    out = out[["collision_id", "total_vehicles", "crash_year", "main_contributing_factor"]]

    dq = apply_quality_rules(out)
    
    metrics_summary = pd.DataFrame(
        [
            {"run_date": run_date_str, "metric": "rows_out", "value": int(len(dq))},
            {"run_date": run_date_str, "metric": "distinct_collision_id", "value": int(dq["collision_id"].nunique(dropna=True))},
            {"run_date": run_date_str, "metric": "null_total_vehicles", "value": int(dq["total_vehicles"].isna().sum())},
            {"run_date": run_date_str, "metric": "null_main_contributing_factor", "value": int(dq["main_contributing_factor"].isna().sum())},
        ]
    )
    metrics_by_reason = pd.DataFrame(columns=["run_date", "reason", "count"])

    if dry_run:
        print("[DRY-RUN] Skipping writes.")
        print(dq.head(10).to_string(index=False))
        return

    gold_dir = gold_path(GOLD_DATASET, VERSION, variant)
    _write_parquet_overwrite(gold_dir, dq, partition_cols=[PARTITION_COL])
    print(f"Gold written to: {gold_dir}")

    metrics_dir = gold_metrics_path(GOLD_DATASET, VERSION, variant) / f"run_date={run_date_str}"
    _write_metrics_csv(metrics_dir, metrics_summary, metrics_by_reason)
    print(f"Gold metrics written to: {metrics_dir / 'metrics.csv'}")
