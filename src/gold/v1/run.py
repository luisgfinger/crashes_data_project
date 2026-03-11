from __future__ import annotations

import pandas as pd

from src.config import gold_dim_path, gold_fact_path
from src.utils.io_utils import _write_parquet_overwrite, silver_partition_path, write_partition_overwrite
from src.gold.v1.dims.dim_vehicle import build_dim_vehicle
from src.gold.v1.facts.fact_crash import build_fact_crash

PARTITION_COL = "run_date"
VEHICLES_SILVER_DATASET = "vehicles"
CRASHES_SILVER_DATASET = "crashes"


def run(run_date_str: str, variant: str = "full", dry_run: bool = False) -> None:

    vehicles_file = silver_partition_path(VEHICLES_SILVER_DATASET, variant, run_date_str)                                                                   
    crashes_file = silver_partition_path(CRASHES_SILVER_DATASET, variant, run_date_str)

    vehicles = pd.read_parquet(vehicles_file, engine="pyarrow")
    crashes = pd.read_parquet(crashes_file, engine="pyarrow")

    print(f"Silver vehicles rows read: {len(vehicles)}")
    print(f"Silver crashes rows read:  {len(crashes)}")

    dim_vehicle = build_dim_vehicle(vehicles)
    print(f"dim_vehicle rows: {len(dim_vehicle)}")
    print(dim_vehicle.head(10).to_string(index=False))

    fact_crash = build_fact_crash(crashes, vehicles, run_date_str=run_date_str)
    print(f"fact_crash rows: {len(fact_crash)}")
    print(fact_crash.head(10).to_string(index=False))

    if dry_run:
        print("[DRY-RUN] Skipping writes.")
        return

    dim_out = gold_dim_path("dim_vehicle")
    _write_parquet_overwrite(dim_out, dim_vehicle)
    print(f"dim_vehicle written to: {dim_out}")

    target = gold_fact_path("fact_crash") / f"run_date={run_date_str}"
    write_partition_overwrite(target, fact_crash)  
    print(f"fact_crash written to: {target}")
                                                                                           
