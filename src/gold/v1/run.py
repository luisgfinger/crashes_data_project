from __future__ import annotations

import pandas as pd

from src.config import silver_path, gold_dim_path, gold_fact_path
from src.utils.io_utils import _write_parquet_overwrite
from src.gold.v1.dims.dim_vehicle import build_dim_vehicle
from src.gold.v1.facts.fact_crash import build_fact_crash

PARTITION_COL = "run_date"


def run(run_date_str: str, variant: str = "full", dry_run: bool = False) -> None:

    vehicles_dir = silver_path("vehicles", variant) / f"run_date={run_date_str}"
    crashes_dir = silver_path("crashes", variant) / f"run_date={run_date_str}"

    print(f"Reading Silver vehicles: {vehicles_dir}")
    print(f"Reading Silver crashes:  {crashes_dir}")

    vehicles = pd.read_parquet(vehicles_dir)
    crashes = pd.read_parquet(crashes_dir)

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

    fact_out = gold_fact_path("fact_crash")
    _write_parquet_overwrite(fact_out, fact_crash, partition_cols=[PARTITION_COL])
    print(f"fact_crash written to: {fact_out}")