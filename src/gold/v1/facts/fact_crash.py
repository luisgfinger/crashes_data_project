import pandas as pd
import numpy as np

def build_fact_crash(
    crashes: pd.DataFrame,
    vehicles: pd.DataFrame,
    run_date_str: str,
) -> pd.DataFrame:
    vehicles_agg = (
        vehicles.groupby("collision_id", dropna=False)
        .agg(
            total_vehicles=("unique_id", "count"),
            total_occupants=("vehicle_occupants", "sum"),
        )
        .reset_index()
    )
    out = crashes.copy()
    out["date_key"] = out["crash_date"].dt.strftime("%Y%m%d").astype("Int64")
    out = out.merge(vehicles_agg, on="collision_id", how="left")

    out["total_vehicles"] = out["total_vehicles"].fillna(0).astype("Int64")
    out["total_occupants"] = out["total_occupants"].fillna(0).astype("Int64")
    out["run_date"] = run_date_str

    cols = [
        "collision_id",
        "date_key",
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
        "total_vehicles",
        "total_occupants",
        "run_date",
    ]
    cols = [c for c in cols if c in out.columns]
    return out[cols].copy()