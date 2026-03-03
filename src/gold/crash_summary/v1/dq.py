import pandas as pd
from datetime import date

def apply_quality_rules(df: pd.DataFrame, run_date_str: str | None = None) -> pd.DataFrame:
    run_date_str = run_date_str or date.today().isoformat()

    out = df.copy()

    out["run_date"] = run_date_str

    if not out["collision_id"].is_unique:
        raise ValueError("collision_id must be unique in crash_summary")

    if not (out["total_vehicles"].fillna(0) >= 0).all():
        raise ValueError("total_vehicles must be >= 0")
    
    out["total_vehicles"] = pd.to_numeric(out["total_vehicles"], errors="coerce").fillna(0).astype("int64")
    
    return out
