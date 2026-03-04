import pandas as pd

from src.utils.io_utils import _hash_int64

def build_dim_vehicle(vehicles: pd.DataFrame) -> pd.DataFrame:
    df = vehicles.copy()

    df["vehicle_make_norm"] = df["vehicle_make"].astype("string").fillna("").str.strip().str.upper()
    df["vehicle_type_norm"] = df["vehicle_type"].astype("string").fillna("").str.strip().str.upper()
    
    df.loc[df["vehicle_make_norm"] == "", "vehicle_make_norm"] = pd.NA
    df.loc[df["vehicle_type_norm"] == "", "vehicle_type_norm"] = pd.NA

    dim = (
        df[["vehicle_make_norm", "vehicle_type_norm"]]
        .dropna()
        .drop_duplicates()
        .copy()
    )

    dim["vehicle_dim_key"] = dim["vehicle_make_norm"] + "|" + dim["vehicle_type_norm"]
    dim["vehicle_dim_id"] = dim["vehicle_dim_key"].apply(_hash_int64).astype("Int64")

    dim = dim.rename(columns={
        "vehicle_make_norm": "vehicle_make",
        "vehicle_type_norm": "vehicle_type",
    })

    dim = dim[["vehicle_dim_id", "vehicle_make", "vehicle_type"]].sort_values(
        ["vehicle_make", "vehicle_type"]
    ).reset_index(drop=True)

    return dim