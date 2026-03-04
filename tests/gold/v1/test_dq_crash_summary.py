import pandas as pd

from src.gold.v1.dims.dim_vehicle import build_dim_vehicle


def test_build_dim_vehicle_normalizes_deduplicates_and_drops_blank_pairs():
    df = pd.DataFrame(
        {
            "vehicle_make": [" Toyota ", "TOYOTA", "Ford", None, "Honda"],
            "vehicle_type": [" Sedan ", "SEDAN", "SUV", "Truck", None],
        }
    )

    out = build_dim_vehicle(df)

    assert list(out.columns) == ["vehicle_dim_id", "vehicle_make", "vehicle_type"]
    assert len(out) == 2
    assert out[["vehicle_make", "vehicle_type"]].to_dict("records") == [
        {"vehicle_make": "FORD", "vehicle_type": "SUV"},
        {"vehicle_make": "TOYOTA", "vehicle_type": "SEDAN"},
    ]
    assert out["vehicle_dim_id"].notna().all()


def test_build_dim_vehicle_returns_stable_ids_for_same_normalized_pair():
    df = pd.DataFrame(
        {
            "vehicle_make": [" Toyota ", "TOYOTA"],
            "vehicle_type": [" Sedan ", "SEDAN"],
        }
    )

    out = build_dim_vehicle(df)

    assert len(out) == 1
    assert str(out["vehicle_dim_id"].dtype) == "Int64"
