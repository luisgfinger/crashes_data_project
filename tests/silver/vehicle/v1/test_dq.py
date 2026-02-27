import pandas as pd
import pytest
from datetime import date

from src.dq.silver.vehicles.v1.dq import apply_quality_rules_vehicles


def make_df(rows):
    df = pd.DataFrame(rows)
    for col in ["unique_id", "collision_id", "vehicle_year"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def test_sets_run_date_and_adds_run_date_column():
    df = make_df([{"unique_id": 1, "collision_id": 10, "vehicle_year": 2010}])

    dq = apply_quality_rules_vehicles(df, run_date_str="2026-02-27")

    assert dq.run_date == "2026-02-27"
    assert "run_date" in dq.clean_df.columns
    assert (dq.clean_df["run_date"] == "2026-02-27").all()


def test_clean_row_when_ids_present_and_vehicle_year_valid():
    df = make_df(
        [
            {"unique_id": 1, "collision_id": 10, "vehicle_year": 2010},
            {"unique_id": 2, "collision_id": 11, "vehicle_year": date.today().year + 1},
        ]
    )

    dq = apply_quality_rules_vehicles(df, run_date_str="2026-02-27")

    assert len(dq.clean_df) == 2
    assert len(dq.quarantine_df) == 0
    assert len(dq.discard_df) == 0
    assert "dq_reasons" not in dq.clean_df.columns


def test_quarantine_when_vehicle_year_out_of_range():
    current_year = date.today().year
    df = make_df(
        [
            {"unique_id": 1, "collision_id": 10, "vehicle_year": 1899}, 
            {"unique_id": 2, "collision_id": 11, "vehicle_year": current_year + 2},
        ]
    )

    dq = apply_quality_rules_vehicles(df, run_date_str="2026-02-27")

    assert len(dq.clean_df) == 0
    assert len(dq.discard_df) == 0
    assert len(dq.quarantine_df) == 2
    assert dq.quarantine_df["dq_reasons"].str.contains("invalid_vehicle_year_range").all()


def test_quarantine_when_vehicle_year_column_missing():
    df = make_df([{"unique_id": 1, "collision_id": 10}])

    dq = apply_quality_rules_vehicles(df, run_date_str="2026-02-27")

    assert len(dq.clean_df) == 0
    assert len(dq.discard_df) == 0
    assert len(dq.quarantine_df) == 1
    assert "No_vehicle_year" in dq.quarantine_df.iloc[0]["dq_reasons"]


def test_discard_when_missing_unique_id_or_collision_id():
    df = make_df(
        [
            {"unique_id": None, "collision_id": 10, "vehicle_year": 2010}, 
            {"unique_id": 2, "collision_id": None, "vehicle_year": 2010},
            {"unique_id": 3, "collision_id": 12, "vehicle_year": 2010},
        ]
    )

    dq = apply_quality_rules_vehicles(df, run_date_str="2026-02-27")

    assert len(dq.discard_df) == 2
    assert len(dq.clean_df) == 1
    assert len(dq.quarantine_df) == 0
    assert dq.discard_df["dq_reasons"].str.contains("discard_missing_id").all()


def test_discard_has_reason_even_if_also_invalid_year():
    df = make_df([{"unique_id": None, "collision_id": 10, "vehicle_year": 1899}])

    dq = apply_quality_rules_vehicles(df, run_date_str="2026-02-27")

    assert len(dq.discard_df) == 1
    reasons = dq.discard_df.iloc[0]["dq_reasons"]
    assert "discard_missing_id" in reasons
    assert "invalid_vehicle_year_range" in reasons


def test_metrics_summary_counts_match_outputs():
    df = make_df(
        [
            {"unique_id": 1, "collision_id": 10, "vehicle_year": 2010},  
            {"unique_id": 2, "collision_id": 11, "vehicle_year": 1899},    
            {"unique_id": None, "collision_id": 12, "vehicle_year": 2010},  
        ]
    )

    dq = apply_quality_rules_vehicles(df, run_date_str="2026-02-27")

    m = dict(zip(dq.metrics_summary["metric"], dq.metrics_summary["value"]))

    assert m["total_rows_read"] == 3
    assert m["total_clean"] == len(dq.clean_df)
    assert m["total_quarantine"] == len(dq.quarantine_df)
    assert m["total_discard"] == len(dq.discard_df)


def test_metrics_by_reason_explodes_reasons_and_counts():
    df = make_df(
        [
            {"unique_id": 1, "collision_id": 10, "vehicle_year": 1899},   
            {"unique_id": None, "collision_id": 11, "vehicle_year": 1899}, 
        ]
    )

    dq = apply_quality_rules_vehicles(df, run_date_str="2026-02-27")

    counts = dict(zip(dq.metrics_by_reason["reason"], dq.metrics_by_reason["count"]))

    assert counts["invalid_vehicle_year_range"] == 2
    assert counts["discard_missing_id"] == 1