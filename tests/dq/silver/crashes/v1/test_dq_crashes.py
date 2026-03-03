import pandas as pd
import pytest

from src.dq.silver.crashes.v1.dq import apply_quality_rules_crashes, _append_reason

def test_append_reason_sets_when_empty():
    df = pd.DataFrame({"dq_reasons": ["", None, ""]})
    mask = pd.Series([True, True, False], index=df.index)

    _append_reason(df, mask, "invalid_date")

    assert df.loc[0, "dq_reasons"] == "invalid_date"
    assert df.loc[1, "dq_reasons"] == "invalid_date"
    assert df.loc[2, "dq_reasons"] in ("", None)


def test_append_reason_appends_with_semicolon_when_existing():
    df = pd.DataFrame({"dq_reasons": ["invalid_date", "", None]})
    mask = pd.Series([True, True, True], index=df.index)

    _append_reason(df, mask, "discard_missing_id")

    assert df.loc[0, "dq_reasons"] == "invalid_date;discard_missing_id"
    assert df.loc[1, "dq_reasons"] == "discard_missing_id"
    assert df.loc[2, "dq_reasons"] == "discard_missing_id"


def test_no_columns_crash_date_keeps_all_clean_and_no_metrics_by_reason():
    df = pd.DataFrame(
        {
            "unique_id": [1, 2],
            "collision_id": [10, 20],
            "other": ["a", "b"],
        }
    )

    res = apply_quality_rules_crashes(df, run_date_str="2026-03-03")

    assert len(res.clean_df) == 2
    assert len(res.quarantine_df) == 0
    assert len(res.discard_df) == 0

    assert "dq_reasons" not in res.clean_df.columns

    assert set(res.metrics_summary["metric"]) == {
        "total_rows_read",
        "total_clean",
        "total_quarantine",
        "total_discard",
    }
    assert res.metrics_by_reason.empty


def test_invalid_crash_date_null_or_future_goes_to_quarantine_with_reason(monkeypatch):

    import src.dq.silver.crashes.v1.dq 
    from datetime import date as real_date

    class FakeDate(real_date):
        @classmethod
        def today(cls):
            return cls(2026, 3, 3)

    monkeypatch.setattr(src.dq.silver.crashes.v1.dq, "date", FakeDate)

    df = pd.DataFrame(
        {
            "unique_id": [1, 2, 3],
            "collision_id": [10, 20, 30],
            "crash_date": [None, "2026-03-10", "2026-03-03"],
        }
    )

    res = apply_quality_rules_crashes(df, run_date_str="2026-03-03")

    assert len(res.clean_df) == 1
    assert len(res.quarantine_df) == 2
    assert len(res.discard_df) == 0

    assert set(res.quarantine_df["dq_reasons"].unique()) == {"invalid_date"}

    assert len(res.metrics_by_reason) == 1
    assert res.metrics_by_reason.loc[0, "reason"] == "invalid_date"
    assert int(res.metrics_by_reason.loc[0, "count"]) == 2


def test_missing_ids_go_to_discard_with_reason(monkeypatch):
    import src.dq.silver.crashes.v1.dq 
    from datetime import date as real_date

    class FakeDate(real_date):
        @classmethod
        def today(cls):
            return cls(2026, 3, 3)

    monkeypatch.setattr(src.dq.silver.crashes.v1.dq , "date", FakeDate)

    df = pd.DataFrame(
        {
            "unique_id": [1, None, 3, None],
            "collision_id": [10, 20, None, None],
            "crash_date": ["2026-03-03", "2026-03-03", "2026-03-03", "2026-03-03"],
        }
    )

    res = apply_quality_rules_crashes(df, run_date_str="2026-03-03")

    assert len(res.discard_df) == 3
    assert len(res.clean_df) == 1
    assert len(res.quarantine_df) == 0

    assert set(res.discard_df["dq_reasons"].unique()) == {"discard_missing_id"}

    assert len(res.metrics_by_reason) == 1
    assert res.metrics_by_reason.loc[0, "reason"] == "discard_missing_id"
    assert int(res.metrics_by_reason.loc[0, "count"]) == 3


def test_discard_and_quarantine_same_row_prefers_discard_bucket_and_keeps_reasons():
    df = pd.DataFrame(
        {
            "unique_id": [None, 2],
            "collision_id": [10, 20],
            "crash_date": [None, None],
        }
    )

    res = apply_quality_rules_crashes(df, run_date_str="2026-03-03")

    assert len(res.discard_df) == 1 
    assert len(res.quarantine_df) == 1  
    assert len(res.clean_df) == 0

    reasons = res.discard_df.iloc[0]["dq_reasons"]
    assert "invalid_date" in reasons
    assert "discard_missing_id" in reasons
    assert ";" in reasons  
    m = {row["reason"]: int(row["count"]) for _, row in res.metrics_by_reason.iterrows()}
    assert m["invalid_date"] == 2
    assert m["discard_missing_id"] == 1


def test_clean_df_drops_dq_reasons_column_only():
    df = pd.DataFrame(
        {
            "unique_id": [1],
            "collision_id": [10],
            "crash_date": ["2026-03-03"],
        }
    )
    res = apply_quality_rules_crashes(df, run_date_str="2026-03-03")

    assert "dq_reasons" not in res.clean_df.columns
    assert "run_date" in res.clean_df.columns
    assert res.clean_df.iloc[0]["run_date"] == "2026-03-03"