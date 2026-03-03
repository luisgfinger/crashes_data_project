import pandas as pd
import pytest

from src.silver.vehicles.v1.run import run


class DummyPath:
    """Path fake só para suportar / e prints."""
    def __init__(self, value: str):
        self.value = value

    def __truediv__(self, other: str):
        return DummyPath(f"{self.value}/{other}")

    def __str__(self):
        return self.value

    def __repr__(self):
        return f"DummyPath({self.value!r})"


class DummyDQ:
    def __init__(self, clean_df, quarantine_df, metrics_summary, metrics_by_reason, run_date):
        self.clean_df = clean_df
        self.quarantine_df = quarantine_df
        self.metrics_summary = metrics_summary
        self.metrics_by_reason = metrics_by_reason
        self.run_date = run_date


def _make_raw_df():
    return pd.DataFrame(
        {
            "UNIQUE_ID": [" 1 ", "x", None],
            "COLLISION_ID": [" 10", "20 ", "30 "],
            "VEHICLE_TYPE": [" Sedan ", "Truck", None],
            "VEHICLE_MAKE": [" Toyota ", " Ford ", ""],
            "VEHICLE_YEAR": [" 2010 ", "not_a_year", None],
            "STATE_REGISTRATION": [" NY ", " NJ", None],
            "VEHICLE_DAMAGE": [" Front ", None, ""],
            "VEHICLE_DAMAGE_1": ["Rear ", " Side", None],
            "VEHICLE_DAMAGE_2": ["", None, " Roof "],
            "VEHICLE_DAMAGE_3": [None, "", " "],
        }
    )


def test_run_vehicles_dry_run_skips_writes_and_applies_transforms(monkeypatch, capsys):
    calls = {
        "assert_cols": 0,
        "dq_df": None,
        "write_clean": 0,
        "write_quarantine": 0,
        "write_metrics": 0,
    }

    monkeypatch.setattr("src.silver.vehicles.v1.run.bronze_path", lambda dataset: DummyPath(f"/bronze/{dataset}"))
    monkeypatch.setattr("src.silver.vehicles.v1.run.silver_path", lambda dataset, version, variant: DummyPath(f"/silver/{dataset}/{version}/{variant}"))
    monkeypatch.setattr("src.silver.vehicles.v1.run.silver_quarantine_path", lambda dataset, version, variant: DummyPath(f"/quarantine/{dataset}/{version}/{variant}"))
    monkeypatch.setattr("src.silver.vehicles.v1.run.silver_metrics_path", lambda dataset, version, variant: DummyPath(f"/metrics/{dataset}/{version}/{variant}"))

    monkeypatch.setattr("src.silver.vehicles.v1.run.find_latest_csv", lambda bronze_dir: "/bronze/latest.csv")

    raw_df = _make_raw_df()
    monkeypatch.setattr("src.silver.vehicles.v1.run.pd.read_csv", lambda *args, **kwargs: raw_df.copy())

    def fake_assert_columns_exist(df, cols):
        calls["assert_cols"] += 1
        missing = [c for c in cols if c not in df.columns]
        assert not missing

    monkeypatch.setattr("src.silver.vehicles.v1.run._assert_columns_exist", fake_assert_columns_exist)

    def fake_apply_quality_rules_vehicles(df, run_date_str):
        assert set(df.columns) == {
            "unique_id",
            "collision_id",
            "vehicle_type",
            "vehicle_make",
            "vehicle_year",
            "state_registration",
            "vehicle_damage",
            "vehicle_damage_1",
            "vehicle_damage_2",
            "vehicle_damage_3",
        }

        assert df.loc[0, "vehicle_type"] == "Sedan"
        assert df.loc[0, "vehicle_make"] == "Toyota"
        assert df.loc[0, "state_registration"] == "NY"
        assert str(df["unique_id"].dtype) == "Int64"
        assert str(df["vehicle_year"].dtype) == "Int64"
        assert int(df.loc[0, "unique_id"]) == 1
        assert int(df.loc[0, "vehicle_year"]) == 2010
        assert pd.isna(df.loc[1, "vehicle_year"]) 

        calls["dq_df"] = df.copy()

        clean = df.iloc[[0]].copy()
        quarantine = df.iloc[[1]].copy()
        metrics_summary = pd.DataFrame(
            [{"run_date": run_date_str, "metric": "total_rows_read", "value": len(df)}]
        )
        metrics_by_reason = pd.DataFrame(columns=["run_date", "reason", "count"])
        return DummyDQ(clean, quarantine, metrics_summary, metrics_by_reason, run_date_str)

    monkeypatch.setattr("src.silver.vehicles.v1.run.apply_quality_rules_vehicles", fake_apply_quality_rules_vehicles)

    monkeypatch.setattr(
        "src.silver.vehicles.v1.run._write_parquet_overwrite",
        lambda *a, **k: calls.__setitem__("write_clean", calls["write_clean"] + 1),
    )
    monkeypatch.setattr(
        "src.silver.vehicles.v1.run._write_metrics_csv",
        lambda *a, **k: calls.__setitem__("write_metrics", calls["write_metrics"] + 1),
    )

    run(run_date_str="2026-03-03", variant="full", dry_run=True)

    out = capsys.readouterr().out
    assert "Reading Bronze file:" in out
    assert "DQ summary:" in out
    assert "[DRY-RUN] Skipping writes." in out

    assert calls["assert_cols"] == 1
    assert calls["dq_df"] is not None
    assert calls["write_clean"] == 0
    assert calls["write_metrics"] == 0


def test_run_vehicles_non_dry_run_calls_writes_with_expected_paths(monkeypatch):
    writes = {"clean": None, "quarantine": None, "metrics": None}

    monkeypatch.setattr("src.silver.vehicles.v1.run.bronze_path", lambda dataset: DummyPath(f"/bronze/{dataset}"))
    monkeypatch.setattr("src.silver.vehicles.v1.run.silver_path", lambda dataset, version, variant: DummyPath(f"/silver/{dataset}/{version}/{variant}"))
    monkeypatch.setattr("src.silver.vehicles.v1.run.silver_quarantine_path", lambda dataset, version, variant: DummyPath(f"/quarantine/{dataset}/{version}/{variant}"))
    monkeypatch.setattr("src.silver.vehicles.v1.run.silver_metrics_path", lambda dataset, version, variant: DummyPath(f"/metrics/{dataset}/{version}/{variant}"))

    monkeypatch.setattr("src.silver.vehicles.v1.run.find_latest_csv", lambda bronze_dir: "/bronze/latest.csv")
    monkeypatch.setattr("src.silver.vehicles.v1.run.pd.read_csv", lambda *args, **kwargs: _make_raw_df())

    monkeypatch.setattr("src.silver.vehicles.v1.run._assert_columns_exist", lambda df, cols: None)

    def fake_dq(df, run_date_str):
        clean = df.copy()
        quarantine = df.iloc[0:0].copy()
        metrics_summary = pd.DataFrame([{"run_date": run_date_str, "metric": "total_rows_read", "value": len(df)}])
        metrics_by_reason = pd.DataFrame(columns=["run_date", "reason", "count"])
        return DummyDQ(clean, quarantine, metrics_summary, metrics_by_reason, run_date_str)

    monkeypatch.setattr("src.silver.vehicles.v1.run.apply_quality_rules_vehicles", fake_dq)

    def fake_write_parquet_overwrite(path, df, partition_cols=None):
        if partition_cols is not None:
            writes["clean"] = {"path": str(path), "rows": len(df), "partition_cols": partition_cols}
        else:
            writes["quarantine"] = {"path": str(path), "rows": len(df)}

    monkeypatch.setattr("src.silver.vehicles.v1.run._write_parquet_overwrite", fake_write_parquet_overwrite)

    def fake_write_metrics_csv(path, metrics_summary, metrics_by_reason):
        writes["metrics"] = {
            "path": str(path),
            "summary_rows": len(metrics_summary),
            "by_reason_rows": len(metrics_by_reason),
        }

    monkeypatch.setattr("src.silver.vehicles.v1.run._write_metrics_csv", fake_write_metrics_csv)

    run(run_date_str="2026-03-03", variant="full", dry_run=False)

    assert writes["clean"] is not None
    assert writes["clean"]["path"] == "/silver/vehicles/v1/full"
    assert writes["clean"]["partition_cols"] == ["run_date"]

    assert writes["quarantine"] is not None
    assert writes["quarantine"]["path"] == "/quarantine/vehicles/v1/full/run_date=2026-03-03"

    assert writes["metrics"] is not None
    assert writes["metrics"]["path"] == "/metrics/vehicles/v1/full/run_date=2026-03-03"