import pandas as pd
import pytest

from src.gold.crash_summary.v1.run import run


class DummyPath:
    def __init__(self, value: str):
        self.value = value

    def __truediv__(self, other: str):
        return DummyPath(f"{self.value}/{other}")

    def __str__(self):
        return self.value

    def __repr__(self):
        return f"DummyPath({self.value!r})"


def _df_vehicles():
    return pd.DataFrame(
        {
            "collision_id": [100, 100, 100, 200, None],
            "vehicle_type": ["A", "B", "C", "D", "E"],
        }
    )


def _df_crashes():
    return pd.DataFrame(
        {
            "collision_id": [100, 100, 200, 300],
            "crash_year": [2026, 2026, 2025, 2024],
            "contributing_factor_1": ["", None, "Speeding", ""],
            "contributing_factor_2": ["Alcohol", "Alcohol", "", ""],
        }
    )


def test_dry_run_skips_writes_and_builds_expected_output(monkeypatch, capsys):
    calls = {
        "apply_dq_called": 0,
        "dq_input": None,
        "write_parquet": 0,
        "write_metrics": 0,
    }

    monkeypatch.setattr("src.gold.crash_summary.v1.run.silver_path", lambda dataset, version, variant: DummyPath(f"/silver/{dataset}/{version}/{variant}"))
    monkeypatch.setattr("src.gold.crash_summary.v1.run.gold_path", lambda dataset, version, variant: DummyPath(f"/gold/{dataset}/{version}/{variant}"))
    monkeypatch.setattr("src.gold.crash_summary.v1.run.gold_metrics_path", lambda dataset, version, variant: DummyPath(f"/gold-metrics/{dataset}/{version}/{variant}"))

    def fake_read_parquet(path, dtype_backend=None):
        path_str = str(path)
        if "/vehicles/" in path_str:
            return _df_vehicles()
        if "/crashes/" in path_str:
            return _df_crashes()
        raise AssertionError(f"unexpected parquet path: {path_str}")

    monkeypatch.setattr("src.gold.crash_summary.v1.run.pd.read_parquet", fake_read_parquet)

    def fake_apply_quality_rules(df):
        calls["apply_dq_called"] += 1
        calls["dq_input"] = df.copy()

        assert list(df.columns) == ["collision_id", "total_vehicles", "crash_year", "main_contributing_factor"]

        m = df.set_index("collision_id")["total_vehicles"].to_dict()
        assert m[100] == 3
        assert m[200] == 1
        assert pd.isna(m[300])

        mc = df.set_index("collision_id")["main_contributing_factor"].to_dict()
        assert mc[100] == "Alcohol"
        assert mc[200] == "Speeding"
        assert mc[300] == "Unspecified"

        return df

    monkeypatch.setattr("src.gold.crash_summary.v1.run.apply_quality_rules", fake_apply_quality_rules)

    monkeypatch.setattr("src.gold.crash_summary.v1.run._write_parquet_overwrite", lambda *a, **k: calls.__setitem__("write_parquet", calls["write_parquet"] + 1))
    monkeypatch.setattr("src.gold.crash_summary.v1.run._write_metrics_csv", lambda *a, **k: calls.__setitem__("write_metrics", calls["write_metrics"] + 1))

    run(run_date_str="2026-03-03", variant="full", dry_run=True)

    out = capsys.readouterr().out
    assert "[DRY-RUN] Skipping writes." in out
    assert calls["apply_dq_called"] == 1
    assert calls["dq_input"] is not None
    assert calls["write_parquet"] == 0
    assert calls["write_metrics"] == 0


def test_non_dry_run_writes_gold_and_metrics_with_expected_paths_and_partition(monkeypatch):
    writes = {"parquet": None, "metrics": None}

    monkeypatch.setattr("src.gold.crash_summary.v1.run.silver_path", lambda dataset, version, variant: DummyPath(f"/silver/{dataset}/{version}/{variant}"))
    monkeypatch.setattr("src.gold.crash_summary.v1.run.gold_path", lambda dataset, version, variant: DummyPath(f"/gold/{dataset}/{version}/{variant}"))
    monkeypatch.setattr("src.gold.crash_summary.v1.run.gold_metrics_path", lambda dataset, version, variant: DummyPath(f"/gold-metrics/{dataset}/{version}/{variant}"))
    monkeypatch.setattr(
        "src.gold.crash_summary.v1.run.pd.read_parquet",
        lambda path, dtype_backend=None: _df_vehicles() if "/vehicles/" in str(path) else _df_crashes(),
    )

    monkeypatch.setattr("src.gold.crash_summary.v1.run.apply_quality_rules", lambda df: df)

    def fake_write_parquet_overwrite(path, df, partition_cols=None):
        writes["parquet"] = {"path": str(path), "rows": len(df), "partition_cols": partition_cols}

    monkeypatch.setattr("src.gold.crash_summary.v1.run._write_parquet_overwrite", fake_write_parquet_overwrite)

    def fake_write_metrics_csv(path, metrics_summary, metrics_by_reason):
        writes["metrics"] = {
            "path": str(path),
            "summary": metrics_summary.copy(),
            "by_reason": metrics_by_reason.copy(),
        }

    monkeypatch.setattr("src.gold.crash_summary.v1.run._write_metrics_csv", fake_write_metrics_csv)

    run(run_date_str="2026-03-03", variant="full", dry_run=False)

    assert writes["parquet"] is not None
    assert writes["parquet"]["path"] == "/gold/crash_summary/v1/full"
    assert writes["parquet"]["partition_cols"] == ["run_date"]

    assert writes["metrics"] is not None
    assert writes["metrics"]["path"] == "/gold-metrics/crash_summary/v1/full/run_date=2026-03-03"

    ms = writes["metrics"]["summary"]
    assert set(ms["metric"]) == {
        "rows_out",
        "distinct_collision_id",
        "null_total_vehicles",
        "null_main_contributing_factor",
    }

    rows_out = int(ms.loc[ms["metric"] == "rows_out", "value"].iloc[0])
    assert rows_out == 3

    distinct = int(ms.loc[ms["metric"] == "distinct_collision_id", "value"].iloc[0])
    assert distinct == 3

    null_tv = int(ms.loc[ms["metric"] == "null_total_vehicles", "value"].iloc[0])
    assert null_tv == 1

    null_mcf = int(ms.loc[ms["metric"] == "null_main_contributing_factor", "value"].iloc[0])
    assert null_mcf == 0