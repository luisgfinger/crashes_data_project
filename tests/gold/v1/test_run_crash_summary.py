import pandas as pd

from src.gold.v1.run import run


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
            "collision_id": [100, 100, 200],
            "unique_id": [1, 2, 3],
            "vehicle_occupants": [2, 1, 1],
            "vehicle_make": [" Toyota ", "Toyota", "Ford"],
            "vehicle_type": [" Sedan ", "Sedan", "SUV"],
        }
    )


def _df_crashes():
    return pd.DataFrame(
        {
            "collision_id": [100, 200],
            "crash_date": pd.to_datetime(["2026-03-03", "2026-03-03"]),
            "crash_time": ["09:30", "10:00"],
            "number_of_persons_injured": [1, 0],
            "number_of_persons_killed": [0, 0],
            "number_of_pedestrians_injured": [0, 0],
            "number_of_pedestrians_killed": [0, 0],
            "number_of_cyclist_injured": [0, 0],
            "number_of_cyclist_killed": [0, 0],
            "number_of_motorist_injured": [1, 0],
            "number_of_motorist_killed": [0, 0],
        }
    )


def test_dry_run_skips_writes_and_builds_expected_output(monkeypatch, capsys):
    calls = {
        "build_dim_called": 0,
        "build_input": None,
        "build_fact_called": 0,
        "build_fact_inputs": None,
        "write_parquet": 0,
    }

    monkeypatch.setattr("src.gold.v1.run.silver_path", lambda dataset, version, variant: DummyPath(f"/silver/{dataset}/{version}/{variant}"))
    monkeypatch.setattr("src.gold.v1.run.gold_dim_path", lambda version, table: DummyPath(f"/gold/{version}/dim/{table}"))
    monkeypatch.setattr("src.gold.v1.run.gold_fact_path", lambda version, table: DummyPath(f"/gold/{version}/fact/{table}"))

    def fake_read_parquet(path):
        path_str = str(path)
        if path_str == "/silver/vehicles/v1/full/run_date=2026-03-03":
            return _df_vehicles()
        if path_str == "/silver/crashes/v1/full/run_date=2026-03-03":
            return _df_crashes()
        raise AssertionError(f"unexpected path: {path_str}")

    monkeypatch.setattr("src.gold.v1.run.pd.read_parquet", fake_read_parquet)

    def fake_build_dim_vehicle(df):
        calls["build_dim_called"] += 1
        calls["build_input"] = df.copy()
        return pd.DataFrame(
            {
                "vehicle_dim_id": [101, 202],
                "vehicle_make": ["TOYOTA", "FORD"],
                "vehicle_type": ["SEDAN", "SUV"],
            }
        )

    monkeypatch.setattr("src.gold.v1.run.build_dim_vehicle", fake_build_dim_vehicle)

    def fake_build_fact_crash(crashes, vehicles, run_date_str):
        calls["build_fact_called"] += 1
        calls["build_fact_inputs"] = {
            "crashes": crashes.copy(),
            "vehicles": vehicles.copy(),
            "run_date": run_date_str,
        }
        return pd.DataFrame(
            {
                "collision_id": [100, 200],
                "run_date": [run_date_str, run_date_str],
            }
        )

    monkeypatch.setattr("src.gold.v1.run.build_fact_crash", fake_build_fact_crash)

    monkeypatch.setattr("src.gold.v1.run._write_parquet_overwrite", lambda *a, **k: calls.__setitem__("write_parquet", calls["write_parquet"] + 1))

    run(run_date_str="2026-03-03", variant="full", dry_run=True)

    out = capsys.readouterr().out
    assert "Silver vehicles rows read: 3" in out
    assert "Silver crashes rows read:  2" in out
    assert "dim_vehicle rows: 2" in out
    assert "fact_crash rows: 2" in out
    assert "[DRY-RUN] Skipping writes." in out
    assert calls["build_dim_called"] == 1
    assert calls["build_input"] is not None
    assert calls["build_fact_called"] == 1
    assert calls["build_fact_inputs"]["run_date"] == "2026-03-03"
    assert calls["write_parquet"] == 0


def test_non_dry_run_writes_dim_and_fact_to_expected_paths(monkeypatch):
    writes = []

    monkeypatch.setattr("src.gold.v1.run.silver_path", lambda dataset, version, variant: DummyPath(f"/silver/{dataset}/{version}/{variant}"))
    monkeypatch.setattr("src.gold.v1.run.gold_dim_path", lambda version, table: DummyPath(f"/gold/{version}/dim/{table}"))
    monkeypatch.setattr("src.gold.v1.run.gold_fact_path", lambda version, table: DummyPath(f"/gold/{version}/fact/{table}"))

    def fake_read_parquet(path):
        if "vehicles" in str(path):
            return _df_vehicles()
        return _df_crashes()

    monkeypatch.setattr("src.gold.v1.run.pd.read_parquet", fake_read_parquet)

    monkeypatch.setattr(
        "src.gold.v1.run.build_dim_vehicle",
        lambda df: pd.DataFrame(
            {
                "vehicle_dim_id": [101],
                "vehicle_make": ["TOYOTA"],
                "vehicle_type": ["SEDAN"],
            }
        ),
    )
    monkeypatch.setattr(
        "src.gold.v1.run.build_fact_crash",
        lambda crashes, vehicles, run_date_str: pd.DataFrame(
            {
                "collision_id": [100, 200],
                "run_date": [run_date_str, run_date_str],
            }
        ),
    )

    def fake_write_parquet_overwrite(path, df, partition_cols=None):
        writes.append(
            {
                "path": str(path),
                "rows": len(df),
                "cols": list(df.columns),
                "partition_cols": partition_cols,
            }
        )

    monkeypatch.setattr("src.gold.v1.run._write_parquet_overwrite", fake_write_parquet_overwrite)

    run(run_date_str="2026-03-03", variant="full", dry_run=False)

    assert len(writes) == 2
    assert writes[0]["path"] == "/gold/v1/dim/dim_vehicle"
    assert writes[0]["rows"] == 1
    assert writes[0]["cols"] == ["vehicle_dim_id", "vehicle_make", "vehicle_type"]
    assert writes[0]["partition_cols"] is None
    assert writes[1]["path"] == "/gold/v1/fact/fact_crash"
    assert writes[1]["rows"] == 2
    assert writes[1]["cols"] == ["collision_id", "run_date"]
    assert writes[1]["partition_cols"] == ["run_date"]
