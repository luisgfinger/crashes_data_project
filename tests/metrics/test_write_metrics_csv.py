import pandas as pd
import pytest

from src.metrics.metrics import _write_metrics_csv


def read_metrics_csv(metrics_path):
    return pd.read_csv(metrics_path / "metrics.csv")


def test_creates_folder_and_writes_csv(tmp_path):
    metrics_path = tmp_path / "metrics_out"

    metrics_summary = pd.DataFrame(
        [
            {"run_date": "2026-02-27", "metric": "dq_total", "value": 10},
            {"run_date": "2026-02-27", "metric": "dq_passed", "value": 9},
        ]
    )

    metrics_by_reason = pd.DataFrame(
        [
            {"run_date": "2026-02-27", "reason": "missing_id", "count": 1},
            {"run_date": "2026-02-27", "reason": "invalid_date", "count": 2},
        ]
    )

    _write_metrics_csv(metrics_path, metrics_summary, metrics_by_reason)

    assert metrics_path.exists()
    assert (metrics_path / "metrics.csv").exists()

    df = read_metrics_csv(metrics_path)

    assert len(df) == 2 + 2

    assert list(df.columns) == ["run_date", "metric", "value", "reason", "count"]

    reasons_rows = df[df["metric"] == "dq_reason_count"].copy()
    assert len(reasons_rows) == 2
    assert (reasons_rows["value"] == reasons_rows["count"]).all()


def test_adds_reason_and_count_if_missing_in_summary(tmp_path):
    metrics_path = tmp_path / "metrics_out"

    metrics_summary = pd.DataFrame(
        [{"run_date": "2026-02-27", "metric": "dq_total", "value": 10}]
    )

    _write_metrics_csv(metrics_path, metrics_summary, metrics_by_reason=None)

    df = read_metrics_csv(metrics_path)

    assert list(df.columns) == ["run_date", "metric", "value", "reason", "count"]
    assert len(df) == 1
    assert df.loc[0, "metric"] == "dq_total"


def test_metrics_by_reason_none_creates_empty_reasons_block(tmp_path):
    metrics_path = tmp_path / "metrics_out"

    metrics_summary = pd.DataFrame(
        [{"run_date": "2026-02-27", "metric": "dq_total", "value": 10}]
    )

    _write_metrics_csv(metrics_path, metrics_summary, metrics_by_reason=None)

    df = read_metrics_csv(metrics_path)
    assert len(df) == 1
    assert (df["metric"] == "dq_total").all()


def test_metrics_by_reason_empty_dataframe_creates_empty_reasons_block(tmp_path):
    metrics_path = tmp_path / "metrics_out"

    metrics_summary = pd.DataFrame(
        [{"run_date": "2026-02-27", "metric": "dq_total", "value": 10}]
    )

    empty_reasons = pd.DataFrame(columns=["run_date", "reason", "count"])
    _write_metrics_csv(metrics_path, metrics_summary, metrics_by_reason=empty_reasons)

    df = read_metrics_csv(metrics_path)
    assert len(df) == 1
    assert (df["metric"] == "dq_total").all()


def test_removes_existing_directory_before_writing(tmp_path):
    metrics_path = tmp_path / "metrics_out"
    metrics_path.mkdir(parents=True)
    (metrics_path / "old.txt").write_text("should be removed", encoding="utf-8")

    metrics_summary = pd.DataFrame(
        [{"run_date": "2026-02-27", "metric": "dq_total", "value": 10}]
    )

    _write_metrics_csv(metrics_path, metrics_summary, metrics_by_reason=None)

    assert (metrics_path / "old.txt").exists() is False
    assert (metrics_path / "metrics.csv").exists() is True


def test_raises_if_summary_missing_required_columns(tmp_path):
    metrics_path = tmp_path / "metrics_out"

    bad_summary = pd.DataFrame([{"run_date": "2026-02-27"}])

    with pytest.raises(ValueError, match="metrics_summary must contain columns: run_date, metric, value"):
        _write_metrics_csv(metrics_path, bad_summary, metrics_by_reason=None)