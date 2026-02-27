import time
from pathlib import Path

import pandas as pd
import pytest


from src.utils.io_utils import (
    find_latest_csv,
    _assert_columns_exist,
    _normalize_time_to_hhmm,
    _rmtree_force,
    _write_parquet_overwrite,
)


def test_find_latest_csv_raises_when_none(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        find_latest_csv(tmp_path)


def test_find_latest_csv_returns_most_recent(tmp_path: Path):
    f1 = tmp_path / "a.csv"
    f2 = tmp_path / "b.csv"

    f1.write_text("x\n1\n", encoding="utf-8")
    time.sleep(0.02)
    f2.write_text("x\n2\n", encoding="utf-8")

    latest = find_latest_csv(tmp_path)
    assert latest.name == "b.csv"


def test_assert_columns_exist_ok():
    df = pd.DataFrame({"a": [1], "b": [2]})
    _assert_columns_exist(df, ["a", "b"])


def test_assert_columns_exist_raises_keyerror():
    df = pd.DataFrame({"a": [1]})
    with pytest.raises(KeyError) as e:
        _assert_columns_exist(df, ["a", "b"])
    assert "Missing required columns" in str(e.value)
    assert "b" in str(e.value)


def test_normalize_time_to_hhmm_handles_nulls_and_blanks():
    s = pd.Series([None, pd.NA, "", "   "])
    out = _normalize_time_to_hhmm(s)

    assert out.dtype.name == "string"
    assert out.isna().all()


def test_normalize_time_to_hhmm_normalizes_hhmm_and_hhmmss():
    s = pd.Series(["1:2", "01:02", "9:5:3", "09:05:03", " 7:08 "])
    out = _normalize_time_to_hhmm(s).tolist()

    assert out == ["01:02", "01:02", "09:05:03", "09:05:03", "07:08"]


def test_normalize_time_to_hhmm_invalid_formats_become_na():
    s = pd.Series(["1234", "12", ":", "12:", ":34", "ab:cd", "25:00", "10:99"])
    out = _normalize_time_to_hhmm(s)

    assert out.isna().all()


def test_rmtree_force_removes_directory(tmp_path: Path):
    d = tmp_path / "to_delete"
    d.mkdir()
    (d / "x.txt").write_text("hi", encoding="utf-8")

    assert d.exists()
    _rmtree_force(d)
    assert not d.exists()


def test_write_parquet_overwrite_creates_single_file_when_no_partitions(tmp_path: Path):
    out_dir = tmp_path / "out_parquet"
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})

    _write_parquet_overwrite(out_dir, df, partition_cols=None)

    assert out_dir.exists()
    assert (out_dir / "data.parquet").exists()

    back = pd.read_parquet(out_dir / "data.parquet")
    pd.testing.assert_frame_equal(back, df)


def test_write_parquet_overwrite_overwrites_existing_directory(tmp_path: Path):
    out_dir = tmp_path / "out_parquet"
    out_dir.mkdir()
    (out_dir / "old.txt").write_text("old", encoding="utf-8")

    df = pd.DataFrame({"a": [1]})
    _write_parquet_overwrite(out_dir, df)

    assert not (out_dir / "old.txt").exists()
    assert (out_dir / "data.parquet").exists()


def test_write_parquet_overwrite_if_path_is_file(tmp_path: Path):
    out_path = tmp_path / "out_parquet"
    out_path.write_text("i am a file", encoding="utf-8")
    assert out_path.is_file()

    df = pd.DataFrame({"a": [1]})
    _write_parquet_overwrite(out_path, df)

    assert out_path.is_dir()
    assert (out_path / "data.parquet").exists()


@pytest.mark.skipif(
    pytest.importorskip("pyarrow") is None, reason="pyarrow not installed"
)
def test_write_parquet_overwrite_with_partitions_creates_partition_dirs(tmp_path: Path):
    out_dir = tmp_path / "out_part"
    df = pd.DataFrame(
        {
            "state": ["PR", "PR", "SP"],
            "value": [1, 2, 3],
        }
    )

    _write_parquet_overwrite(out_dir, df, partition_cols=["state"])

    assert out_dir.exists()
    assert any(p.is_dir() and p.name.startswith("state=") for p in out_dir.iterdir())