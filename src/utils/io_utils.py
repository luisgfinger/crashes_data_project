from pathlib import Path
import pandas as pd
import shutil


def find_latest_csv(folder: Path) -> Path:
    """
    Returns the most recently modified CSV file in the given folder.
    Raises FileNotFoundError if no CSV is found.
    """
    csv_files = sorted(folder.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No CSV found in {folder}")

    return max(csv_files, key=lambda f: f.stat().st_mtime)

def _assert_columns_exist(df: pd.DataFrame, required: list[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Missing required columns in Bronze CSV: {missing}")


def _normalize_time_to_hhmm(s: pd.Series) -> pd.Series:
    # Accepts strings like "7:5" -> "07:05"; invalid -> <NA>
    def fix(x):
        if x is None or pd.isna(x):
            return pd.NA
        t = str(x).strip()
        if not t:
            return pd.NA
        parts = t.split(":")
        if len(parts) < 2:
            return pd.NA
        hh = parts[0].zfill(2)
        mm = parts[1].zfill(2)
        if len(parts) >= 3:
            ss = parts[2].zfill(2)
            return f"{hh}:{mm}:{ss}"
        return f"{hh}:{mm}"

    return s.apply(fix).astype("string")

def _rmtree_force(path: Path) -> None:
    """Remove folder on Windows even if files are readonly."""
    def onerror(func, p, exc_info):
        # remove readonly and retry
        os.chmod(p, stat.S_IWRITE)
        func(p)

    shutil.rmtree(path, onerror=onerror)

def _write_parquet_overwrite(path, df: pd.DataFrame, partition_cols=None) -> None:
    path = Path(path)

    # If something exists there (file or folder), remove it
    if path.exists():
        if path.is_file():
            path.unlink()
        else:
            _rmtree_force(path)

    path.mkdir(parents=True, exist_ok=True)

    # If no partitioning, ALWAYS write to a file inside the folder
    if partition_cols:
        df.to_parquet(path, engine="pyarrow", index=False, partition_cols=partition_cols)
    else:
        df.to_parquet(path / "data.parquet", engine="pyarrow", index=False)