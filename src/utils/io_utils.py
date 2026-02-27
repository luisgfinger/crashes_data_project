from pathlib import Path
import pandas as pd
import shutil


def find_latest_csv(folder: Path) -> Path:
    csv_files = sorted(folder.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No CSV found in {folder}")

    return max(csv_files, key=lambda f: f.stat().st_mtime)

def _assert_columns_exist(df: pd.DataFrame, required: list[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Missing required columns in Bronze CSV: {missing}")
    
def _normalize_time_to_hhmm(s: pd.Series) -> pd.Series:
    def fix(x):
        if x is None or pd.isna(x):
            return pd.NA
        t = str(x).strip()
        if not t:
            return pd.NA
        parts = t.split(":")
        if len(parts) not in (2, 3):
            return pd.NA
        if any(p.strip() == "" for p in parts):
            return pd.NA
        if not all(p.isdigit() for p in parts):
            return pd.NA
        hh = int(parts[0])
        mm = int(parts[1])
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            return pd.NA
        if len(parts) == 3:
            ss = int(parts[2])
            if not (0 <= ss <= 59):
                return pd.NA
            return f"{hh:02d}:{mm:02d}:{ss:02d}"
        return f"{hh:02d}:{mm:02d}"

    return s.apply(fix).astype("string")

def _rmtree_force(path: Path) -> None:
    def onerror(func, p, exc_info):
        os.chmod(p, stat.S_IWRITE)
        func(p)

    shutil.rmtree(path, onerror=onerror)

def _write_parquet_overwrite(path, df: pd.DataFrame, partition_cols=None) -> None:
    path = Path(path)
    if path.exists():
        if path.is_file():
            path.unlink()
        else:
            _rmtree_force(path)

    path.mkdir(parents=True, exist_ok=True)

    if partition_cols:
        df.to_parquet(path, engine="pyarrow", index=False, partition_cols=partition_cols)
    else:
        df.to_parquet(path / "data.parquet", engine="pyarrow", index=False)


