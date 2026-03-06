from __future__ import annotations

from pathlib import Path
import typer
import pandas as pd

from src.bronze.ingest_bronze import ingest_bronze_dataset
from src.utils.io_utils import _write_parquet_overwrite


def run(run_date_str: str, variant: str, dry_run: bool, start_date: str) -> None:
    dataset = "crashes"
    where = f"crash_date >= '{start_date}T00:00:00.000'"
    order = "crash_date"

    typer.echo(
        f"[BRONZE] dataset={dataset} variant={variant} run_date={run_date_str} start_date={start_date}"
    )
    typer.echo(f"[BRONZE] where={where}")
    typer.echo(f"[BRONZE] order={order}")

    if dry_run:
        typer.echo("[BRONZE] dry_run=True -> nothing executed.")
        return

    jsonl_path_str = ingest_bronze_dataset(
        dataset=dataset,
        variant=variant,
        run_date=run_date_str,
        where=where,
        order=order,
    )

    jsonl_path = Path(jsonl_path_str)
    typer.echo(f"[BRONZE] JSONL written: {jsonl_path}")

    df = pd.read_json(jsonl_path, lines=True)

    typer.echo(f"[BRONZE] rows loaded into df: {len(df):,}")

    run_date_clean = run_date_str.replace("-", "")
    parquet_path = jsonl_path.parent / f"raw_{run_date_clean}.parquet"

    _write_parquet_overwrite(parquet_path, df)

    jsonl_path.unlink()

    typer.echo(f"[BRONZE] Parquet written at: {parquet_path}")