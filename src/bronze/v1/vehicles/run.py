from __future__ import annotations
import typer
import pandas as pd

from api.source_nyc.nyc_open_data import NYCOpenDataClient
from src.config import bronze_path
from src.utils.io_utils import _write_parquet_overwrite


def run(run_date_str: str, variant: str, dry_run: bool, start_date: str) -> None:
    dataset = "vehicles"
    where = f"crash_date >= '{start_date}T00:00:00.000'"
    order = "collision_id"

    typer.echo(
        f"[BRONZE] dataset={dataset} variant={variant} run_date={run_date_str}"
    )
    typer.echo(f"[BRONZE] where={where}")
    typer.echo(f"[BRONZE] order={order}")

    if dry_run:
        typer.echo("[BRONZE] dry_run=True -> nothing executed.")
        return

    client = NYCOpenDataClient()

    all_rows = []

    for chunk in client.download_json_paged(
        dataset=dataset,
        where=where,
        order=order,
    ):
        all_rows.extend(chunk)

    df = pd.DataFrame(all_rows)

    typer.echo(f"[BRONZE] rows downloaded: {len(df):,}")

    bronze_dir = bronze_path(dataset)

    run_date_clean = run_date_str.replace("-", "")
    parquet_path = bronze_dir / f"raw_{run_date_clean}.parquet"

    _write_parquet_overwrite(parquet_path, df)

    typer.echo(f"[BRONZE] Parquet written at: {parquet_path}")