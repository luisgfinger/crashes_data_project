from __future__ import annotations

import os
import typer

from src.bronze.ingest_bronze import ingest_bronze_dataset


def run(run_date_str: str, variant: str, dry_run: bool, start_date: str) -> None:
    dataset = "crashes"
    run_date = run_date_str.replace("-", "") 

    where = f"crash_date >= '{start_date}T00:00:00.000'"
    order = "crash_date"
    token = os.getenv("SOCRATA_APP_TOKEN")

    typer.echo(f"[BRONZE] dataset={dataset} variant={variant} run_date={run_date_str} start_date={start_date}")
    typer.echo(f"[BRONZE] where={where}")
    typer.echo(f"[BRONZE] order={order}")

    if dry_run:
        return

    out = ingest_bronze_dataset(
        dataset=dataset,
        variant=variant,
        app_token=token,
        run_date=run_date,
        limit=50_000,
        where=where,
        order=order,
    )