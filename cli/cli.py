import typer
from datetime import date, datetime
from enum import Enum
from typing import Callable, Dict, Optional, Tuple

from api.s3_upload.s3_upload import upload_dir_to_s3
from src.silver.v1.vehicles.run import run as run_silver_vehicles_v1
from src.silver.v1.crashes.run import run as run_silver_crashes_v1

from src.gold.v1.run import run as run_gold_v1

from src.bronze.v1.vehicles.run import run as run_bronze_vehicles_v1
from src.bronze.v1.crashes.run import run as run_bronze_crashes_v1

app = typer.Typer(help="crashes-data-project CLI")
bronze_app = typer.Typer(help="Run BRONZE pipelines") 
silver_app = typer.Typer(help="Run SILVER pipelines")
gold_app = typer.Typer(help="Run GOLD pipelines")


class Dataset(str, Enum):
    vehicles = "vehicles"
    crashes = "crashes"


class Version(str, Enum):
    v1 = "v1"


class Variant(str, Enum):
    full = "full"
    incremental = "incremental"
    backfill = "backfill"


class GoldDataset(str, Enum):
    warehouse = "warehouse"


class GoldVersion(str, Enum):
    v1 = "v1"


PipelineFn = Callable[..., None]

BRONZE_PIPELINES: Dict[Tuple[Dataset, Version], PipelineFn] = {
    (Dataset.vehicles, Version.v1): run_bronze_vehicles_v1,
    (Dataset.crashes, Version.v1): run_bronze_crashes_v1,
}

PIPELINES: Dict[Tuple[Dataset, Version], PipelineFn] = {
    (Dataset.vehicles, Version.v1): run_silver_vehicles_v1,
    (Dataset.crashes, Version.v1): run_silver_crashes_v1,
}

GOLD_PIPELINES: Dict[Tuple[GoldDataset, GoldVersion], PipelineFn] = {
    (GoldDataset.warehouse, GoldVersion.v1): run_gold_v1,
}


def _validate_run_date(run_date: Optional[str]) -> str:
    run_date_str = run_date or date.today().isoformat()
    try:
        datetime.strptime(run_date_str, "%Y-%m-%d")
    except ValueError as e:
        raise typer.BadParameter("--run-date must be in YYYY-MM-DD format") from e
    return run_date_str


def _validate_start_date(start_date: str) -> str:
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
    except ValueError as e:
        raise typer.BadParameter("--start-date must be in YYYY-MM-DD format") from e
    return start_date


def _available_bronze_pipelines_hint() -> str:
    items = sorted([f"bronze/{ds.value}/{ver.value}" for (ds, ver) in BRONZE_PIPELINES.keys()])
    return "Available pipelines:\n- " + "\n- ".join(items)


def _available_pipelines_hint() -> str:
    items = sorted([f"silver/{ds.value}/{ver.value}" for (ds, ver) in PIPELINES.keys()])
    return "Available pipelines:\n- " + "\n- ".join(items)


def _available_gold_pipelines_hint() -> str:
    items = sorted([f"gold/{ds.value}/{ver.value}" for (ds, ver) in GOLD_PIPELINES.keys()])
    return "Available pipelines:\n- " + "\n- ".join(items)


@bronze_app.command("run")
def run_bronze(
    dataset: Dataset = typer.Option(
        Dataset.vehicles, "--dataset", "-d",
        help="Which dataset pipeline to run.", show_default=True
    ),
    version: Version = typer.Option(
        Version.v1, "--version", "-v",
        help="Pipeline version.", show_default=True
    ),
    variant: Variant = typer.Option(
        Variant.full, "--variant",
        help="Execution mode (full/incremental/backfill).", show_default=True
    ),
    start_date: str = typer.Option(
        "2023-01-01", "--start-date",
        help="Lower bound (inclusive) for ingestion. Default is 2023-01-01.", show_default=True
    ),
    run_date: Optional[str] = typer.Option(
        None, "--run-date",
        help="Run date in YYYY-MM-DD. Defaults to today.", show_default=False
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run",
        help="Simulate execution (no writes).", show_default=True
    ),
):
    run_date_str = _validate_run_date(run_date)
    start_date_str = _validate_start_date(start_date)

    pipeline = BRONZE_PIPELINES.get((dataset, version))
    if pipeline is None:
        raise typer.BadParameter(
            f"Pipeline not found: bronze/{dataset.value}/{version.value}\n\n{_available_bronze_pipelines_hint()}"
        )
    pipeline(run_date_str=run_date_str, variant=variant.value, dry_run=dry_run, start_date=start_date_str)


@silver_app.command("run")
def run_silver(
    dataset: Dataset = typer.Option(
        Dataset.vehicles, "--dataset", "-d",
        help="Which dataset pipeline to run.", show_default=True
    ),
    version: Version = typer.Option(
        Version.v1, "--version", "-v",
        help="Pipeline version.", show_default=True
    ),
    variant: Variant = typer.Option(
        Variant.full, "--variant",
        help="Execution mode (full/incremental/backfill).", show_default=True
    ),
    run_date: Optional[str] = typer.Option(
        None, "--run-date",
        help="Run date in YYYY-MM-DD. Defaults to today.", show_default=False
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run",
        help="Simulate execution (no writes).", show_default=True
    ),
):
    run_date_str = _validate_run_date(run_date)

    pipeline = PIPELINES.get((dataset, version))
    if pipeline is None:
        raise typer.BadParameter(
            f"Pipeline not found: silver/{dataset.value}/{version.value}\n\n{_available_pipelines_hint()}"
        )

    pipeline(run_date_str=run_date_str, variant=variant.value, dry_run=dry_run)


@gold_app.command("run")
def run_gold(
    dataset: GoldDataset = typer.Option(
        GoldDataset.warehouse, "--dataset", "-d",
        help="Gold orchestrator pipeline.", show_default=True
    ),
    version: GoldVersion = typer.Option(
        GoldVersion.v1, "--version", "-v",
        help="Pipeline version.", show_default=True
    ),
    variant: Variant = typer.Option(
        Variant.full, "--variant",
        help="Execution mode (full/incremental/backfill).", show_default=True
    ),
    run_date: Optional[str] = typer.Option(
        None, "--run-date",
        help="Run date in YYYY-MM-DD. Defaults to today.", show_default=False
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run",
        help="Simulate execution (no writes).", show_default=True
    ),
):
    run_date_str = _validate_run_date(run_date)

    pipeline = GOLD_PIPELINES.get((dataset, version))
    if pipeline is None:
        raise typer.BadParameter(
            f"Pipeline not found: gold/{dataset.value}/{version.value}\n\n{_available_gold_pipelines_hint()}"
        )

    pipeline(run_date_str=run_date_str, variant=variant.value, dry_run=dry_run)

    if not dry_run:
        typer.echo("\nUploading GOLD outputs to S3...")

        ok, fail = upload_dir_to_s3(
            bucket="crashes-data-luis-007",
            local_dir="data/gold",
            prefix="dwh/gold"
        )

        typer.echo(f"Upload finished. Success: {ok} | Fails: {fail}")

app.add_typer(bronze_app, name="bronze")
app.add_typer(silver_app, name="silver")
app.add_typer(gold_app, name="gold")


if __name__ == "__main__":
    app()