import typer
from datetime import date, datetime
from enum import Enum
from typing import Callable, Dict, Optional

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


class Variant(str, Enum):
    full = "full"
    incremental = "incremental"
    backfill = "backfill"


class GoldDataset(str, Enum):
    data_lake = "data_lake"


PipelineFn = Callable[..., None]

BRONZE_PIPELINES: Dict[Dataset, PipelineFn] = {
    Dataset.vehicles: run_bronze_vehicles_v1,
    Dataset.crashes: run_bronze_crashes_v1,
}

SILVER_PIPELINES: Dict[Dataset, PipelineFn] = {
    Dataset.vehicles: run_silver_vehicles_v1,
    Dataset.crashes: run_silver_crashes_v1,
}

GOLD_PIPELINES: Dict[GoldDataset, PipelineFn] = {
    GoldDataset.data_lake: run_gold_v1,
}


S3_BUCKET_DEFAULT = "crashes-data-luis-007"
S3_PREFIX_ROOT_DEFAULT = "data_lake"


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


def _upload_dataset_to_s3_if_enabled(
    *,
    layer: str,
    dataset: str,
    dry_run: bool,
    bucket: str,
    prefix_root: str,
    profile: str,
) -> None:
    if dry_run:
        return

    typer.echo(f"\nUploading {layer.upper()} dataset '{dataset}' outputs to S3...")

    ok, fail = upload_dir_to_s3(
        bucket=bucket,
        local_dir=f"data/{layer}/{dataset}",
        prefix=f"{prefix_root}/{layer}/{dataset}",
        profile=profile,
    )

    typer.echo(f"Upload finished. Success: {ok} | Fails: {fail}")


def _upload_layer_to_s3_if_enabled(
    *,
    layer: str,
    dry_run: bool,
    bucket: str,
    prefix_root: str,
    profile: str,
) -> None:
    if dry_run:
        return

    typer.echo(f"\nUploading {layer.upper()} outputs to S3...")

    ok, fail = upload_dir_to_s3(
        bucket=bucket,
        local_dir=f"data/{layer}",
        prefix=f"{prefix_root}/{layer}",
        profile=profile,
    )

    typer.echo(f"Upload finished. Success: {ok} | Fails: {fail}")


@bronze_app.command("run")
def run_bronze(
    dataset: Dataset = typer.Option(Dataset.vehicles, "--dataset", "-d", show_default=True),
    variant: Variant = typer.Option(Variant.full, "--variant", show_default=True),
    start_date: str = typer.Option("2023-01-01", "--start-date", show_default=True),
    run_date: Optional[str] = typer.Option(None, "--run-date"),
    dry_run: bool = typer.Option(False, "--dry-run", show_default=True),
    bucket: str = typer.Option(S3_BUCKET_DEFAULT, "--bucket", help="S3 bucket name.", show_default=True),
    prefix_root: str = typer.Option(S3_PREFIX_ROOT_DEFAULT, "--prefix-root", help="Root prefix in S3.", show_default=True),
    profile: str = typer.Option("", "--profile", help="AWS profile name (optional).", show_default=False),
):
    run_date_str = _validate_run_date(run_date)
    start_date_str = _validate_start_date(start_date)

    pipeline = BRONZE_PIPELINES.get(dataset)
    if pipeline is None:
        raise typer.BadParameter(f"Pipeline not found: bronze/{dataset.value}")

    pipeline(run_date_str=run_date_str, variant=variant.value, dry_run=dry_run, start_date=start_date_str)

    _upload_dataset_to_s3_if_enabled(
        layer="bronze",
        dataset=dataset.value,
        dry_run=dry_run,
        bucket=bucket,
        prefix_root=prefix_root,
        profile=profile,
    )


@silver_app.command("run")
def run_silver(
    dataset: Dataset = typer.Option(Dataset.vehicles, "--dataset", "-d", show_default=True),
    variant: Variant = typer.Option(Variant.full, "--variant", show_default=True),
    run_date: Optional[str] = typer.Option(None, "--run-date"),
    dry_run: bool = typer.Option(False, "--dry-run", show_default=True),
    bucket: str = typer.Option(S3_BUCKET_DEFAULT, "--bucket", help="S3 bucket name.", show_default=True),
    prefix_root: str = typer.Option(S3_PREFIX_ROOT_DEFAULT, "--prefix-root", help="Root prefix in S3.", show_default=True),
    profile: str = typer.Option("", "--profile", help="AWS profile name (optional).", show_default=False),
):
    run_date_str = _validate_run_date(run_date)

    pipeline = SILVER_PIPELINES.get(dataset)
    if pipeline is None:
        raise typer.BadParameter(f"Pipeline not found: silver/{dataset.value}")

    pipeline(run_date_str=run_date_str, variant=variant.value, dry_run=dry_run)

    _upload_dataset_to_s3_if_enabled(
        layer="silver",
        dataset=dataset.value,
        dry_run=dry_run,
        bucket=bucket,
        prefix_root=prefix_root,
        profile=profile,
    )


@gold_app.command("run")
def run_gold(
    dataset: GoldDataset = typer.Option(GoldDataset.data_lake, "--dataset", "-d", show_default=True),
    variant: Variant = typer.Option(Variant.full, "--variant", show_default=True),
    run_date: Optional[str] = typer.Option(None, "--run-date"),
    dry_run: bool = typer.Option(False, "--dry-run", show_default=True),
    bucket: str = typer.Option(S3_BUCKET_DEFAULT, "--bucket", help="S3 bucket name.", show_default=True),
    prefix_root: str = typer.Option(S3_PREFIX_ROOT_DEFAULT, "--prefix-root", help="Root prefix in S3.", show_default=True),
    profile: str = typer.Option("", "--profile", help="AWS profile name (optional).", show_default=False),
):
    run_date_str = _validate_run_date(run_date)

    pipeline = GOLD_PIPELINES.get(dataset)
    if pipeline is None:
        raise typer.BadParameter(f"Pipeline not found: gold/{dataset.value}")

    pipeline(run_date_str=run_date_str, variant=variant.value, dry_run=dry_run)

    _upload_layer_to_s3_if_enabled(
        layer="gold",
        dry_run=dry_run,
        bucket=bucket,
        prefix_root=prefix_root,
        profile=profile,
    )


app.add_typer(bronze_app, name="bronze")
app.add_typer(silver_app, name="silver")
app.add_typer(gold_app, name="gold")


if __name__ == "__main__":
    app()