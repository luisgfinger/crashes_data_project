import typer
from datetime import date, datetime
from enum import Enum
from typing import Callable, Dict, Optional, Tuple

from src.silver.vehicles.v1.run import run as run_silver_vehicles_v1
from src.silver.crashes.v1.run import run as run_silver_crashes_v1

app = typer.Typer(help="crashes-data-project CLI")
silver_app = typer.Typer(help="Run SILVER pipelines")

class Dataset(str, Enum):
    vehicles = "vehicles"
    crashes = "crashes"


class Version(str, Enum):
    v1 = "v1"


class Variant(str, Enum):
    full = "full"
    incremental = "incremental"
    backfill = "backfill"


PipelineFn = Callable[..., None]

PIPELINES: Dict[Tuple[Dataset, Version], PipelineFn] = {
    (Dataset.vehicles, Version.v1): run_silver_vehicles_v1,
    (Dataset.crashes, Version.v1): run_silver_crashes_v1,
}


def _validate_run_date(run_date: Optional[str]) -> str:
   
    run_date_str = run_date or date.today().isoformat()
    try:
        datetime.strptime(run_date_str, "%Y-%m-%d")
    except ValueError as e:
        raise typer.BadParameter("--run-date must be in YYYY-MM-DD format") from e
    return run_date_str


def _available_pipelines_hint() -> str:
    items = sorted([f"silver/{ds.value}/{ver.value}" for (ds, ver) in PIPELINES.keys()])
    return "Available pipelines:\n- " + "\n- ".join(items)


@silver_app.command("run")
def run(
    dataset: Dataset = typer.Option(
        Dataset.vehicles,
        "--dataset",
        "-d",
        help="Which dataset pipeline to run.",
        show_default=True,
    ),
    version: Version = typer.Option(
        Version.v1,
        "--version",
        "-v",
        help="Pipeline version.",
        show_default=True,
    ),
    variant: Variant = typer.Option(
        Variant.full,
        "--variant",
        help="Execution mode (full/incremental/backfill).",
        show_default=True,
    ),
    run_date: Optional[str] = typer.Option(
        None,
        "--run-date",
        help="Run date in YYYY-MM-DD. Defaults to today.",
        show_default=False,
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Simulate execution (no writes).",
        show_default=True,
    ),
):
    run_date_str = _validate_run_date(run_date)

    pipeline = PIPELINES.get((dataset, version))
    if pipeline is None:
        raise typer.BadParameter(
            f"Pipeline not found: silver/{dataset.value}/{version.value}\n\n{_available_pipelines_hint()}"
        )

    pipeline(run_date_str=run_date_str, variant=variant.value, dry_run=dry_run)


app.add_typer(silver_app, name="silver")

if __name__ == "__main__":
    app()