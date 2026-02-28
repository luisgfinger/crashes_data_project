import typer
from datetime import date
from typing import Optional

from src.silver.vehicles.v1.run import run as run_silver_vehicles_v1

app = typer.Typer(help="crashes-data-project CLI")
silver_app = typer.Typer(help="Run SILVER pipelines")

@silver_app.command("run")
def run(
    dataset: str = typer.Option("vehicles", "--dataset", "-d"),
    version: str = typer.Option("v1", "--version", "-v"),
    variant: str = typer.Option("full", "--variant"),
    run_date: Optional[str] = typer.Option(None, "--run-date"),
    dry_run: bool = typer.Option(False, "--dry-run"),
):
    run_date_str = run_date or date.today().isoformat()

    if dataset == "vehicles" and version == "v1":
        run_silver_vehicles_v1(run_date_str=run_date_str, variant=variant, dry_run=dry_run)
        return

    raise typer.BadParameter(f"Pipeline não encontrada: silver/{dataset}/{version}")

app.add_typer(silver_app, name="silver")

if __name__ == "__main__":
    app()