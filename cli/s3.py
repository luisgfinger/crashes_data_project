import typer
from api.s3_upload.s3_upload import upload_dir_to_s3

app = typer.Typer(help="Upload files to AWS S3")


@app.command()
def upload(
    bucket: str = typer.Option("crashes-data-luis-007", "--bucket", "-b"),
    local_dir: str = typer.Option("data/", "--local-dir", "-d"),
    prefix: str = typer.Option("data_lake/", "--prefix", "-p"),
    profile: str = typer.Option("", "--profile"),
):

    typer.echo("Uploading files to S3...")

    ok, fail = upload_dir_to_s3(
        bucket=bucket,
        local_dir=local_dir,
        prefix=prefix,
        profile=profile,
    )

    typer.echo(f"Success: {ok} | Fails: {fail}")