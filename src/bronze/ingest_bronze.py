from __future__ import annotations
from typing import Optional
import json

from api.source_nyc.nyc_open_data import NYCOpenDataClient
from src.config import bronze_output_path


def ingest_bronze_dataset(
    dataset: str,
    variant: str,
    app_token: Optional[str] = None,
    run_date: Optional[str] = None,
    limit: int = 50000,
    where: Optional[str] = None,
    order: Optional[str] = None,
) -> str:

    client = NYCOpenDataClient(app_token=app_token)

    out_path = bronze_output_path(
        dataset=dataset,
        variant=variant,
        run_date=run_date,
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", encoding="utf-8") as f:

        for chunk in client.download_json_paged(
            dataset=dataset,
            limit=limit,
            where=where,
            order=order,
        ):

            for row in chunk:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

    return str(out_path)