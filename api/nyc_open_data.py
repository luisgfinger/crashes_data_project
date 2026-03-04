from __future__ import annotations

import requests
from typing import Optional

NYC_OPEN_DATA_BASE = "https://data.cityofnewyork.us/resource"

DATASETS = {
    "crashes": "h9gi-nx95",
    "vehicles": "bm4k-52h4",
}

class NYCOpenDataClient:
    def __init__(self, app_token: Optional[str] = None, timeout: int = 120):
        self.app_token = app_token
        self.timeout = timeout

    def _headers(self) -> dict:
        headers = {}
        if self.app_token:
            headers["X-App-Token"] = self.app_token
        return headers

    def download_csv_paged(
        self,
        dataset: str,
        limit: int = 50000,
        where: Optional[str] = None,
        order: Optional[str] = None,
    ):
   
        if dataset not in DATASETS:
            raise ValueError(f"Dataset inválido: {dataset}. Use: {list(DATASETS.keys())}")

        four_by_four = DATASETS[dataset]
        url = f"{NYC_OPEN_DATA_BASE}/{four_by_four}.csv"

        offset = 0
        first = True

        while True:
            params = {"$limit": limit, "$offset": offset}
            if where:
                params["$where"] = where
            if order:
                params["$order"] = order

            r = requests.get(url, headers=self._headers(), params=params, timeout=self.timeout)
            r.raise_for_status()

            text = r.text.strip()
            lines = text.splitlines()

            if len(lines) <= 1:
                break

            if first:
                yield "\n".join(lines) + "\n" 
                first = False
            else:
                yield "\n".join(lines[1:]) + "\n"

            offset += limit