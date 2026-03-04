from __future__ import annotations

from datetime import datetime

def parse_start_date_to_where_iso(start_date: str) -> str:

    datetime.strptime(start_date, "%Y-%m-%d")
    return f"{start_date}T00:00:00.000"