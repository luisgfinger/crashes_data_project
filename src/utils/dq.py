from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable, Tuple

import pandas as pd


@dataclass(frozen=True)
class DQResult:
    clean_df: pd.DataFrame
    quarantine_df: pd.DataFrame
    discard_df: pd.DataFrame
    metrics_summary: pd.DataFrame
    metrics_by_reason: pd.DataFrame
    run_date: str


def _append_reason(df: pd.DataFrame, mask: pd.Series, reason: str) -> None:
    """
    Append a reason to dq_reasons for rows where mask is True.
    dq_reasons is a semi-colon separated string, e.g. "missing_crash_date;missing_collision_id"
    """
    df.loc[mask, "dq_reasons"] = df.loc[mask, "dq_reasons"].where(
        df.loc[mask, "dq_reasons"].isna() | (df.loc[mask, "dq_reasons"] == ""),
        df.loc[mask, "dq_reasons"] + ";" + reason,
    )
    df.loc[mask & (df["dq_reasons"].isna() | (df["dq_reasons"] == "")), "dq_reasons"] = reason


def apply_quality_rules_vehicles(df: pd.DataFrame, run_date_str: str | None = None) -> DQResult:
    """
    Applies Data Quality rules for Vehicles dataset and returns:
      - clean_df: good rows
      - quarantine_df: bad rows (kept for audit)
      - discard_df: useless rows (optional)
      - metrics dataframes
    """
    run_date_str = run_date_str or date.today().isoformat()

    out = df.copy()

    # Create DQ columns
    out["dq_reasons"] = ""
    out["run_date"] = run_date_str

    # ---------- QUARANTINE RULES ----------
    # These rows are not thrown away; they go to quarantine with reasons.

    # crash_datetime missing
    if "crash_datetime" in out.columns:
        mask = out["crash_datetime"].isna()
        _append_reason(out, mask, "missing_crash_datetime")
    else:
        # If pipeline didn't create it, everything is effectively invalid
        _append_reason(out, pd.Series([True] * len(out), index=out.index), "missing_crash_datetime_column")

    # crash_year missing (cannot partition / time-based analysis)
    if "crash_year" in out.columns:
        mask = out["crash_year"].isna()
        _append_reason(out, mask, "missing_crash_year")

    # vehicle_year invalid range (only if present)
    # You can tune the range. Here: 1900..(current_year+1)
    if "vehicle_year" in out.columns:
        current_year = date.today().year
        vy = out["vehicle_year"]
        mask = vy.notna() & ((vy < 1900) | (vy > current_year + 1))
        _append_reason(out, mask, "invalid_vehicle_year_range")

    # ---------- DISCARD RULES (OPTIONAL) ----------
    # Discard means "useless even for quarantine".
    # Keep this minimal in the beginning.
    discard_mask = pd.Series([False] * len(out), index=out.index)

    # Example discard: unique_id or collision_id missing
    if "unique_id" in out.columns or "collision_id" in out.columns:
        discard_mask = out["unique_id"].isna() & out["collision_id"].isna()

    # Mark discard reason (optional)
    if discard_mask.any():
        _append_reason(out, discard_mask, "discard_missing_unique_or_collision_id")

    # ---------- SPLIT ----------
    # discard first
    discard_df = out[discard_mask].copy()

    # quarantine: has any dq_reasons and not discarded
    has_reasons = out["dq_reasons"].notna() & (out["dq_reasons"].str.len() > 0)
    quarantine_mask = has_reasons & (~discard_mask)
    quarantine_df = out[quarantine_mask].copy()

    # clean: no reasons and not discarded
    clean_mask = (~has_reasons) & (~discard_mask)
    clean_df = out[clean_mask].copy()

    # ---------- METRICS ----------
    total_read = len(out)
    total_clean = len(clean_df)
    total_quarantine = len(quarantine_df)
    total_discard = len(discard_df)

    metrics_summary = pd.DataFrame(
        [
            {"run_date": run_date_str, "metric": "total_rows_read", "value": total_read},
            {"run_date": run_date_str, "metric": "total_clean", "value": total_clean},
            {"run_date": run_date_str, "metric": "total_quarantine", "value": total_quarantine},
            {"run_date": run_date_str, "metric": "total_discard", "value": total_discard},
        ]
    )

    # counts by reason (explode "a;b;c" into multiple rows)
    if total_quarantine + total_discard > 0:
        reason_series = out.loc[has_reasons, "dq_reasons"].astype("string")
        exploded = reason_series.str.split(";").explode()
        metrics_by_reason = (
            exploded.value_counts(dropna=True)
            .rename_axis("reason")
            .reset_index(name="count")
        )
        metrics_by_reason.insert(0, "run_date", run_date_str)
    else:
        metrics_by_reason = pd.DataFrame(columns=["run_date", "reason", "count"])

    return DQResult(
        clean_df=clean_df,
        quarantine_df=quarantine_df,
        discard_df=discard_df,
        metrics_summary=metrics_summary,
        metrics_by_reason=metrics_by_reason,
        run_date=run_date_str,
    )