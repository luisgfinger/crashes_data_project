from __future__ import annotations

from dataclasses import dataclass
from datetime import date
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
   
    df.loc[mask, "dq_reasons"] = df.loc[mask, "dq_reasons"].where(
        df.loc[mask, "dq_reasons"].isna() | (df.loc[mask, "dq_reasons"] == ""),
        df.loc[mask, "dq_reasons"] + ";" + reason,
    )
    df.loc[mask & (df["dq_reasons"].isna() | (df["dq_reasons"] == "")), "dq_reasons"] = reason


def apply_quality_rules_crashes(df: pd.DataFrame, run_date_str: str | None = None) -> DQResult:

    run_date_str = run_date_str or date.today().isoformat()
    out = df.copy()

    out["dq_reasons"] = ""
    out["run_date"] = run_date_str

    discard_mask = pd.Series(False, index=out.index)
    NUMERIC_COLUMNS = [
        "number_of_persons_injured",
        "number_of_persons_killed",
        "number_of_pedestrians_injured",
        "number_of_pedestrians_killed",
        "number_of_cyclist_injured",
        "number_of_cyclist_killed",
        "number_of_motorist_injured",
        "number_of_motorist_killed",
    ]

    for col in NUMERIC_COLUMNS:
        if col in out.columns:
            mask = out[col] < 0
            _append_reason(out, mask, f"invalid_{col}")

    if "crash_date" in out.columns:
        out["crash_date"] = pd.to_datetime(out["crash_date"], errors="coerce")
        today_ts = pd.Timestamp(date.today()).normalize()
        mask = out["crash_date"].isna() | (out["crash_date"] > today_ts)
        _append_reason(out, mask, "invalid_date")
    else:
        _append_reason(out, pd.Series(True, index=out.index), "missing_crash_date")

    if "collision_id" in out.columns:
        discard_mask = out["collision_id"].isna() | (out["collision_id"].astype("string").str.len() == 0)
    else:
        discard_mask = pd.Series(True, index=out.index)

    if discard_mask.any():
        _append_reason(out, discard_mask, "discard_missing_id")

    injured_needed = [
        "number_of_persons_injured",
        "number_of_pedestrians_injured",
        "number_of_cyclist_injured",
        "number_of_motorist_injured",
    ]
    if all(c in out.columns for c in injured_needed):
        injured_subtotal = (
            out["number_of_pedestrians_injured"].fillna(0)
            + out["number_of_cyclist_injured"].fillna(0)
            + out["number_of_motorist_injured"].fillna(0)
        )
        mask = out["number_of_persons_injured"].fillna(0) < injured_subtotal
        _append_reason(out, mask, "inconsistent_persons_injured_breakdown")

    killed_needed = [
        "number_of_persons_killed",
        "number_of_pedestrians_killed",
        "number_of_cyclist_killed",
        "number_of_motorist_killed",
    ]
    if all(c in out.columns for c in killed_needed):
        killed_subtotal = (
            out["number_of_pedestrians_killed"].fillna(0)
            + out["number_of_cyclist_killed"].fillna(0)
            + out["number_of_motorist_killed"].fillna(0)
        )
        mask = out["number_of_persons_killed"].fillna(0) < killed_subtotal
        _append_reason(out, mask, "inconsistent_persons_killed_breakdown")

    discard_df = out[discard_mask].copy()

    has_reasons = out["dq_reasons"].notna() & (out["dq_reasons"].str.len() > 0)
    quarantine_mask = has_reasons & (~discard_mask)
    quarantine_df = out[quarantine_mask].copy()

    clean_mask = (~has_reasons) & (~discard_mask)
    clean_df = out[clean_mask].copy().drop(columns=["dq_reasons"])

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

    if (total_quarantine + total_discard) > 0:
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