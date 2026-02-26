import shutil
import pandas as pd

def _write_metrics_csv(metrics_path, metrics_summary: pd.DataFrame, metrics_by_reason: pd.DataFrame) -> None:

    if metrics_path.exists():
        shutil.rmtree(metrics_path)
    metrics_path.mkdir(parents=True, exist_ok=True)

    summary_csv = metrics_summary.copy()
    if "reason" not in summary_csv.columns:
        summary_csv["reason"] = pd.NA
    if "count" not in summary_csv.columns:
        summary_csv["count"] = pd.NA

    if metrics_by_reason is None or metrics_by_reason.empty:
        reasons_csv = pd.DataFrame(columns=["run_date", "metric", "value", "reason", "count"])
    else:
        reasons_csv = metrics_by_reason.copy()
        reasons_csv["metric"] = "dq_reason_count"
        reasons_csv["value"] = reasons_csv["count"]
        reasons_csv = reasons_csv[["run_date", "metric", "value", "reason", "count"]]

    if "metric" in summary_csv.columns and "value" in summary_csv.columns:
        summary_csv = summary_csv[["run_date", "metric", "value", "reason", "count"]]
    else:
        raise ValueError("metrics_summary must contain columns: run_date, metric, value")

    report = pd.concat([summary_csv, reasons_csv], ignore_index=True)
    report.to_csv(metrics_path / "metrics.csv", index=False)