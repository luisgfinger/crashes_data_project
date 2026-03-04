import pandas as pd

from src.utils.io_utils import _rmtree_force

def _write_metrics_json(metrics_path, metrics_summary: pd.DataFrame, metrics_by_reason: pd.DataFrame) -> None:

    if metrics_path.exists():
        _rmtree_force(metrics_path)
    metrics_path.mkdir(parents=True, exist_ok=True)

    summary_json = metrics_summary.copy()
    if "reason" not in summary_json.columns:
        summary_json["reason"] = pd.NA
    if "count" not in summary_json.columns:
        summary_json["count"] = pd.NA

    if metrics_by_reason is None or metrics_by_reason.empty:
        reasons_json = pd.DataFrame(columns=["run_date", "metric", "value", "reason", "count"])
    else:
        reasons_json = metrics_by_reason.copy()
        reasons_json["metric"] = "dq_reason_count"
        reasons_json["value"] = reasons_json["count"]
        reasons_json = reasons_json[["run_date", "metric", "value", "reason", "count"]]

    if "metric" in summary_json.columns and "value" in summary_json.columns:
        summary_json = summary_json[["run_date", "metric", "value", "reason", "count"]]
    else:
        raise ValueError("metrics_summary must contain columns: run_date, metric, value")

    report = pd.concat([summary_json, reasons_json], ignore_index=True)
    report.to_json(metrics_path / "metrics.json", index=False)