#!/usr/bin/env python3
"""
Aggregation step of the local pipeline.

Reads data/logs.jsonl (written by simulator/log_simulator.py) and produces:

  * data/minute_metrics.csv   - per-service, per-minute metrics
  * data/service_summary.csv  - per-service totals for the whole window
  * src/data/metrics.json     - everything the Next.js dashboard needs
                                (totals + overall per-minute series + services)

In the full Docker stack the same aggregations live in ClickHouse as
materialized views (see clickhouse/schema.sql, mv_service_minute); this script
is the file-based equivalent so the dashboard can be built and deployed to
Vercel without a live ClickHouse instance.

Usage:  python3 pipeline/aggregate.py [--logs data/logs.jsonl]
"""

import argparse
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent


def main():
    p = argparse.ArgumentParser(description="Aggregate raw logs into metrics")
    p.add_argument("--logs", default=str(ROOT / "data" / "logs.jsonl"))
    p.add_argument("--out-dir", default=str(ROOT / "data"))
    p.add_argument("--json-out", default=str(ROOT / "src" / "data" / "metrics.json"))
    args = p.parse_args()

    df = pd.read_json(args.logs, lines=True)
    if df.empty:
        raise SystemExit(f"No logs found in {args.logs}")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["minute"] = df["timestamp"].dt.floor("min")
    df["is_error"] = (df["status_code"] >= 500).astype(int)

    # ---- per-service, per-minute -------------------------------------------
    minute = (
        df.groupby(["minute", "service"])
        .agg(
            requests=("status_code", "size"),
            errors=("is_error", "sum"),
            p95_latency_ms=("latency_ms", lambda s: s.quantile(0.95)),
            avg_latency_ms=("latency_ms", "mean"),
        )
        .reset_index()
    )
    minute["error_rate"] = (100.0 * minute["errors"] / minute["requests"]).round(3)
    minute["p95_latency_ms"] = minute["p95_latency_ms"].round(2)
    minute["avg_latency_ms"] = minute["avg_latency_ms"].round(2)
    minute_out = minute.copy()
    minute_out["minute"] = minute_out["minute"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    minute_out = minute_out.rename(columns={"minute": "timestamp"})
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    minute_out.to_csv(out_dir / "minute_metrics.csv", index=False)

    # ---- per-service summary ------------------------------------------------
    summary = (
        df.groupby("service")
        .agg(
            requests=("status_code", "size"),
            errors=("is_error", "sum"),
            p95_latency_ms=("latency_ms", lambda s: s.quantile(0.95)),
            avg_latency_ms=("latency_ms", "mean"),
        )
        .reset_index()
        .sort_values("requests", ascending=False)
    )
    summary["error_rate"] = (100.0 * summary["errors"] / summary["requests"]).round(3)
    summary["p95_latency_ms"] = summary["p95_latency_ms"].round(2)
    summary["avg_latency_ms"] = summary["avg_latency_ms"].round(2)
    summary.to_csv(out_dir / "service_summary.csv", index=False)

    # ---- overall per-minute series (for the dashboard charts) ---------------
    overall = (
        df.groupby("minute")
        .agg(
            requests=("status_code", "size"),
            errors=("is_error", "sum"),
            p95_latency_ms=("latency_ms", lambda s: s.quantile(0.95)),
            avg_latency_ms=("latency_ms", "mean"),
        )
        .reset_index()
        .sort_values("minute")
    )
    overall["error_rate"] = (100.0 * overall["errors"] / overall["requests"]).round(3)
    overall["p95_latency_ms"] = overall["p95_latency_ms"].round(2)
    overall["avg_latency_ms"] = overall["avg_latency_ms"].round(2)

    total_logs = int(len(df))
    total_errors = int(df["is_error"].sum())
    metrics = {
        "generated_from": "data/logs.jsonl via pipeline/aggregate.py",
        "window": {
            "start": df["timestamp"].min().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end": df["timestamp"].max().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "minutes": int(df["minute"].nunique()),
        },
        "totals": {
            "total_logs": total_logs,
            "total_errors": total_errors,
            "overall_error_rate": round(100.0 * total_errors / total_logs, 3),
            "overall_p95_ms": round(float(df["latency_ms"].quantile(0.95)), 2),
            "overall_avg_ms": round(float(df["latency_ms"].mean()), 2),
            "anomalies_detected": 0,
            "services_count": int(df["service"].nunique()),
        },
        "series": [
            {
                "timestamp": row.minute.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "requests": int(row.requests),
                "errors": int(row.errors),
                "error_rate": float(row.error_rate),
                "p95_latency_ms": float(row.p95_latency_ms),
                "avg_latency_ms": float(row.avg_latency_ms),
            }
            for row in overall.itertuples()
        ],
        "services": [
            {
                "service": row.service,
                "requests": int(row.requests),
                "errors": int(row.errors),
                "error_rate": float(row.error_rate),
                "p95_latency_ms": float(row.p95_latency_ms),
                "avg_latency_ms": float(row.avg_latency_ms),
            }
            for row in summary.itertuples()
        ],
        "anomalies": [],
    }

    json_out = Path(args.json_out)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    print(f"Logs: {total_logs} | errors: {total_errors} "
          f"({metrics['totals']['overall_error_rate']}%) | "
          f"p95: {metrics['totals']['overall_p95_ms']} ms | "
          f"services: {metrics['totals']['services_count']}")
    print(f"Wrote {out_dir / 'minute_metrics.csv'}, {out_dir / 'service_summary.csv'}, {json_out}")


if __name__ == "__main__":
    main()
