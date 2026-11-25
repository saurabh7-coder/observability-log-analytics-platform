#!/usr/bin/env python3
"""
Aggregation step of the local pipeline.

Reads data/logs.jsonl (written by simulator/log_simulator.py) and produces:

  * data/minute_metrics.csv   - per-service, per-minute metrics
  * data/service_summary.csv  - per-service totals for the whole window
  * src/data/metrics.json     - everything the Next.js dashboard needs
                                (totals incl. SLO summary + overall per-minute
                                series + services + per-service timeseries)
  * src/data/log_sample.json  - ~300-log sample for the dashboard's log
                                explorer (mix of normal traffic + ERROR logs
                                from both chaos windows, with trace_ids)

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

# Chaos windows of the default simulator run, as (start, end) minute offsets
# from the window start - used only to guarantee the log sample includes the
# interesting ERROR logs. Keep in sync with simulator/log_simulator.py
# defaults (error burst 200-234, latency degradation 290-319).
CHAOS_WINDOWS = [(200, 235), (290, 320)]
LOG_SAMPLE_SIZE = 300

# SLO target for the dashboard's SLO cards: 99% of requests must succeed,
# i.e. a 1% error budget over the window.
SLO_TARGET_PCT = 99.0


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

    # ---- per-service timeseries (for the dashboard drill-down) --------------
    service_timeseries = {}
    for service, g in minute.groupby("service"):
        g = g.sort_values("minute")
        service_timeseries[service] = [
            {
                "t": row.minute.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "error_rate": float(row.error_rate),
                "p95": float(row.p95_latency_ms),
                "requests": int(row.requests),
            }
            for row in g.itertuples()
        ]

    # ---- log sample for the dashboard's log explorer ------------------------
    # Deterministic mix: ERROR logs from both chaos windows first (capped),
    # then a seeded random sample of the remaining traffic to fill the quota.
    window_start = df["timestamp"].min()
    minute_offset = (df["minute"] - window_start.floor("min")).dt.total_seconds() / 60.0
    in_chaos = minute_offset.apply(
        lambda m: any(start <= m < end for start, end in CHAOS_WINDOWS))
    chaos_errors = df[in_chaos & (df["is_error"] == 1)].head(120)
    rest = df.drop(index=chaos_errors.index)
    filler = rest.sample(
        n=min(LOG_SAMPLE_SIZE - len(chaos_errors), len(rest)), random_state=42)
    sample = (
        pd.concat([chaos_errors, filler])
        .sort_values("timestamp")
        .head(LOG_SAMPLE_SIZE)
    )
    log_fields = ["timestamp", "service", "level", "status_code", "latency_ms",
                  "method", "endpoint", "trace_id", "message"]
    log_sample = [
        {
            "timestamp": row.timestamp.strftime("%Y-%m-%dT%H:%M:%S.") +
                         f"{row.timestamp.microsecond // 1000:03d}Z",
            "service": row.service,
            "level": row.level,
            "status_code": int(row.status_code),
            "latency_ms": float(row.latency_ms),
            "method": row.method,
            "endpoint": row.endpoint,
            "trace_id": row.trace_id,
            "message": row.message,
        }
        for row in sample.itertuples()
    ]
    sample_out = Path(args.json_out).parent / "log_sample.json"
    sample_out.write_text(json.dumps(log_sample, indent=2), encoding="utf-8")

    total_logs = int(len(df))
    total_errors = int(df["is_error"].sum())
    overall_error_rate = round(100.0 * total_errors / total_logs, 3)
    # SLO summary vs the 99% target (1% error budget), computed honestly from
    # the data: remaining = max(0, (1 - error_rate_pct / budget_pct)) * 100.
    error_budget_pct = round(100.0 - SLO_TARGET_PCT, 3)  # 1.0
    error_budget_remaining_pct = round(
        max(0.0, (1.0 - overall_error_rate / error_budget_pct)) * 100.0, 2)
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
            "overall_error_rate": overall_error_rate,
            "overall_p95_ms": round(float(df["latency_ms"].quantile(0.95)), 2),
            "overall_avg_ms": round(float(df["latency_ms"].mean()), 2),
            "anomalies_detected": 0,
            "services_count": int(df["service"].nunique()),
            # SLO summary vs the 99% target (see SLO_TARGET_PCT above).
            "uptime_pct": round(100.0 - overall_error_rate, 3),
            "avg_latency_ms": round(float(df["latency_ms"].mean()), 2),
            "slo_target_pct": SLO_TARGET_PCT,
            "error_budget_pct": error_budget_pct,
            "error_budget_remaining_pct": error_budget_remaining_pct,
            "slo_met": overall_error_rate <= error_budget_pct,
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
        "service_timeseries": service_timeseries,
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
    print(f"Wrote {sample_out} ({len(log_sample)} logs, "
          f"{int(sample['is_error'].sum())} errors) | "
          f"uptime {metrics['totals']['uptime_pct']}% vs SLO {SLO_TARGET_PCT}% "
          f"(error budget remaining: {error_budget_remaining_pct}%)")


if __name__ == "__main__":
    main()
