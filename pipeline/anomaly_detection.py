#!/usr/bin/env python3
"""
Nightly anomaly detection (also run by the Airflow DAG in airflow/dags/).

Method: for each service, compare every minute's error rate against the
rolling baseline of the *previous* 30 minutes (mean and standard deviation).
A minute is flagged when:

    z_score = (error_rate - baseline_mean) / baseline_std  >=  3.0
    AND requests >= 10          (min-requests guard against tiny samples)
    AND error_rate >= 20%       (absolute floor - tuning note below)
    AND baseline has >= 10 minutes of history

Tuning note: with only 12-45 requests per service-minute, 1-2 chance errors
swing a minute's error rate by 7-14 points and produced ~71 raw z-score
flags on the seed-42 dataset, almost all noise. Adding the 20% absolute
floor (a real incident burst sits at ~42%) keeps every genuine chaos-window
detection while cutting the false positives to a handful - see README.

Threshold 3.0 is the classic "3 sigma" rule: tight enough that normal noise
rarely crosses it (~0.1% of minutes by chance), loose enough that a real
error burst (2% -> 40%+) crosses it immediately. See README for the
false-positive / false-negative discussion.

Reads  data/minute_metrics.csv      (from pipeline/aggregate.py)
Writes data/anomalies.csv
Merges anomalies + totals.anomalies_detected into src/data/metrics.json
(In the Docker stack the same rows are inserted into the ClickHouse
`anomalies` table - see clickhouse/schema.sql.)

Usage:  python3 pipeline/anomaly_detection.py [--threshold 3.0] [--window 30]
"""

import argparse
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
MIN_REQUESTS = 10
MIN_HISTORY = 10
MIN_ERROR_RATE = 20.0  # absolute floor (%) - suppresses small-sample noise


def detect(metrics_csv: Path, window: int, threshold: float) -> pd.DataFrame:
    df = pd.read_csv(metrics_csv, parse_dates=["timestamp"])
    df = df.sort_values(["service", "timestamp"]).reset_index(drop=True)

    flagged = []
    for service, g in df.groupby("service"):
        g = g.reset_index(drop=True)
        # Baseline = previous `window` minutes only (shift by 1 so a burst
        # does not contaminate its own baseline in its first minutes).
        baseline_mean = g["error_rate"].shift(1).rolling(window, min_periods=MIN_HISTORY).mean()
        baseline_std = g["error_rate"].shift(1).rolling(window, min_periods=MIN_HISTORY).std()
        for i in range(len(g)):
            mean, std = baseline_mean.iloc[i], baseline_std.iloc[i]
            if pd.isna(mean) or pd.isna(std) or std <= 0:
                continue
            if g["requests"].iloc[i] < MIN_REQUESTS:
                continue
            if g["error_rate"].iloc[i] < MIN_ERROR_RATE:
                continue
            z = (g["error_rate"].iloc[i] - mean) / std
            if z >= threshold:
                flagged.append({
                    "timestamp": g["timestamp"].iloc[i].strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "service": service,
                    "error_rate": float(g["error_rate"].iloc[i]),
                    "baseline_mean": round(float(mean), 3),
                    "baseline_std": round(float(std), 3),
                    "z_score": round(float(z), 2),
                    "requests": int(g["requests"].iloc[i]),
                    "errors": int(g["errors"].iloc[i]),
                    "p95_latency_ms": float(g["p95_latency_ms"].iloc[i]),
                })
    out = pd.DataFrame(flagged, columns=[
        "timestamp", "service", "error_rate", "baseline_mean", "baseline_std",
        "z_score", "requests", "errors", "p95_latency_ms",
    ])
    return out.sort_values("z_score", ascending=False).reset_index(drop=True) if not out.empty else out


def main():
    p = argparse.ArgumentParser(description="Rolling z-score anomaly detection")
    p.add_argument("--metrics", default=str(ROOT / "data" / "minute_metrics.csv"))
    p.add_argument("--out", default=str(ROOT / "data" / "anomalies.csv"))
    p.add_argument("--json", default=str(ROOT / "src" / "data" / "metrics.json"))
    p.add_argument("--window", type=int, default=30, help="Rolling baseline window in minutes")
    p.add_argument("--threshold", type=float, default=3.0, help="z-score threshold")
    args = p.parse_args()

    anomalies = detect(Path(args.metrics), args.window, args.threshold)
    anomalies.to_csv(args.out, index=False)
    print(f"Anomalies detected: {len(anomalies)}")
    if not anomalies.empty:
        top = anomalies.iloc[0]
        print(f"Top anomaly: {top['service']} at {top['timestamp']} "
              f"error_rate={top['error_rate']}% z={top['z_score']}")

    json_path = Path(args.json)
    if json_path.exists():
        metrics = json.loads(json_path.read_text(encoding="utf-8"))
        metrics["anomalies"] = anomalies.to_dict(orient="records")
        metrics["totals"]["anomalies_detected"] = int(len(anomalies))
        metrics["anomaly_detection"] = {
            "method": f"rolling {args.window}-min z-score on per-service error rate",
            "threshold": args.threshold,
            "min_requests_per_minute": MIN_REQUESTS,
            "min_error_rate_pct": MIN_ERROR_RATE,
        }
        json_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
        print(f"Merged anomalies into {json_path}")
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
