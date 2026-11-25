#!/usr/bin/env python3
"""
Nightly anomaly detection (also run by the Airflow DAG in airflow/dags/).

Two detectors, both comparing every minute against the rolling baseline of
the *previous* 30 minutes for the same service (baseline is shifted by one
minute so a burst does not contaminate its own baseline):

1. Error-rate detector (type="error_rate")
       z_score = (error_rate - baseline_mean) / baseline_std  >=  3.0
       AND requests >= 10          (min-requests guard against tiny samples)
       AND error_rate >= 20%       (absolute floor - tuning note below)

2. Latency detector (type="latency")
       z_score = (p95_latency - baseline_mean) / baseline_std  >=  3.0
       AND requests >= 10
       AND p95_latency > 2.5 x rolling baseline *median* p95
   The median-ratio floor is the latency analogue of the error-rate floor:
   p95 is jumpy at 12-45 requests/minute, so a z-score alone flags noise.
   Tuning: the floor started at 1.8x and flagged 7 extra minutes on
   low-traffic services (auth, notification, search), whose per-minute p95
   is dominated by single-request lognormal tails - a lone slow request can
   double a 15-request minute's p95. At 2.5x every remaining latency flag
   sits inside a real chaos window (the injected degradations run at
   ~3.8-4x baseline p95), and the noise flags are gone. The latency
   detector also catches the checkout error burst, whose latency degrades
   ~3.8x alongside its errors - that overlap is real, not double-counting:
   the same incident hurts both signals.

Tuning note (error rate): with only 12-45 requests per service-minute, 1-2
chance errors swing a minute's error rate by 7-14 points and produced ~71
raw z-score flags on the seed-42 dataset, almost all noise. Adding the 20%
absolute floor (a real incident burst sits at ~42%) keeps every genuine
chaos-window detection while cutting the false positives to a handful -
see README. One honest side effect, kept deliberately: the latency
scenario's slightly elevated error rate (~6.5% design, on ~15
requests/minute) occasionally spikes a single minute to 20-25% by chance,
and the error detector flags those minutes too (3 on the seed-42 run).
The measurements are real; they are also a textbook small-sample lesson,
and they are why the latency detector exists - see README.

Threshold 3.0 is the classic "3 sigma" rule: tight enough that normal noise
rarely crosses it (~0.1% of minutes by chance), loose enough that a real
incident crosses it immediately. See README for the false-positive /
false-negative discussion.

In the output rows, baseline_mean / baseline_std are in the flagged
metric's own units (% for error_rate anomalies, ms for latency anomalies).

Reads  data/minute_metrics.csv      (from pipeline/aggregate.py)
Writes data/anomalies.csv           (with a `type` column)
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
LATENCY_MEDIAN_MULT = 2.5  # p95 must exceed 2.5x the rolling baseline median (tuned - see docstring)

ANOMALY_COLUMNS = [
    "timestamp", "service", "type", "error_rate", "p95_latency_ms",
    "baseline_mean", "baseline_std", "z_score", "requests", "errors",
]


def _rolling_baseline(series: pd.Series, window: int):
    """(mean, std, median) of the previous `window` values, shifted by 1."""
    shifted = series.shift(1)
    mean = shifted.rolling(window, min_periods=MIN_HISTORY).mean()
    std = shifted.rolling(window, min_periods=MIN_HISTORY).std()
    median = shifted.rolling(window, min_periods=MIN_HISTORY).median()
    return mean, std, median


def detect_from_frame(df: pd.DataFrame, window: int = 30,
                      threshold: float = 3.0) -> pd.DataFrame:
    """Run both detectors over a per-service/per-minute metrics frame.

    Expected columns: timestamp, service, requests, errors, error_rate,
    p95_latency_ms. Returns a DataFrame with ANOMALY_COLUMNS, sorted by
    z_score descending.
    """
    df = df.sort_values(["service", "timestamp"]).reset_index(drop=True)
    flagged = []

    for service, g in df.groupby("service"):
        g = g.reset_index(drop=True)
        err_mean, err_std, _ = _rolling_baseline(g["error_rate"], window)
        lat_mean, lat_std, lat_median = _rolling_baseline(g["p95_latency_ms"], window)

        for i in range(len(g)):
            if g["requests"].iloc[i] < MIN_REQUESTS:
                continue
            base_row = {
                "timestamp": pd.Timestamp(g["timestamp"].iloc[i]).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "service": service,
                "error_rate": float(g["error_rate"].iloc[i]),
                "p95_latency_ms": float(g["p95_latency_ms"].iloc[i]),
                "requests": int(g["requests"].iloc[i]),
                "errors": int(g["errors"].iloc[i]),
            }

            # --- error-rate detector -----------------------------------
            mean, std = err_mean.iloc[i], err_std.iloc[i]
            if pd.notna(mean) and pd.notna(std) and std > 0 \
                    and g["error_rate"].iloc[i] >= MIN_ERROR_RATE:
                z = (g["error_rate"].iloc[i] - mean) / std
                if z >= threshold:
                    flagged.append({**base_row, "type": "error_rate",
                                    "baseline_mean": round(float(mean), 3),
                                    "baseline_std": round(float(std), 3),
                                    "z_score": round(float(z), 2)})

            # --- latency detector --------------------------------------
            mean, std, median = lat_mean.iloc[i], lat_std.iloc[i], lat_median.iloc[i]
            if pd.notna(mean) and pd.notna(std) and std > 0 and pd.notna(median) \
                    and g["p95_latency_ms"].iloc[i] > LATENCY_MEDIAN_MULT * median:
                z = (g["p95_latency_ms"].iloc[i] - mean) / std
                if z >= threshold:
                    flagged.append({**base_row, "type": "latency",
                                    "baseline_mean": round(float(mean), 3),
                                    "baseline_std": round(float(std), 3),
                                    "z_score": round(float(z), 2)})

    out = pd.DataFrame(flagged, columns=ANOMALY_COLUMNS)
    return out.sort_values("z_score", ascending=False).reset_index(drop=True) \
        if not out.empty else out


def detect(metrics_csv: Path, window: int, threshold: float) -> pd.DataFrame:
    df = pd.read_csv(metrics_csv, parse_dates=["timestamp"])
    return detect_from_frame(df, window=window, threshold=threshold)


def main():
    p = argparse.ArgumentParser(description="Rolling z-score anomaly detection (error rate + latency)")
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
        counts = anomalies["type"].value_counts().to_dict()
        print(f"By type: {counts}")
        for kind in ("error_rate", "latency"):
            sub = anomalies[anomalies["type"] == kind]
            if not sub.empty:
                top = sub.iloc[0]
                metric = (f"error_rate={top['error_rate']}%" if kind == "error_rate"
                          else f"p95={top['p95_latency_ms']} ms")
                print(f"Top {kind} anomaly: {top['service']} at {top['timestamp']} "
                      f"{metric} z={top['z_score']}")

    json_path = Path(args.json)
    if json_path.exists():
        metrics = json.loads(json_path.read_text(encoding="utf-8"))
        metrics["anomalies"] = anomalies.to_dict(orient="records")
        metrics["totals"]["anomalies_detected"] = int(len(anomalies))
        if not anomalies.empty:
            counts = anomalies["type"].value_counts().to_dict()
            metrics["totals"]["anomalies_error_rate"] = int(counts.get("error_rate", 0))
            metrics["totals"]["anomalies_latency"] = int(counts.get("latency", 0))
        metrics["anomaly_detection"] = {
            "method": f"rolling {args.window}-min z-score on per-service error rate and p95 latency",
            "threshold": args.threshold,
            "min_requests_per_minute": MIN_REQUESTS,
            "min_error_rate_pct": MIN_ERROR_RATE,
            "latency_median_multiplier": LATENCY_MEDIAN_MULT,
            "types": ["error_rate", "latency"],
        }
        json_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
        print(f"Merged anomalies into {json_path}")
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
