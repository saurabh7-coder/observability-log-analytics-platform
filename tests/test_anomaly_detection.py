"""Tests for pipeline/anomaly_detection.py.

Synthetic per-minute metrics only - no full dataset needed. A flat series
must flag nothing; a series with an injected error spike and an injected
latency spike must flag each with the right `type`.
"""
from datetime import datetime, timedelta, timezone

import pandas as pd

import anomaly_detection as ad

START = datetime(2026, 1, 1, tzinfo=timezone.utc)


def make_frame(minutes=120, error_spike_minute=None, latency_spike_minute=None):
    rows = []
    for i in range(minutes):
        error_rate = 2.0 if i % 2 == 0 else 4.0      # baseline ~3% +/- 1
        p95 = 190.0 if i % 2 == 0 else 210.0         # baseline ~200ms +/- 10
        if i == error_spike_minute:
            error_rate = 45.0
        if i == latency_spike_minute:
            p95 = 1200.0
        rows.append({
            "timestamp": (START + timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "service": "svc-a",
            "requests": 50,
            "errors": round(error_rate / 100 * 50),
            "error_rate": error_rate,
            "p95_latency_ms": p95,
            "avg_latency_ms": p95 / 2,
        })
    return pd.DataFrame(rows)


def test_flat_series_flags_nothing():
    df = make_frame()
    df["error_rate"] = 3.0
    df["p95_latency_ms"] = 200.0
    result = ad.detect_from_frame(df, window=30, threshold=3.0)
    assert result.empty


def test_injected_spikes_are_flagged_with_types():
    df = make_frame(error_spike_minute=60, latency_spike_minute=90)
    result = ad.detect_from_frame(df, window=30, threshold=3.0)
    assert not result.empty
    assert "type" in result.columns

    err = result[result["type"] == "error_rate"]
    lat = result[result["type"] == "latency"]
    assert len(err) == 1 and err.iloc[0]["timestamp"] == "2026-01-01T01:00:00Z"
    assert len(lat) == 1 and lat.iloc[0]["timestamp"] == "2026-01-01T01:30:00Z"
    assert err.iloc[0]["z_score"] >= 3.0
    assert lat.iloc[0]["z_score"] >= 3.0


def test_detect_reads_csv(tmp_path):
    csv_path = tmp_path / "minute_metrics.csv"
    make_frame(error_spike_minute=60, latency_spike_minute=90).to_csv(csv_path, index=False)
    result = ad.detect(csv_path, window=30, threshold=3.0)
    assert set(result["type"]) == {"error_rate", "latency"}
    assert list(result.columns) == ad.ANOMALY_COLUMNS
