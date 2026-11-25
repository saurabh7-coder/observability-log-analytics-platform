"""Tests for simulator/log_simulator.py.

Small, fast, synthetic: each test generates a handful of minutes with a
fixed seed instead of running the full 360-minute dataset.
"""
import random
from datetime import datetime, timedelta, timezone

import log_simulator as sim

START = datetime(2026, 1, 1, tzinfo=timezone.utc)
REQUIRED_FIELDS = {
    "timestamp": str, "service": str, "level": str, "status_code": int,
    "latency_ms": (int, float), "method": str, "endpoint": str,
    "trace_id": str, "message": str,
}


def generate_range(service, first, last, seed=42):
    """All logs for one service over minutes [first, last) with one rng."""
    rng = random.Random(seed)
    logs = []
    for idx in range(first, last):
        minute_logs = sim.generate_minute(rng, START + timedelta(minutes=idx), idx)
        logs.extend(l for l in minute_logs if l["service"] == service)
    return logs


def error_rate(logs):
    return sum(1 for l in logs if l["status_code"] >= 500) / len(logs)


def p95(values):
    ordered = sorted(values)
    return ordered[int(0.95 * (len(ordered) - 1))]


def test_deterministic_with_seed():
    a = sim.generate_minute(random.Random(42), START, 10)
    b = sim.generate_minute(random.Random(42), START, 10)
    assert a == b
    assert len(a) > 100  # a full minute across all 8 services


def test_log_schema():
    logs = sim.generate_minute(random.Random(1), START, 0)
    assert logs, "expected logs"
    for log in logs:
        for field, types in REQUIRED_FIELDS.items():
            assert field in log, f"missing field {field}"
            assert isinstance(log[field], types), f"{field} has wrong type"
        assert log["service"] in sim.SERVICES
        assert log["level"] in ("INFO", "WARN", "ERROR")
        assert log["latency_ms"] > 0
        assert len(log["trace_id"]) == 16


def test_chaos_kind_windows():
    assert sim.chaos_kind("checkout-service", 210, sim.DEFAULT_ERROR_CHAOS,
                          sim.DEFAULT_LATENCY_CHAOS) == "error"
    assert sim.chaos_kind("payment-service", 300, sim.DEFAULT_ERROR_CHAOS,
                          sim.DEFAULT_LATENCY_CHAOS) == "latency"
    assert sim.chaos_kind("checkout-service", 100, sim.DEFAULT_ERROR_CHAOS,
                          sim.DEFAULT_LATENCY_CHAOS) is None
    assert sim.chaos_kind("payment-service", 300, sim.DEFAULT_ERROR_CHAOS,
                          None) is None  # latency scenario disabled


def test_error_chaos_raises_checkout_error_rate():
    baseline = generate_range("checkout-service", 100, 120)
    chaos = generate_range("checkout-service", 200, 220)
    assert error_rate(chaos) > error_rate(baseline) + 0.15
    assert error_rate(chaos) > 0.30  # design target ~42%


def test_latency_chaos_raises_payment_p95_with_slight_errors():
    baseline = generate_range("payment-service", 100, 120)
    chaos = generate_range("payment-service", 290, 310)
    base_p95 = p95([l["latency_ms"] for l in baseline])
    chaos_p95 = p95([l["latency_ms"] for l in chaos])
    assert chaos_p95 > 2.0 * base_p95  # design target ~4x
    # "Slow, not down": errors rise only slightly, nowhere near the 42% burst.
    assert error_rate(chaos) < 0.20
    assert error_rate(chaos) > error_rate(baseline)
