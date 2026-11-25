#!/usr/bin/env python3
"""
Microservice log simulator for the Observability & Log Analytics Data Platform.

Emits structured JSON logs for 8 fake microservices. Supports:

  * batch mode  - generate a fixed number of simulated minutes, fast, to a file
  * stream mode - keep generating minute-by-minute in real time (for the
                  Docker Compose / Vector -> Kafka -> ClickHouse live stack)

Chaos mode injects incidents into services for configurable windows. TWO
scenarios are enabled by default in batch mode:

  1. Error burst (scenario "error"): checkout-service, minutes 200-234 -
     ~42% of requests fail and latency degrades ~3.8x.
  2. Latency degradation (scenario "latency"): payment-service, minutes
     290-319 - latency degrades ~4x while the error rate is only slightly
     elevated (base + ~4 points). This is the classic "slow, not down"
     incident that an error-rate-only detector would miss; the latency
     detector in pipeline/anomaly_detection.py exists to catch it.

    python3 simulator/log_simulator.py --mode batch --minutes 360 \
        --chaos-service checkout-service --chaos-start 200 --chaos-duration 35 \
        --latency-chaos-service payment-service --latency-chaos-start 290 \
        --latency-chaos-duration 30

Use --no-latency-chaos to disable scenario 2. The defaults (seed 42) are
chosen so the standard demo dataset always contains both incidents for the
anomaly-detection job to find.
"""

import argparse
import json
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

SERVICES = {
    "api-gateway":           {"rpm": 45, "base_error": 0.015, "median_latency": 85},
    "auth-service":          {"rpm": 25, "base_error": 0.012, "median_latency": 45},
    "checkout-service":      {"rpm": 18, "base_error": 0.020, "median_latency": 120},
    "payment-service":       {"rpm": 15, "base_error": 0.025, "median_latency": 150},
    "inventory-service":     {"rpm": 20, "base_error": 0.010, "median_latency": 60},
    "search-service":        {"rpm": 30, "base_error": 0.018, "median_latency": 95},
    "notification-service":  {"rpm": 12, "base_error": 0.008, "median_latency": 40},
    "recommendation-service": {"rpm": 22, "base_error": 0.022, "median_latency": 110},
}

ENDPOINTS = {
    "api-gateway":            ["/api/orders", "/api/products", "/api/users", "/api/cart"],
    "auth-service":           ["/auth/login", "/auth/register", "/auth/refresh", "/auth/logout"],
    "checkout-service":       ["/checkout/start", "/checkout/address", "/checkout/confirm"],
    "payment-service":        ["/payments/charge", "/payments/refund", "/payments/status"],
    "inventory-service":      ["/inventory/check", "/inventory/reserve", "/inventory/release"],
    "search-service":         ["/search", "/search/autocomplete", "/search/filters"],
    "notification-service":   ["/notify/email", "/notify/sms", "/notify/push"],
    "recommendation-service": ["/recommendations", "/recommendations/similar"],
}

METHODS = ["GET", "GET", "GET", "POST", "POST", "PUT", "DELETE"]
OK_STATUSES = [200, 200, 200, 201, 204, 301, 404]
ERROR_STATUSES = [500, 502, 503]

# Default chaos scenarios (both on in batch mode; see module docstring).
DEFAULT_ERROR_CHAOS = {"service": "checkout-service", "start": 200, "duration": 35}
DEFAULT_LATENCY_CHAOS = {"service": "payment-service", "start": 290, "duration": 30}


def chaos_kind(service: str, minute_idx: int, error_chaos, latency_chaos):
    """Return "error", "latency" or None for a service/minute.

    A chaos spec is {"service": ..., "start": minute, "duration": minutes}
    or None (scenario disabled). The error scenario wins if both overlap.
    """
    for kind, spec in (("error", error_chaos), ("latency", latency_chaos)):
        if spec and service == spec["service"] \
                and spec["start"] <= minute_idx < spec["start"] + spec["duration"]:
            return kind
    return None


def make_log(rng: random.Random, ts: datetime, service: str, cfg: dict, chaos=None) -> dict:
    error_rate = cfg["base_error"]
    latency_multiplier = 1.0
    if chaos == "error":
        error_rate = 0.42          # error burst: ~42% of requests fail
        latency_multiplier = 3.8   # ...and the rest get much slower
    elif chaos == "latency":
        # Latency degradation: ~4x slower, errors only slightly elevated
        # (base 2.5% -> ~6.5% for payment-service) - "slow, not down".
        error_rate = min(cfg["base_error"] + 0.04, 0.10)
        latency_multiplier = 4.0

    is_error = rng.random() < error_rate
    if is_error:
        status = rng.choice(ERROR_STATUSES)
        level = "ERROR"
    else:
        status = rng.choice(OK_STATUSES)
        level = "WARN" if status >= 400 else "INFO"

    # Log-normal-ish latency around the service median.
    latency = rng.lognormvariate(0.0, 0.45) * cfg["median_latency"] * latency_multiplier
    if is_error:
        latency *= rng.uniform(1.1, 1.8)

    return {
        "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "service": service,
        "level": level,
        "status_code": status,
        "latency_ms": round(latency, 2),
        "method": rng.choice(METHODS),
        "endpoint": rng.choice(ENDPOINTS[service]),
        "request_id": uuid.UUID(int=rng.getrandbits(128)).hex[:16],
        "trace_id": uuid.UUID(int=rng.getrandbits(128)).hex[:16],
        "message": f"{service} request {'failed' if is_error else 'completed'} "
                   f"with status {status}",
    }


def generate_minute(rng, minute_start: datetime, minute_idx: int,
                    error_chaos=DEFAULT_ERROR_CHAOS,
                    latency_chaos=DEFAULT_LATENCY_CHAOS) -> list:
    logs = []
    for service, cfg in SERVICES.items():
        # Small diurnal-style wobble so traffic is not perfectly flat.
        wobble = 1.0 + 0.25 * ((minute_idx % 60) / 60.0 - 0.5)
        count = max(1, int(round(cfg["rpm"] * wobble * rng.uniform(0.85, 1.15))))
        chaos = chaos_kind(service, minute_idx, error_chaos, latency_chaos)
        for _ in range(count):
            ts = minute_start + timedelta(seconds=rng.uniform(0, 59.999))
            logs.append(make_log(rng, ts, service, cfg, chaos))
    logs.sort(key=lambda x: x["timestamp"])
    return logs


def parse_start_time(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def main():
    p = argparse.ArgumentParser(description="Microservice log simulator")
    p.add_argument("--mode", choices=["batch", "stream"], default="batch")
    p.add_argument("--minutes", type=int, default=360,
                   help="Minutes to generate in batch mode (stream mode: minutes before exiting, 0 = forever)")
    p.add_argument("--out", default="data/logs.jsonl", help="Output JSONL file ('-' for stdout)")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--chaos-service", default="checkout-service", choices=list(SERVICES.keys()))
    p.add_argument("--chaos-start", type=int, default=200, help="Minute index where the error-burst window starts")
    p.add_argument("--chaos-duration", type=int, default=35, help="Error-burst window length in minutes")
    p.add_argument("--latency-chaos-service", default="payment-service", choices=list(SERVICES.keys()),
                   help="Service hit by the latency-degradation scenario")
    p.add_argument("--latency-chaos-start", type=int, default=290,
                   help="Minute index where the latency-degradation window starts")
    p.add_argument("--latency-chaos-duration", type=int, default=30,
                   help="Latency-degradation window length in minutes")
    p.add_argument("--no-latency-chaos", action="store_true",
                   help="Disable the latency-degradation scenario")
    p.add_argument("--interval", type=float, default=1.0,
                   help="Stream mode only: real seconds to wait per simulated minute")
    p.add_argument("--start-time", default="2026-11-25T00:00:00Z",
                   help="UTC start time, format YYYY-MM-DDTHH:MM:SSZ")
    args = p.parse_args()

    rng = random.Random(args.seed)
    start = parse_start_time(args.start_time)

    error_chaos = {"service": args.chaos_service, "start": args.chaos_start,
                   "duration": args.chaos_duration}
    latency_chaos = None if args.no_latency_chaos else {
        "service": args.latency_chaos_service, "start": args.latency_chaos_start,
        "duration": args.latency_chaos_duration}

    out_fh = sys.stdout if args.out == "-" else None
    if out_fh is None:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_fh = open(out_path, "a" if args.mode == "stream" else "w", encoding="utf-8")

    total = 0
    minute_idx = 0
    try:
        while True:
            if args.mode == "batch" and minute_idx >= args.minutes:
                break
            if args.mode == "stream" and args.minutes and minute_idx >= args.minutes:
                break
            logs = generate_minute(rng, start + timedelta(minutes=minute_idx), minute_idx,
                                   error_chaos, latency_chaos)
            for log in logs:
                out_fh.write(json.dumps(log) + "\n")
            out_fh.flush()
            total += len(logs)
            if args.mode == "stream":
                print(f"[simulator] minute {minute_idx}: {len(logs)} logs "
                      f"(total {total})", file=sys.stderr)
                time.sleep(args.interval)
            minute_idx += 1
    except KeyboardInterrupt:
        print("[simulator] stopped", file=sys.stderr)
    finally:
        if out_fh is not sys.stdout:
            out_fh.close()

    latency_desc = ("disabled" if latency_chaos is None else
                    f"{latency_chaos['service']} minutes {latency_chaos['start']}-"
                    f"{latency_chaos['start'] + latency_chaos['duration'] - 1}")
    print(f"Generated {total} logs over {minute_idx} minutes "
          f"(error chaos: {args.chaos_service} minutes {args.chaos_start}-"
          f"{args.chaos_start + args.chaos_duration - 1}; "
          f"latency chaos: {latency_desc})", file=sys.stderr)


if __name__ == "__main__":
    main()
