# Changes — enhancements round

All enhancements implemented and verified locally (pipeline re-run, pytest,
production build + smoke test). Verified results: 67,199 logs, 1.963% error
rate, p95 229.76 ms, 16 anomalies (6 error_rate + 10 latency), 8/8 tests pass.

## Created

- `tests/conftest.py` — sys.path setup for simulator/pipeline imports
- `tests/test_simulator.py` — determinism, log schema, chaos-kind windows,
  error-burst and latency-degradation scenario tests (5 tests)
- `tests/test_anomaly_detection.py` — flat series flags nothing; injected
  error/latency spikes flagged with correct `type`; CSV entry point (3 tests)
- `.github/workflows/ci.yml` — CI on push/PR: python job (requirements +
  pytest) and node job (npm ci + npm run build)
- `src/app/api/logs/route.js` — serves the log sample for the log explorer
- `src/data/log_sample.json` — generated 300-log sample (124 ERROR logs,
  incl. chaos-window errors with trace IDs)
- `CHANGES.md` — this file

## Modified

- `simulator/log_simulator.py` — second chaos scenario: payment-service
  latency degradation (minutes 290–319, ~4× latency, errors only slightly
  elevated), default-on in batch mode, `--latency-chaos-*` / `--no-latency-chaos`
  flags; `chaos_kind()` helper; `generate_minute()` now takes scenario specs
- `pipeline/anomaly_detection.py` — new latency detector (rolling 30-min
  z-score on p95, ≥10 requests, p95 > 2.5× rolling median floor, tuned from
  1.8× — see docstring); every anomaly tagged `type`; `detect_from_frame()`
  refactor for testability; totals gain per-type counts
- `pipeline/aggregate.py` — exports `service_timeseries`, writes
  `src/data/log_sample.json`, adds SLO summary to totals (uptime %,
  slo_target 99%, error budget remaining)
- `src/app/page.js` — service drill-down (row click + selector), live stream
  mode (1.5 s simulated points + live log feed), log explorer (service/level
  filters + search), SLO cards row, anomaly `type` column; replay kept
- `src/app/globals.css` — styles for the above (controls, clickable rows,
  badges, live feed)
- `src/data/metrics.json` — regenerated (new totals/SLO, service_timeseries,
  16 typed anomalies)
- `data/logs.jsonl` — regenerated with both chaos scenarios (git-ignored,
  excluded from the zip)
- `data/minute_metrics.csv`, `data/service_summary.csv` — regenerated
- `data/anomalies.csv` — regenerated, gains `type` column
- `grafana/dashboards/service-health.json` — 2 new panels (Latency Anomalies
  by p95; Per-Service p95 Latency table), version 2, JSON validated
- `README.md` — CI/tech badges, "What's new — enhancements" section, updated
  verified-results table and tuning notes, updated run instructions and
  project structure
