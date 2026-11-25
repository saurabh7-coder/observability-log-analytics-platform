"use client";

import { useEffect, useMemo, useState } from "react";
import LineChart from "../components/Charts";

const fmtTime = (ts) => (ts ? ts.slice(11, 16) : "");

export default function Home() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [replay, setReplay] = useState(false);

  useEffect(() => {
    fetch("/api/metrics")
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(setData)
      .catch((e) => setError(e.message));
  }, []);

  // "Replay chaos scenario": visually re-inject the recorded incident into
  // the last 35 minutes of the series, client-side only - the same trick the
  // brief suggests for live demos when the full stack is not running 24/7.
  const series = useMemo(() => {
    if (!data) return [];
    if (!replay) return data.series;
    const n = data.series.length;
    return data.series.map((p, i) => {
      if (i < n - 35) return p;
      const ramp = (i - (n - 35)) / 35;
      return {
        ...p,
        error_rate: Math.round((p.error_rate + 28 * Math.sin(Math.min(ramp * Math.PI, Math.PI)) + 8) * 100) / 100,
        p95_latency_ms: Math.round(p.p95_latency_ms * (1 + 2.4 * Math.sin(Math.min(ramp * Math.PI, Math.PI))) * 100) / 100,
      };
    });
  }, [data, replay]);

  if (error)
    return (
      <main>
        <p className="loading">Could not load metrics ({error}). Run the pipeline first - see README.</p>
      </main>
    );
  if (!data) return <main><p className="loading">Loading metrics…</p></main>;

  const t = data.totals;
  const errorPoints = series.map((p) => ({ x: fmtTime(p.timestamp), y: p.error_rate }));
  const p95Points = series.map((p) => ({ x: fmtTime(p.timestamp), y: p.p95_latency_ms }));

  return (
    <main>
      <header>
        <h1>🔭 Observability &amp; Log Analytics Platform</h1>
        <p>
          Structured logs from 8 simulated microservices, shipped by Vector, buffered in Kafka and
          stored in ClickHouse - with nightly Airflow z-score anomaly detection on error-rate trends.
          This dashboard renders the file-based pipeline output (<code>src/data/metrics.json</code>);
          the same data powers the Grafana dashboard in the Docker stack.
        </p>
        <p className="window">
          Window: {data.window.start} → {data.window.end} UTC · {data.window.minutes} minutes ·{" "}
          {data.anomaly_detection
            ? `Detection: ${data.anomaly_detection.method}, threshold z ≥ ${data.anomaly_detection.threshold}`
            : ""}
        </p>
      </header>

      <section className="cards">
        <div className="card"><div className="label">Total Logs</div><div className="value">{t.total_logs.toLocaleString()}</div></div>
        <div className="card"><div className="label">Overall Error %</div><div className="value">{t.overall_error_rate}%</div></div>
        <div className="card"><div className="label">Overall p95 Latency</div><div className="value">{t.overall_p95_ms} ms</div></div>
        <div className="card alert"><div className="label">Anomalies Detected</div><div className="value">{t.anomalies_detected}</div></div>
        <div className="card"><div className="label">Services</div><div className="value">{t.services_count}</div></div>
      </section>

      <section className="panel">
        <h2>Error Rate Over Time (all services)</h2>
        <p className="sub">Per-minute % of requests with status ≥ 500. The spike is the injected chaos burst on checkout-service.</p>
        <LineChart points={errorPoints} color="#f87171" yLabel="Error rate %" formatY={(v) => `${v}%`} />
      </section>

      <section className="panel">
        <h2>p95 Latency Over Time (all services)</h2>
        <p className="sub">Per-minute 95th-percentile latency in milliseconds - it degrades together with the error burst.</p>
        <LineChart points={p95Points} color="#38bdf8" yLabel="p95 latency ms" formatY={(v) => `${v} ms`} />
      </section>

      <section className="panel">
        <h2>Replay Chaos Scenario</h2>
        <p className="sub">
          Re-injects the recorded incident (error burst + latency degradation, 35 minutes) into the
          end of the charts above - handy for a live demo without the full stack running.
        </p>
        <button className={replay ? "replay off" : "replay"} onClick={() => setReplay(!replay)}>
          {replay ? "⏹ Stop replay" : "▶ Replay chaos scenario"}
        </button>
        {replay && <span className="replay-note">Chaos replay active - charts are showing the injected incident.</span>}
      </section>

      <section className="panel">
        <h2>Service Health</h2>
        <p className="sub">Totals per service for the whole window.</p>
        <table>
          <thead>
            <tr>
              <th>Service</th><th className="num">Requests</th><th className="num">Errors</th>
              <th className="num">Error %</th><th className="num">p95 (ms)</th><th className="num">Avg (ms)</th><th>Status</th>
            </tr>
          </thead>
          <tbody>
            {data.services.map((s) => (
              <tr key={s.service}>
                <td>{s.service}</td>
                <td className="num">{s.requests.toLocaleString()}</td>
                <td className="num">{s.errors.toLocaleString()}</td>
                <td className="num">{s.error_rate}%</td>
                <td className="num">{s.p95_latency_ms}</td>
                <td className="num">{s.avg_latency_ms}</td>
                <td>
                  <span className={s.error_rate > 3 ? "badge bad" : "badge ok"}>
                    {s.error_rate > 3 ? "Degraded" : "Healthy"}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section className="panel">
        <h2>Detected Anomalies (top 15)</h2>
        <p className="sub">
          Rolling 30-minute z-score on per-service error rate, threshold z ≥ 3, found by
          pipeline/anomaly_detection.py (nightly via Airflow in the full stack).
        </p>
        {data.anomalies.length === 0 ? (
          <p>No anomalies detected in this window.</p>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Time (UTC)</th><th>Service</th><th className="num">Error %</th>
                <th className="num">Baseline %</th><th className="num">z-score</th>
                <th className="num">Requests</th><th className="num">Errors</th><th className="num">p95 (ms)</th>
              </tr>
            </thead>
            <tbody>
              {data.anomalies.slice(0, 15).map((a, i) => (
                <tr key={i}>
                  <td>{a.timestamp.replace("T", " ").replace("Z", "")}</td>
                  <td>{a.service}</td>
                  <td className="num">{a.error_rate}%</td>
                  <td className="num">{a.baseline_mean}%</td>
                  <td className="num"><span className="badge z">z = {a.z_score}</span></td>
                  <td className="num">{a.requests}</td>
                  <td className="num">{a.errors}</td>
                  <td className="num">{a.p95_latency_ms}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <footer>
        Pipeline: simulator/log_simulator.py → Vector → Kafka → ClickHouse (raw_logs, mv_service_minute,
        anomalies) → Grafana / this dashboard · Orchestration: Airflow DAG
        observability_anomaly_detection (nightly 01:15) with optional Slack alerting.
      </footer>
    </main>
  );
}
