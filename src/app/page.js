"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import LineChart from "../components/Charts";

const fmtTime = (ts) => (ts ? ts.slice(11, 16) : "");
const fmtTs = (ts) => (ts ? ts.replace("T", " ").replace("Z", "") : "");
const randHex = (n) =>
  Array.from({ length: n }, () => "0123456789abcdef"[Math.floor(Math.random() * 16)]).join("");

export default function Home() {
  const [data, setData] = useState(null);
  const [logs, setLogs] = useState([]);
  const [error, setError] = useState(null);
  const [replay, setReplay] = useState(false);

  // Drill-down, live mode, log explorer state
  const [selected, setSelected] = useState(null);
  const [live, setLive] = useState(false);
  const [liveSeries, setLiveSeries] = useState(null);
  const [liveLogs, setLiveLogs] = useState([]);
  const [fService, setFService] = useState("all");
  const [fLevel, setFLevel] = useState("all");
  const [fSearch, setFSearch] = useState("");

  useEffect(() => {
    fetch("/api/metrics")
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(setData)
      .catch((e) => setError(e.message));
    fetch("/api/logs")
      .then((r) => (r.ok ? r.json() : []))
      .then(setLogs)
      .catch(() => setLogs([]));
  }, []);

  // "Replay chaos scenario": visually re-inject the recorded incident into
  // the last 35 minutes of the series, client-side only - the same trick the
  // brief suggests for live demos when the full stack is not running 24/7.
  const baseSeries = useMemo(() => {
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
  const baseSeriesRef = useRef(baseSeries);
  baseSeriesRef.current = baseSeries;

  const endpointsByService = useMemo(() => {
    const map = {};
    logs.forEach((l) => {
      if (!map[l.service]) map[l.service] = new Set();
      map[l.service].add(l.endpoint);
    });
    const out = {};
    Object.keys(map).forEach((k) => (out[k] = [...map[k]]));
    return out;
  }, [logs]);

  // Live stream mode (client-side only): every ~1.5s append one simulated
  // minute-point derived from the services' baselines, plus one log line.
  useEffect(() => {
    if (!live || !data) return;
    const tick = () => {
      setLiveSeries((prev) => {
        const base = prev && prev.length ? prev : baseSeriesRef.current;
        if (!base.length) return prev;
        const last = base[base.length - 1];
        const nextTs = new Date(new Date(last.timestamp).getTime() + 60000).toISOString();
        const spike = Math.random() < 0.06;
        const err = Math.max(
          0,
          data.totals.overall_error_rate + (Math.random() - 0.5) * 1.6 + (spike ? 14 : 0)
        );
        const p95 = Math.max(
          20,
          data.totals.overall_p95_ms * (0.85 + Math.random() * 0.4) * (spike ? 2.2 : 1)
        );
        const point = {
          timestamp: nextTs,
          requests: 0,
          errors: 0,
          error_rate: Math.round(err * 100) / 100,
          p95_latency_ms: Math.round(p95 * 100) / 100,
          avg_latency_ms: 0,
        };
        return [...base.slice(-359), point];
      });

      // One simulated log line, weighted by each service's traffic share.
      const totalReq = data.services.reduce((s, x) => s + x.requests, 0);
      let r = Math.random() * totalReq;
      let svc = data.services[0];
      for (const s of data.services) {
        r -= s.requests;
        if (r <= 0) { svc = s; break; }
      }
      const isErr = Math.random() * 100 < svc.error_rate;
      const status = isErr ? [500, 502, 503][Math.floor(Math.random() * 3)] : 200;
      const eps = endpointsByService[svc.service] || ["/"];
      setLiveLogs((prev) =>
        [
          {
            timestamp: new Date().toISOString(),
            service: svc.service,
            level: isErr ? "ERROR" : "INFO",
            status_code: status,
            latency_ms: Math.round(svc.avg_latency_ms * (0.5 + Math.random()) * (isErr ? 1.5 : 1)),
            endpoint: eps[Math.floor(Math.random() * eps.length)],
            trace_id: randHex(16),
            message: `${svc.service} request ${isErr ? "failed" : "completed"} with status ${status}`,
          },
          ...prev,
        ].slice(0, 8)
      );
    };
    const id = setInterval(tick, 1500);
    return () => clearInterval(id);
  }, [live, data, endpointsByService]);

  const toggleLive = () => {
    if (live) setLiveSeries(null);
    setLive(!live);
  };

  const series = live && liveSeries ? liveSeries : baseSeries;

  const filteredLogs = useMemo(() => {
    const q = fSearch.trim().toLowerCase();
    return logs.filter((l) => {
      if (fService !== "all" && l.service !== fService) return false;
      if (fLevel !== "all" && l.level !== fLevel) return false;
      if (q && !`${l.endpoint} ${l.message} ${l.trace_id}`.toLowerCase().includes(q)) return false;
      return true;
    });
  }, [logs, fService, fLevel, fSearch]);

  if (error)
    return (
      <main>
        <p className="loading">Could not load metrics ({error}). Run the pipeline first - see README.</p>
      </main>
    );
  if (!data) return <main><p className="loading">Loading metrics…</p></main>;

  const t = data.totals;
  const uptime = t.uptime_pct ?? Math.round((100 - t.overall_error_rate) * 1000) / 1000;
  const sloTarget = t.slo_target_pct ?? 99;
  const budgetLeft = t.error_budget_remaining_pct ?? 0;
  const avgLat = t.avg_latency_ms ?? t.overall_avg_ms;

  const errorPoints = series.map((p) => ({ x: fmtTime(p.timestamp), y: p.error_rate }));
  const p95Points = series.map((p) => ({ x: fmtTime(p.timestamp), y: p.p95_latency_ms }));

  const selSummary = selected ? data.services.find((s) => s.service === selected) : null;
  const selSeries = selected && data.service_timeseries ? data.service_timeseries[selected] || [] : [];
  const logServices = [...new Set(logs.map((l) => l.service))].sort();
  const logLevels = [...new Set(logs.map((l) => l.level))].sort();

  return (
    <main>
      <header>
        <h1>🔭 Observability &amp; Log Analytics Platform</h1>
        <p>
          Structured logs from 8 simulated microservices, shipped by Vector, buffered in Kafka and
          stored in ClickHouse - with nightly Airflow z-score anomaly detection on error-rate and
          latency trends. This dashboard renders the file-based pipeline output
          (<code>src/data/metrics.json</code>); the same data powers the Grafana dashboard in the
          Docker stack.
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

      <section className="cards slo">
        <div className="card"><div className="label">Uptime (window)</div><div className="value">{uptime}%</div></div>
        <div className="card">
          <div className="label">SLO Target</div><div className="value">{sloTarget}%</div>
          <div className="note">{uptime >= sloTarget ? "SLO met for this window" : "SLO missed for this window"}</div>
        </div>
        <div className="card">
          <div className="label">Error Budget Remaining</div><div className="value">{budgetLeft}%</div>
          <div className="note">of the {(100 - sloTarget).toFixed(1)}% budget allowed by the SLO</div>
        </div>
        <div className="card"><div className="label">Avg Latency</div><div className="value">{avgLat} ms</div></div>
      </section>

      <section className="panel">
        <h2>Error Rate Over Time (all services) {live && <span className="live-dot">● LIVE</span>}</h2>
        <p className="sub">Per-minute % of requests with status ≥ 500. Spikes: the injected checkout error burst and the payment latency degradation.</p>
        <LineChart points={errorPoints} color="#f87171" yLabel="Error rate %" formatY={(v) => `${v}%`} />
      </section>

      <section className="panel">
        <h2>p95 Latency Over Time (all services) {live && <span className="live-dot">● LIVE</span>}</h2>
        <p className="sub">Per-minute 95th-percentile latency in milliseconds - it degrades together with both chaos scenarios.</p>
        <LineChart points={p95Points} color="#38bdf8" yLabel="p95 latency ms" formatY={(v) => `${v} ms`} />
      </section>

      <section className="panel">
        <h2>Live Stream Mode &amp; Replay</h2>
        <p className="sub">
          Live mode appends a simulated minute-point and log line every ~1.5 seconds, derived from
          the services&apos; baselines (client-side only). Replay re-injects the recorded incident
          (35 minutes) into the end of the charts - handy for demos without the full stack running.
        </p>
        <button className={live ? "replay off" : "replay live-btn"} onClick={toggleLive}>
          {live ? "⏹ Stop live stream" : "▶ Start live stream"}
        </button>
        <button className={replay ? "replay off" : "replay"} onClick={() => setReplay(!replay)} style={{ marginLeft: 10 }}>
          {replay ? "⏹ Stop replay" : "▶ Replay chaos scenario"}
        </button>
        {replay && <span className="replay-note">Chaos replay active - charts are showing the injected incident.</span>}
        {live && liveLogs.length > 0 && (
          <div className="livefeed">
            {liveLogs.map((l, i) => (
              <div key={i}>
                <span className="lf-time">{fmtTs(l.timestamp)}</span>{" "}
                <span className={`badge lvl-${l.level.toLowerCase()}`}>{l.level}</span>{" "}
                {l.service} · {l.status_code} · {l.latency_ms} ms · {l.endpoint} · trace {l.trace_id.slice(0, 8)}…
              </div>
            ))}
          </div>
        )}
      </section>

      <section className="panel">
        <h2>Service Health</h2>
        <p className="sub">Totals per service for the whole window. Click a row (or use the selector) to drill into a service.</p>
        <div className="controls">
          <select value={selected || ""} onChange={(e) => setSelected(e.target.value || null)}>
            <option value="">Select a service to drill down…</option>
            {data.services.map((s) => (
              <option key={s.service} value={s.service}>{s.service}</option>
            ))}
          </select>
          {selected && (
            <button className="linkbtn" onClick={() => setSelected(null)}>✕ Clear selection</button>
          )}
        </div>
        <table>
          <thead>
            <tr>
              <th>Service</th><th className="num">Requests</th><th className="num">Errors</th>
              <th className="num">Error %</th><th className="num">p95 (ms)</th><th className="num">Avg (ms)</th><th>Status</th>
            </tr>
          </thead>
          <tbody>
            {data.services.map((s) => (
              <tr
                key={s.service}
                className={`clickable${selected === s.service ? " selected" : ""}`}
                onClick={() => setSelected(selected === s.service ? null : s.service)}
              >
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

      {selected && selSummary && (
        <section className="panel">
          <h2>Drill-down: {selected}</h2>
          <p className="sub">
            {selSummary.requests.toLocaleString()} requests · {selSummary.errors.toLocaleString()} errors
            ({selSummary.error_rate}%) · p95 {selSummary.p95_latency_ms} ms · avg {selSummary.avg_latency_ms} ms
            over the window. Per-minute series from the pipeline.
          </p>
          <h3 className="chart-title">Error rate % — {selected}</h3>
          <LineChart
            points={selSeries.map((p) => ({ x: fmtTime(p.t), y: p.error_rate }))}
            color="#f87171" yLabel="Error rate %" formatY={(v) => `${v}%`}
          />
          <h3 className="chart-title">p95 latency ms — {selected}</h3>
          <LineChart
            points={selSeries.map((p) => ({ x: fmtTime(p.t), y: p.p95 }))}
            color="#38bdf8" yLabel="p95 latency ms" formatY={(v) => `${v} ms`}
          />
        </section>
      )}

      <section className="panel">
        <h2>Log Explorer</h2>
        <p className="sub">
          A {logs.length}-log sample from the pipeline (including chaos-window ERROR logs) - filter
          by service and level, or search endpoint / message / trace ID.
        </p>
        <div className="controls">
          <select value={fService} onChange={(e) => setFService(e.target.value)}>
            <option value="all">All services</option>
            {logServices.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
          <select value={fLevel} onChange={(e) => setFLevel(e.target.value)}>
            <option value="all">All levels</option>
            {logLevels.map((lv) => (
              <option key={lv} value={lv}>{lv}</option>
            ))}
          </select>
          <input
            type="text"
            placeholder="Search endpoint, message or trace ID…"
            value={fSearch}
            onChange={(e) => setFSearch(e.target.value)}
          />
          <span className="count-note">
            Showing {Math.min(filteredLogs.length, 100)} of {filteredLogs.length} matching logs
          </span>
        </div>
        <table>
          <thead>
            <tr>
              <th>Time (UTC)</th><th>Service</th><th>Level</th><th className="num">Status</th>
              <th className="num">Latency (ms)</th><th>Trace ID</th><th>Endpoint</th><th>Message</th>
            </tr>
          </thead>
          <tbody>
            {filteredLogs.slice(0, 100).map((l, i) => (
              <tr key={i}>
                <td>{fmtTs(l.timestamp)}</td>
                <td>{l.service}</td>
                <td><span className={`badge lvl-${l.level.toLowerCase()}`}>{l.level}</span></td>
                <td className="num">{l.status_code}</td>
                <td className="num">{l.latency_ms}</td>
                <td title={l.trace_id}>{l.trace_id.slice(0, 8)}…</td>
                <td>{l.endpoint}</td>
                <td>{l.message}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {filteredLogs.length === 0 && <p>No logs match these filters.</p>}
      </section>

      <section className="panel">
        <h2>Detected Anomalies (top 15)</h2>
        <p className="sub">
          Rolling 30-minute z-score detectors on per-service error rate and p95 latency,
          threshold z ≥ 3, from pipeline/anomaly_detection.py (nightly via Airflow in the full
          stack). Baseline is in the flagged metric&apos;s units (% for error-rate, ms for latency).
        </p>
        {data.anomalies.length === 0 ? (
          <p>No anomalies detected in this window.</p>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Time (UTC)</th><th>Service</th><th>Type</th><th className="num">Error %</th>
                <th className="num">p95 (ms)</th><th className="num">Baseline</th>
                <th className="num">z-score</th><th className="num">Requests</th><th className="num">Errors</th>
              </tr>
            </thead>
            <tbody>
              {data.anomalies.slice(0, 15).map((a, i) => (
                <tr key={i}>
                  <td>{fmtTs(a.timestamp)}</td>
                  <td>{a.service}</td>
                  <td>
                    <span className={a.type === "latency" ? "badge type-lat" : "badge type-err"}>
                      {a.type || "error_rate"}
                    </span>
                  </td>
                  <td className="num">{a.error_rate}%</td>
                  <td className="num">{a.p95_latency_ms}</td>
                  <td className="num">{a.type === "latency" ? `${a.baseline_mean} ms` : `${a.baseline_mean}%`}</td>
                  <td className="num"><span className="badge z">z = {a.z_score}</span></td>
                  <td className="num">{a.requests}</td>
                  <td className="num">{a.errors}</td>
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
