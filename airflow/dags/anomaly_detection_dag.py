"""
Nightly anomaly-detection DAG.

Schedule: 15 1 * * *  (01:15 every night, after the day's logs have landed)

Tasks:
  1. aggregate_logs      - rebuild per-minute metrics from the raw logs
                           (in production this reads ClickHouse instead of
                           the file; the aggregation logic is identical)
  2. detect_anomalies    - run pipeline/anomaly_detection.py
                           (rolling 30-min z-score, threshold 3)
  3. slack_alert         - if anomalies were found and SLACK_WEBHOOK_URL is
                           set, post a summary to Slack; otherwise log only

In the full stack the detection step also inserts its rows into the
ClickHouse `observability.anomalies` table (see clickhouse/schema.sql);
locally it writes data/anomalies.csv and updates src/data/metrics.json.
"""

import csv
import json
import os
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(os.environ.get("OBSERVABILITY_PROJECT_ROOT", "/opt/observability-platform"))

default_args = {
    "owner": "dataops",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}


def slack_alert(**_context):
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    anomalies_csv = PROJECT_ROOT / "data" / "anomalies.csv"
    rows = []
    if anomalies_csv.exists():
        with open(anomalies_csv, newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))

    if not rows:
        print("No anomalies detected - nothing to alert.")
        return "no_anomalies"

    top = rows[0]
    text = (
        f":rotating_light: *Observability anomaly report* - {len(rows)} anomalous "
        f"service-minute(s) detected.\n"
        f"Top: `{top['service']}` at {top['timestamp']} - error rate "
        f"{top['error_rate']}% (z-score {top['z_score']}, p95 {top['p95_latency_ms']} ms)."
    )
    print(text)
    if not webhook:
        print("SLACK_WEBHOOK_URL not set - alert logged only, not sent.")
        return "logged_only"

    req = urllib.request.Request(
        webhook,
        data=json.dumps({"text": text}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        print(f"Slack alert sent, HTTP {resp.status}")
    return "alert_sent"


with DAG(
    dag_id="observability_anomaly_detection",
    description="Nightly z-score anomaly detection over service error rates",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="15 1 * * *",
    catchup=False,
    tags=["observability", "anomaly-detection"],
) as dag:

    aggregate = BashOperator(
        task_id="aggregate_logs",
        bash_command=f"cd {PROJECT_ROOT} && python3 pipeline/aggregate.py",
    )

    detect = BashOperator(
        task_id="detect_anomalies",
        bash_command=f"cd {PROJECT_ROOT} && python3 pipeline/anomaly_detection.py",
    )

    alert = PythonOperator(
        task_id="slack_alert",
        python_callable=slack_alert,
    )

    aggregate >> detect >> alert
