import { NextResponse } from "next/server";
import metrics from "../../../data/metrics.json";

// Serves src/data/metrics.json, produced by:
//   python3 simulator/log_simulator.py --mode batch --minutes 360 --out data/logs.jsonl
//   python3 pipeline/aggregate.py
//   python3 pipeline/anomaly_detection.py
export async function GET() {
  return NextResponse.json(metrics);
}
