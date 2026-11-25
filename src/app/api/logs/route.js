import { NextResponse } from "next/server";
import logSample from "../../../data/log_sample.json";

// Serves src/data/log_sample.json (~300 logs incl. chaos-window ERROR logs),
// produced by pipeline/aggregate.py for the dashboard's log explorer.
export async function GET() {
  return NextResponse.json(logSample);
}
