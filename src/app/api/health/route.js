import { NextResponse } from "next/server";

export async function GET() {
  return NextResponse.json({
    status: "ok",
    service: "observability-log-analytics-platform",
    timestamp: new Date().toISOString(),
  });
}
