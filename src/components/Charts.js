"use client";

// Minimal inline-SVG line chart - no chart library, so the bundle stays tiny
// and there is nothing extra to install or configure on Vercel.

export default function LineChart({
  points, // [{ x: label, y: number }]
  color = "#38bdf8",
  height = 180,
  yLabel = "",
  formatY = (v) => v,
}) {
  if (!points || points.length === 0) return <p>No data</p>;

  const width = 720;
  const padL = 52;
  const padR = 10;
  const padT = 12;
  const padB = 22;
  const innerW = width - padL - padR;
  const innerH = height - padT - padB;

  const maxY = Math.max(...points.map((p) => p.y)) * 1.15 || 1;
  const minY = 0;
  const x = (i) => padL + (i / (points.length - 1)) * innerW;
  const y = (v) => padT + innerH - ((v - minY) / (maxY - minY)) * innerH;

  const path = points
    .map((p, i) => `${i === 0 ? "M" : "L"}${x(i).toFixed(1)},${y(p.y).toFixed(1)}`)
    .join(" ");
  const area = `${path} L${x(points.length - 1).toFixed(1)},${(padT + innerH).toFixed(1)} L${padL},${(padT + innerH).toFixed(1)} Z`;

  const ticks = [0, 0.25, 0.5, 0.75, 1].map((f) => minY + f * (maxY - minY));
  const peak = points.reduce((a, b) => (b.y > a.y ? b : a), points[0]);

  return (
    <svg
      viewBox={`0 0 ${width} ${height}`}
      style={{ width: "100%", height: "auto", display: "block" }}
      role="img"
      aria-label={yLabel}
    >
      {ticks.map((t, i) => (
        <g key={i}>
          <line x1={padL} x2={width - padR} y1={y(t)} y2={y(t)} stroke="#1f2d47" strokeWidth="1" />
          <text x={padL - 6} y={y(t) + 4} fill="#93a3bb" fontSize="10" textAnchor="end">
            {formatY(Math.round(t * 100) / 100)}
          </text>
        </g>
      ))}
      <path d={area} fill={color} opacity="0.14" />
      <path d={path} fill="none" stroke={color} strokeWidth="2" />
      <circle cx={x(points.indexOf(peak))} cy={y(peak.y)} r="4" fill={color} />
      <text x={x(points.indexOf(peak))} y={y(peak.y) - 9} fill="#e5eaf3" fontSize="11" textAnchor="middle">
        peak {formatY(peak.y)}
      </text>
      <text x={padL} y={height - 4} fill="#64748b" fontSize="10">
        {points[0].x}
      </text>
      <text x={width - padR} y={height - 4} fill="#64748b" fontSize="10" textAnchor="end">
        {points[points.length - 1].x}
      </text>
    </svg>
  );
}
