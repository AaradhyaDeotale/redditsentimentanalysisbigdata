import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
} from "recharts";

const fmtTime = (s) =>
  new Date(s * 1000).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });

// Align the two per-keyword series by window_end into the single row-per-time
// shape Recharts expects. Real windows share window boundaries, so timestamps
// line up across keywords.
function buildChartData(a, b, seriesA, seriesB) {
  const rows = new Map();
  for (const p of seriesA) {
    rows.set(p.window_end, {
      t: p.window_end,
      [a]: Math.round(p.positive_ratio * 100),
    });
  }
  for (const p of seriesB) {
    const row = rows.get(p.window_end) || { t: p.window_end };
    row[b] = Math.round(p.positive_ratio * 100);
    rows.set(p.window_end, row);
  }
  return [...rows.values()].sort((x, y) => x.t - y.t);
}

export default function SentimentChart({ a, b, seriesA, seriesB }) {
  const data = buildChartData(a, b, seriesA, seriesB);
  return (
    <div className="h-72 w-full rounded-xl border border-edge bg-card p-4">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 8, right: 16, bottom: 0, left: -12 }}>
          <CartesianGrid stroke="#2a2e3a" vertical={false} />
          <XAxis
            dataKey="t"
            tickFormatter={fmtTime}
            stroke="#9aa0ac"
            fontSize={12}
            minTickGap={48}
          />
          <YAxis
            domain={[0, 100]}
            stroke="#9aa0ac"
            fontSize={12}
            tickFormatter={(v) => `${v}%`}
            width={48}
          />
          <Tooltip
            labelFormatter={fmtTime}
            contentStyle={{
              background: "#1a1d27",
              border: "1px solid #2a2e3a",
              borderRadius: 8,
              color: "#e8e8ec",
            }}
          />
          <Legend />
          {/* isAnimationActive={false} is the flicker fix: no entry re-animation
              on every append, so the line grows instead of sweeping in again. */}
          <Line
            type="monotone"
            dataKey={a}
            stroke="#58a6ff"
            dot={false}
            isAnimationActive={false}
            connectNulls
          />
          <Line
            type="monotone"
            dataKey={b}
            stroke="#f0883e"
            dot={false}
            isAnimationActive={false}
            connectNulls
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
