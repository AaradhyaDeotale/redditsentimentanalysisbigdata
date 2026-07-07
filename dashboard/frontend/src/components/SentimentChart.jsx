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

// One color family per compared keyword (blue for the first, orange for the
// second - same hues as before), with a couple of extra shades so an
// ambiguous keyword's senses (e.g. "apple (company)" vs "apple (fruit)")
// render as multiple lines that still read as belonging to the same keyword.
const PALETTE_A = ["#58a6ff", "#79c0ff", "#1f6feb"];
const PALETTE_B = ["#f0883e", "#ffa657", "#c9622b"];

const labelFor = (s) => (s.sense ? `${s.base} (${s.sense})` : s.base);

// Attach a display label + color to each of a keyword's sense series.
function styledSeries(namedSeries, palette) {
  return namedSeries.map((s, i) => ({
    ...s,
    label: labelFor(s),
    color: palette[i % palette.length],
  }));
}

// Align every series by window_end into the single row-per-time shape
// Recharts expects. Real windows share window boundaries, so timestamps
// line up across keywords (and across a keyword's senses).
function buildChartData(series) {
  const rows = new Map();
  for (const s of series) {
    for (const p of s.points) {
      const row = rows.get(p.window_end) || { t: p.window_end };
      row[s.key] = Math.round(p.positive_ratio * 100);
      rows.set(p.window_end, row);
    }
  }
  return [...rows.values()].sort((x, y) => x.t - y.t);
}

// seriesA/seriesB: arrays of {key, base, sense, points} from
// lib/messages.js#seriesForBase - one entry per sense the keyword has
// resolved into so far (or a single plain entry for an unambiguous keyword).
export default function SentimentChart({ seriesA, seriesB }) {
  const series = [
    ...styledSeries(seriesA, PALETTE_A),
    ...styledSeries(seriesB, PALETTE_B),
  ];
  const data = buildChartData(series);
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
          {series.map((s) => (
            <Line
              key={s.key}
              type="monotone"
              dataKey={s.key}
              name={s.label}
              stroke={s.color}
              dot={false}
              isAnimationActive={false}
              connectNulls
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
