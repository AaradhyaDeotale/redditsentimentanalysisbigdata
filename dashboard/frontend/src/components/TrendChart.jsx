// Top-terms-over-time line chart for the Trends tab: each line is one of the
// latest window's top terms, drawn across the last few closed windows (the
// store keeps up to 8). A gap in a line means the term was below that
// window's stored top-K cutoff - NOT zero - so lines never connect nulls.

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
import { fmt } from "./TrendingList.jsx";

// CVD-validated 5-slot categorical palette for the dark card surface
// (#1a1d27): darker steps of the app's blue/orange accents plus aqua,
// violet, magenta. Slots follow the latest window's rank order, so the
// chart's colors line up with the ranked list below it.
const SERIES_COLORS = ["#3987e5", "#d95926", "#199e70", "#9085e9", "#d55181"];

const fmtTime = (s) =>
  new Date(s * 1000).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });

function ChartTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null;
  const rows = payload
    .filter((p) => p.value != null)
    .sort((x, y) => y.value - x.value);
  if (rows.length === 0) return null;
  return (
    <div className="rounded-lg border border-edge bg-card px-3 py-2 text-xs shadow-lg">
      <div className="mb-1 font-medium text-text">window {fmtTime(label)}</div>
      {rows.map((p) => (
        <div key={p.dataKey} className="flex items-center gap-2">
          <span
            className="h-2 w-2 shrink-0 rounded-full"
            style={{ background: p.stroke }}
          />
          <span className="text-muted">{p.dataKey}</span>
          <span className="ml-auto pl-3 tabular-nums text-text">
            ~{fmt(p.value)}
          </span>
        </div>
      ))}
    </div>
  );
}

export default function TrendChart({ history }) {
  const windows = history?.windows || [];
  const series = (history?.series || []).slice(0, SERIES_COLORS.length);
  if (windows.length < 2 || series.length === 0) return null;

  // One row per closed window; time lives under "__t" so a trending term
  // literally named "t" can never collide with it.
  const data = windows.map((t, i) => {
    const row = { __t: t };
    for (const s of series) row[s.token] = s.points[i];
    return row;
  });

  return (
    <div className="mb-4">
      <p className="mb-1 text-xs text-muted">
        top {series.length} terms across the last {windows.length} windows
      </p>
      <div className="h-44 w-full">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 4, right: 8, bottom: 0, left: -16 }}>
            <CartesianGrid stroke="#2a2e3a" vertical={false} />
            <XAxis
              dataKey="__t"
              type="number"
              domain={["dataMin", "dataMax"]}
              ticks={windows}
              tickFormatter={fmtTime}
              stroke="#9aa0ac"
              fontSize={11}
              minTickGap={40}
            />
            <YAxis
              stroke="#9aa0ac"
              fontSize={11}
              width={48}
              allowDecimals={false}
              tickFormatter={fmt}
            />
            <Tooltip content={<ChartTooltip />} />
            <Legend
              wrapperStyle={{ fontSize: 11 }}
              iconType="plainline"
              formatter={(value) => (
                <span style={{ color: "#9aa0ac" }}>{value}</span>
              )}
            />
            {series.map((s, i) => (
              <Line
                key={s.token}
                dataKey={s.token}
                stroke={SERIES_COLORS[i]}
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 4 }}
                isAnimationActive={false}
                connectNulls={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
