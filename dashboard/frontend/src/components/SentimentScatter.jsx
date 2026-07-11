import {
  ResponsiveContainer,
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  ZAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ReferenceLine,
} from "recharts";
import { scatterSeries } from "../lib/aggregate.js";

const fmtTime = (s) =>
  new Date(s * 1000).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });

// Match the line chart's convention: keyword A = blue, keyword B = orange.
const COLOR_A = "#58a6ff";
const COLOR_B = "#f0883e";

function ScatterTooltip({ active, payload }) {
  if (!active || !payload?.length) return null;
  const p = payload[0]?.payload;
  if (!p) return null;
  const scoreClass =
    p.score > 0.05 ? "text-pos" : p.score < -0.05 ? "text-neg" : "text-muted";
  const body = p.body ? String(p.body).slice(0, 140) : "";
  return (
    <div className="max-w-xs rounded-lg border border-edge bg-card px-3 py-2 text-xs shadow-lg">
      <div className="mb-1 flex items-center justify-between gap-3">
        <span className="font-medium text-text">{fmtTime(p.t)}</span>
        <span className={scoreClass}>
          {p.score > 0 ? "+" : ""}
          {p.score.toFixed(2)}
        </span>
      </div>
      {p.author && <div className="text-muted">u/{p.author}</div>}
      {body && <div className="mt-1 text-text/80">{body}</div>}
    </div>
  );
}

// No-aggregation view: one dot per scored comment, plotted at its raw sentiment
// score the moment it arrives over the WebSocket. Nothing is averaged.
export default function SentimentScatter({ a, b, comments }) {
  const dataA = scatterSeries(comments, a);
  const dataB = scatterSeries(comments, b);
  const empty = dataA.length === 0 && dataB.length === 0;

  return (
    <div className="h-80 w-full rounded-xl border border-edge bg-card p-4">
      <div className="mb-1 text-xs text-muted">
        each dot = one comment · y = sentiment score (−1…+1) ·{" "}
        <span style={{ color: COLOR_A }}>{a}</span> ·{" "}
        <span style={{ color: COLOR_B }}>{b}</span>
      </div>
      <ResponsiveContainer width="100%" height="88%">
        <ScatterChart margin={{ top: 8, right: 8, bottom: 0, left: -8 }}>
          <CartesianGrid stroke="#2a2e3a" />
          <XAxis
            type="number"
            dataKey="t"
            domain={["dataMin", "dataMax"]}
            tickFormatter={fmtTime}
            stroke="#9aa0ac"
            fontSize={11}
            minTickGap={48}
            name="time"
          />
          <YAxis
            type="number"
            dataKey="score"
            domain={[-1, 1]}
            ticks={[-1, -0.5, 0, 0.5, 1]}
            stroke="#9aa0ac"
            fontSize={11}
            width={36}
            name="score"
          />
          <ZAxis range={[40, 40]} />
          <ReferenceLine y={0} stroke="#484f58" strokeDasharray="4 4" />
          <Tooltip content={<ScatterTooltip />} cursor={{ strokeDasharray: "3 3" }} />
          <Legend wrapperStyle={{ fontSize: 11 }} />
          <Scatter
            name={a}
            data={dataA}
            fill={COLOR_A}
            fillOpacity={0.7}
            isAnimationActive
            animationDuration={300}
            animationEasing="ease-out"
          />
          <Scatter
            name={b}
            data={dataB}
            fill={COLOR_B}
            fillOpacity={0.7}
            isAnimationActive
            animationDuration={300}
            animationEasing="ease-out"
          />
        </ScatterChart>
      </ResponsiveContainer>
      {empty && (
        <div className="pointer-events-none -mt-40 text-center text-sm text-muted">
          waiting for comments…
        </div>
      )}
    </div>
  );
}
