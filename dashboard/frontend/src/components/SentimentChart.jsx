import { useState } from "react";
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ReferenceArea,
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

// Drag across the plot area to select a timestamp range: mousedown/mousemove
// track a transient local selection, mouseup commits it via onRangeChange so
// the parent can zoom the axis and filter other panels (comments, cards) to
// match. `range` is fully controlled by the parent - this component has no
// persistent zoom state of its own, only the in-progress drag rectangle.
export default function SentimentChart({ a, b, seriesA, seriesB, range, onRangeChange }) {
  const data = buildChartData(a, b, seriesA, seriesB);
  const [drag, setDrag] = useState(null); // { left, right } while actively dragging

  function handleMouseDown(e) {
    if (e && e.activeLabel != null) {
      setDrag({ left: e.activeLabel, right: e.activeLabel });
    }
  }

  function handleMouseMove(e) {
    if (drag && e && e.activeLabel != null) {
      setDrag((d) => ({ ...d, right: e.activeLabel }));
    }
  }

  function handleMouseUp() {
    if (drag) {
      let { left, right } = drag;
      if (left > right) [left, right] = [right, left];
      if (left !== right) onRangeChange?.({ start: left, end: right });
      setDrag(null);
    }
  }

  return (
    <div className="h-72 w-full rounded-xl border border-edge bg-card p-4">
      <div className="mb-1 flex items-center justify-between text-xs text-muted">
        <span>
          {range
            ? `${fmtTime(range.start)} – ${fmtTime(range.end)}`
            : "drag on the chart to select a time range"}
        </span>
        {range && (
          <button
            type="button"
            onClick={() => onRangeChange?.(null)}
            className="rounded-md border border-edge px-2 py-0.5 hover:border-accent hover:text-text"
          >
            Clear selection
          </button>
        )}
      </div>
      <ResponsiveContainer width="100%" height="90%">
        <LineChart
          data={data}
          margin={{ top: 8, right: 16, bottom: 0, left: -12 }}
          onMouseDown={handleMouseDown}
          onMouseMove={handleMouseMove}
          onMouseUp={handleMouseUp}
        >
          <CartesianGrid stroke="#2a2e3a" vertical={false} />
          <XAxis
            dataKey="t"
            type="number"
            domain={range ? [range.start, range.end] : ["dataMin", "dataMax"]}
            allowDataOverflow
            tickFormatter={fmtTime}
            stroke="#9aa0ac"
            fontSize={12}
            minTickGap={48}
          />
          <YAxis
            domain={[0, 100]}
            allowDataOverflow
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
          {drag && (
            <ReferenceArea
              x1={drag.left}
              x2={drag.right}
              strokeOpacity={0.3}
              fill="#58a6ff"
              fillOpacity={0.2}
            />
          )}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
