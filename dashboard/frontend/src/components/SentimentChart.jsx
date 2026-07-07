import { useState } from "react";
import {
  ResponsiveContainer,
  ComposedChart,
  Bar,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ReferenceArea,
} from "recharts";
import { buildSentimentChartRows } from "../lib/aggregate.js";

const fmtTime = (s) =>
  new Date(s * 1000).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });

// Keyword A = cool blues (matches blue % line), Keyword B = warm oranges (matches orange % line).
// Within each stack: bright = positive, dark = negative, muted = neutral.
const PALETTE_A = {
  positive: "#58a6ff",
  negative: "#1f4e8c",
  neutral: "#484f58",
};
const PALETTE_B = {
  positive: "#f0883e",
  negative: "#b45309",
  neutral: "#6e5f4a",
};

function ChartTooltip({ active, payload, label, keywordA, keywordB }) {
  if (!active || !payload?.length) return null;
  const row = payload[0]?.payload;
  if (!row) return null;

  const aTotal = row.a_positive + row.a_negative + row.a_neutral;
  const bTotal = row.b_positive + row.b_negative + row.b_neutral;

  return (
    <div className="rounded-lg border border-edge bg-card px-3 py-2 text-xs shadow-lg">
      <div className="mb-2 font-medium text-text">{fmtTime(label)}</div>
      {aTotal > 0 && (
        <div className="mb-1.5">
          <div className="font-medium" style={{ color: PALETTE_A.positive }}>{keywordA}</div>
          <div className="flex gap-2">
            <span style={{ color: PALETTE_A.positive }}>+{row.a_positive}</span>
            <span style={{ color: PALETTE_A.negative }}>−{row.a_negative}</span>
            <span style={{ color: PALETTE_A.neutral }}>○{row.a_neutral}</span>
          </div>
          {row[keywordA] != null && (
            <div className="text-muted">{row[keywordA]}% positive</div>
          )}
        </div>
      )}
      {bTotal > 0 && (
        <div>
          <div className="font-medium" style={{ color: PALETTE_B.positive }}>{keywordB}</div>
          <div className="flex gap-2">
            <span style={{ color: PALETTE_B.positive }}>+{row.b_positive}</span>
            <span style={{ color: PALETTE_B.negative }}>−{row.b_negative}</span>
            <span style={{ color: PALETTE_B.neutral }}>○{row.b_neutral}</span>
          </div>
          {row[keywordB] != null && (
            <div className="text-muted">{row[keywordB]}% positive</div>
          )}
        </div>
      )}
    </div>
  );
}

export default function SentimentChart({
  a,
  b,
  seriesA,
  seriesB,
  comments,
  bucketSeconds,
  range,
  onRangeChange,
}) {
  const data = buildSentimentChartRows(
    comments,
    seriesA,
    seriesB,
    a,
    b,
    bucketSeconds,
  );
  const [drag, setDrag] = useState(null);

  function handleMouseDown(e) {
    if (e?.activeLabel != null) {
      setDrag({ left: e.activeLabel, right: e.activeLabel });
    }
  }

  function handleMouseMove(e) {
    if (drag && e?.activeLabel != null) {
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
    <div className="h-80 w-full rounded-xl border border-edge bg-card p-4">
      <div className="mb-1 flex items-center justify-between text-xs text-muted">
        <span>
          {range
            ? `${fmtTime(range.start)} – ${fmtTime(range.end)}`
            : `blue stack = ${a} · orange stack = ${b} · lines = % positive`}
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
      <ResponsiveContainer width="100%" height="88%">
        <ComposedChart
          data={data}
          margin={{ top: 8, right: 8, bottom: 0, left: -8 }}
          barGap={2}
          barCategoryGap="18%"
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
            fontSize={11}
            minTickGap={48}
          />
          <YAxis
            yAxisId="count"
            stroke="#9aa0ac"
            fontSize={11}
            width={36}
            allowDecimals={false}
            label={{
              value: "comments",
              angle: -90,
              position: "insideLeft",
              fill: "#9aa0ac",
              fontSize: 10,
            }}
          />
          <YAxis
            yAxisId="pct"
            orientation="right"
            domain={[0, 100]}
            stroke="#9aa0ac"
            fontSize={11}
            width={40}
            tickFormatter={(v) => `${v}%`}
          />
          <Tooltip
            content={
              <ChartTooltip keywordA={a} keywordB={b} />
            }
          />
          <Legend
            wrapperStyle={{ fontSize: 11 }}
            formatter={(value) => {
              const labels = {
                a_positive: `${a} positive`,
                a_negative: `${a} negative`,
                a_neutral: `${a} neutral`,
                b_positive: `${b} positive`,
                b_negative: `${b} negative`,
                b_neutral: `${b} neutral`,
                [a]: `${a} % positive`,
                [b]: `${b} % positive`,
              };
              return labels[value] || value;
            }}
          />

          {/* Keyword A — blue-toned stack */}
          <Bar
            yAxisId="count"
            dataKey="a_positive"
            name="a_positive"
            stackId="a"
            fill={PALETTE_A.positive}
            isAnimationActive={false}
          />
          <Bar
            yAxisId="count"
            dataKey="a_negative"
            name="a_negative"
            stackId="a"
            fill={PALETTE_A.negative}
            isAnimationActive={false}
          />
          <Bar
            yAxisId="count"
            dataKey="a_neutral"
            name="a_neutral"
            stackId="a"
            fill={PALETTE_A.neutral}
            radius={[2, 2, 0, 0]}
            isAnimationActive={false}
          />

          {/* Keyword B — orange-toned stack */}
          <Bar
            yAxisId="count"
            dataKey="b_positive"
            name="b_positive"
            stackId="b"
            fill={PALETTE_B.positive}
            isAnimationActive={false}
          />
          <Bar
            yAxisId="count"
            dataKey="b_negative"
            name="b_negative"
            stackId="b"
            fill={PALETTE_B.negative}
            isAnimationActive={false}
          />
          <Bar
            yAxisId="count"
            dataKey="b_neutral"
            name="b_neutral"
            stackId="b"
            fill={PALETTE_B.neutral}
            radius={[2, 2, 0, 0]}
            isAnimationActive={false}
          />

          {/* % positive trend lines */}
          <Line
            yAxisId="pct"
            type="monotone"
            dataKey={a}
            name={a}
            stroke={PALETTE_A.positive}
            strokeWidth={2}
            dot={false}
            isAnimationActive={false}
            connectNulls
          />
          <Line
            yAxisId="pct"
            type="monotone"
            dataKey={b}
            name={b}
            stroke={PALETTE_B.positive}
            strokeWidth={2}
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
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}
