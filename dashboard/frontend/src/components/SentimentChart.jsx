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

// Keyword A = cool blues, Keyword B = warm oranges - bar stacks.
// Within each stack: bright = positive, dark = negative, muted = neutral.
const BAR_PALETTE_A = {
  positive: "#58a6ff",
  negative: "#1f4e8c",
  neutral: "#484f58",
};
const BAR_PALETTE_B = {
  positive: "#f0883e",
  negative: "#b45309",
  neutral: "#6e5f4a",
};

// One color family per compared keyword for the % positive trend lines (same
// hues as that keyword's bar stack), with a couple of extra shades so an
// ambiguous keyword's senses (e.g. "apple (company)" vs "apple (fruit)")
// render as multiple lines that still read as belonging to the same keyword.
const LINE_PALETTE_A = ["#58a6ff", "#79c0ff", "#1f6feb"];
const LINE_PALETTE_B = ["#f0883e", "#ffa657", "#c9622b"];

const labelFor = (s) => (s.sense ? `${s.base} (${s.sense})` : s.base);

// Attach a display label + color to each of a keyword's sense series.
function styledSeries(namedSeries, palette) {
  return namedSeries.map((s, i) => ({
    ...s,
    label: labelFor(s),
    color: palette[i % palette.length],
  }));
}

function ChartTooltip({ active, payload, label, keywordA, keywordB, linesA, linesB }) {
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
          <div className="font-medium" style={{ color: BAR_PALETTE_A.positive }}>{keywordA}</div>
          <div className="flex gap-2">
            <span style={{ color: BAR_PALETTE_A.positive }}>+{row.a_positive}</span>
            <span style={{ color: BAR_PALETTE_A.negative }}>−{row.a_negative}</span>
            <span style={{ color: BAR_PALETTE_A.neutral }}>○{row.a_neutral}</span>
          </div>
          {linesA.map(
            (s) =>
              row[s.key] != null && (
                <div key={s.key} style={{ color: s.color }}>
                  {s.label}: {row[s.key]}%
                </div>
              ),
          )}
        </div>
      )}
      {bTotal > 0 && (
        <div>
          <div className="font-medium" style={{ color: BAR_PALETTE_B.positive }}>{keywordB}</div>
          <div className="flex gap-2">
            <span style={{ color: BAR_PALETTE_B.positive }}>+{row.b_positive}</span>
            <span style={{ color: BAR_PALETTE_B.negative }}>−{row.b_negative}</span>
            <span style={{ color: BAR_PALETTE_B.neutral }}>○{row.b_neutral}</span>
          </div>
          {linesB.map(
            (s) =>
              row[s.key] != null && (
                <div key={s.key} style={{ color: s.color }}>
                  {s.label}: {row[s.key]}%
                </div>
              ),
          )}
        </div>
      )}
    </div>
  );
}

// seriesA/seriesB: flat, comment-count-weighted-merged series per keyword
// (see lib/messages.js#mergeSeries) - used for the bar stacks, which show
// total comment volume regardless of sense.
// namedA/namedB: one entry per sense the keyword has resolved into so far
// (see lib/messages.js#seriesForBase) - used for the % positive trend lines,
// so an ambiguous keyword renders one line per sense.
export default function SentimentChart({
  a,
  b,
  seriesA,
  seriesB,
  namedA,
  namedB,
  comments,
  bucketSeconds,
  range,
  onRangeChange,
}) {
  const linesA = styledSeries(namedA, LINE_PALETTE_A);
  const linesB = styledSeries(namedB, LINE_PALETTE_B);
  const data = buildSentimentChartRows(
    comments,
    seriesA,
    seriesB,
    a,
    b,
    bucketSeconds,
    namedA,
    namedB,
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
              <ChartTooltip keywordA={a} keywordB={b} linesA={linesA} linesB={linesB} />
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
              };
              if (labels[value]) return labels[value];
              const line = [...linesA, ...linesB].find((s) => s.key === value);
              return line ? `${line.label} % positive` : value;
            }}
          />

          {/* Keyword A — blue-toned stack */}
          <Bar
            yAxisId="count"
            dataKey="a_positive"
            name="a_positive"
            stackId="a"
            fill={BAR_PALETTE_A.positive}
            isAnimationActive={false}
          />
          <Bar
            yAxisId="count"
            dataKey="a_negative"
            name="a_negative"
            stackId="a"
            fill={BAR_PALETTE_A.negative}
            isAnimationActive={false}
          />
          <Bar
            yAxisId="count"
            dataKey="a_neutral"
            name="a_neutral"
            stackId="a"
            fill={BAR_PALETTE_A.neutral}
            radius={[2, 2, 0, 0]}
            isAnimationActive={false}
          />

          {/* Keyword B — orange-toned stack */}
          <Bar
            yAxisId="count"
            dataKey="b_positive"
            name="b_positive"
            stackId="b"
            fill={BAR_PALETTE_B.positive}
            isAnimationActive={false}
          />
          <Bar
            yAxisId="count"
            dataKey="b_negative"
            name="b_negative"
            stackId="b"
            fill={BAR_PALETTE_B.negative}
            isAnimationActive={false}
          />
          <Bar
            yAxisId="count"
            dataKey="b_neutral"
            name="b_neutral"
            stackId="b"
            fill={BAR_PALETTE_B.neutral}
            radius={[2, 2, 0, 0]}
            isAnimationActive={false}
          />

          {/* % positive trend lines - one per resolved sense (or a single
              plain line for a keyword with no sense ambiguity yet).
              isAnimationActive={false} is the flicker fix: no entry
              re-animation on every append, so the line grows instead of
              sweeping in again. */}
          {[...linesA, ...linesB].map((s) => (
            <Line
              key={s.key}
              yAxisId="pct"
              type="monotone"
              dataKey={s.key}
              name={s.label}
              stroke={s.color}
              strokeWidth={2}
              dot={false}
              isAnimationActive={false}
              connectNulls
            />
          ))}

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
