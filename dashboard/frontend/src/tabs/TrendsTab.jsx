import { usePoll } from "../lib/usePoll.js";
import { getTrending } from "../lib/api.js";
import { Panel } from "../components/Panel.jsx";
import { TrendingList, fmt } from "../components/TrendingList.jsx";

// Trends tab (P1): Count-Min Sketch heavy hitters (words + two-word phrases),
// with momentum vs the previous window (NEW / rising / cooling). Follows the
// Sentiment tab's compare selection: one panel per compared keyword, side by
// side; before any comparison exists it merges all tracked keywords. Values
// are estimates by design (fixed-memory sketch), so every number carries a
// "~" and the footer names the sketch and its error bound.
// (The HyperLogLog reach data still streams on analytics-results and is
// served by /api/reach - the panel was dropped from the UI, not the pipeline.)

const windowLabel = (end) =>
  end
    ? `window ending ${new Date(end * 1000).toLocaleString([], {
        dateStyle: "medium",
        timeStyle: "short",
      })}`
    : "waiting for first window…";

export default function TrendsTab({ compared }) {
  const comparedKey = (compared || []).join(",");
  const { data, error } = usePoll(
    async () => {
      // One panel PER compared keyword (side by side, like the sentiment
      // chart) - fetch each keyword's scoped view separately. Without an
      // active comparison, fall back to merging all tracked keywords.
      const keywords = [...new Set(compared || [])];
      if (keywords.length > 0) {
        const records = await Promise.all(keywords.map(getTrending));
        return { pair: keywords.map((kw, i) => ({ kw, record: records[i] })) };
      }
      return { trending: await getTrending() };
    },
    4000,
    [comparedKey],
  );

  if (error)
    return (
      <div className="rounded-lg border border-neg/40 bg-neg/10 px-4 py-2 text-sm text-neg">
        {error}
      </div>
    );
  if (!data) return <p className="text-sm text-muted">Loading trends…</p>;

  const { trending, pair } = data;

  return (
    <div className="space-y-4">
      <LateDataBanner late={(pair ? pair[0].record : trending)?.late_drops} />
      {pair ? (
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          {pair.map(({ kw, record }, i) => (
            <TrendingPanel
              key={kw}
              record={record}
              title={`🔥 Trending around "${kw}"`}
              barClass={i === 0 ? "bg-accent" : "bg-accent2"}
            />
          ))}
        </div>
      ) : (
        <div className="mx-auto w-full max-w-3xl">
          <TrendingPanel record={trending} />
        </div>
      )}
    </div>
  );
}

// Shown when Flink's event-time windows recently DROPPED records as late -
// which happens when a slice of the dump is replayed a second time (its
// event time is behind the watermark). Without this banner the tab just
// looks frozen: comments keep flowing (unwindowed) but no new window ever
// closes, so newly tracked keywords never show up.
function LateDataBanner({ late }) {
  const recent =
    late &&
    late.total > 0 &&
    late.last_at &&
    Date.now() / 1000 - late.last_at < 300;
  if (!recent) return null;
  return (
    <div className="rounded-lg border border-neg/40 bg-neg/10 px-4 py-2 text-sm text-neg">
      ⚠ ~{fmt(late.total)} records were dropped as <b>late</b>: the replayed
      data is older than Flink&apos;s event-time watermark, so trends and the
      sentiment graph ignore it (newly added keywords will not appear). Use{" "}
      <b>Reset pipeline</b> on the Pipeline tab, then replay — the fresh job
      re-windows the data with the current keyword set.
    </div>
  );
}

function TrendingPanel({ record, title, barClass = "bg-accent" }) {
  const items = record?.items || [];
  const tracked = record?.keywords || [];
  const sketch = record?.sketch;

  return (
    <Panel
      title={title || "🔥 Trending now"}
      right={
        <span className="text-xs text-muted">
          {windowLabel(record?.window_end)}
        </span>
      }
    >
      {!title && tracked.length > 0 && (
        <p className="mb-3 text-xs text-muted">
          words &amp; phrases around{" "}
          <span className="text-accent">{tracked.join(" · ")}</span>
        </p>
      )}
      <TrendingList
        items={items}
        showKeyword={!title && tracked.length > 1}
        barClass={barClass}
      />
      {sketch && (
        <p className="mt-4 text-xs text-muted">
          Count-Min Sketch {sketch.depth}×{sketch.width} per keyword (~
          {Math.round((sketch.depth * sketch.width * 8) / 1024)} KB each, flat)
          over ~{fmt(sketch.stream_total)} terms — estimates may slightly
          overcount, never undercount. ▲▼ compare against the previous window.
        </p>
      )}
    </Panel>
  );
}

