import { usePoll } from "../lib/usePoll.js";
import { getTrending, getReach, getTrendingExamples } from "../lib/api.js";
import { Panel } from "../components/Panel.jsx";
import { TrendingList, fmt } from "../components/TrendingList.jsx";
import TrendChart from "../components/TrendChart.jsx";
import TrendVoices from "../components/TrendVoices.jsx";

// Trends tab (P1): Count-Min Sketch heavy hitters (words + two-word phrases),
// with momentum vs the previous window (NEW / rising / cooling), a line chart
// of the top terms across the last few closed windows, plus each keyword's
// HyperLogLog reach (~unique authors) as a line under the panel header. Follows the Sentiment tab's compare selection: one panel per
// compared keyword, side by side; before any comparison exists it merges all
// tracked keywords (reach then lists every tracked keyword - the per-keyword
// HLLs can't be summed here, the dashboard only sees their cardinalities).
// Values are estimates by design (fixed-memory sketches), so every number
// carries a "~" and the footer names the sketch and its error bound.

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
        const [records, reaches, examples] = await Promise.all([
          Promise.all(keywords.map(getTrending)),
          Promise.all(keywords.map(getReach)),
          Promise.all(keywords.map(getTrendingExamples)),
        ]);
        return {
          pair: keywords.map((kw, i) => ({
            kw,
            record: records[i],
            // Reach history is oldest-first; the panel shows the latest window.
            reach: reaches[i]?.points?.at(-1) || null,
            voices: examples[i]?.terms || [],
          })),
        };
      }
      const [trending, reach, examples] = await Promise.all([
        getTrending(),
        getReach(),
        getTrendingExamples(),
      ]);
      return {
        trending,
        reachList: reach?.keywords || [],
        voices: examples?.terms || [],
      };
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

  const { trending, pair, reachList, voices } = data;

  return (
    <div className="space-y-4">
      <LateDataBanner late={(pair ? pair[0].record : trending)?.late_drops} />
      {pair ? (
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          {pair.map(({ kw, record, reach, voices: kwVoices }, i) => (
            <TrendingPanel
              key={kw}
              record={record}
              reaches={reach ? [reach] : []}
              voices={kwVoices}
              title={`Trending around "${kw}"`}
              barClass={i === 0 ? "bg-accent" : "bg-accent2"}
            />
          ))}
        </div>
      ) : (
        <div className="mx-auto w-full max-w-3xl">
          <TrendingPanel record={trending} reaches={reachList} voices={voices} />
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
      ~{fmt(late.total)} records were dropped as <b>late</b>: the replayed
      data is older than Flink&apos;s event-time watermark, so trends and the
      sentiment graph ignore it (newly added keywords will not appear). Use{" "}
      <b>Reset pipeline</b> on the Pipeline tab, then replay — the fresh job
      re-windows the data with the current keyword set.
    </div>
  );
}

// The HyperLogLog side of the story: ~unique authors in the latest window,
// kept to a single quiet line so the trends stay the hero. Several records
// (merged view) read as per-keyword figures - the HLLs are only mergeable
// as sketches, and the dashboard receives cardinalities, so no combined
// total is shown. The sketch detail lives in the hover tooltip.
function ReachLine({ reaches }) {
  if (!reaches || reaches.length === 0) return null;
  const hll = reaches[0].sketch;
  const detail = hll
    ? `HyperLogLog, ${1 << hll.precision} registers, ±${(
        hll.std_error * 100
      ).toFixed(1)}% typical error`
    : "HyperLogLog estimate";
  if (reaches.length === 1) {
    const r = reaches[0];
    return (
      <p className="mb-3 text-xs text-muted" title={detail}>
        ~<span className="font-medium text-text">{fmt(r.unique_authors)}</span>{" "}
        unique authors · ~{fmt(r.comment_count)} comments this window
      </p>
    );
  }
  return (
    <p className="mb-3 text-xs text-muted" title={detail}>
      unique authors:{" "}
      {reaches.map((r, i) => (
        <span key={r.keyword} className="whitespace-nowrap">
          {i > 0 && " · "}
          <span className="text-accent">{r.keyword}</span> ~
          <span className="font-medium text-text">{fmt(r.unique_authors)}</span>
        </span>
      ))}
    </p>
  );
}

function TrendingPanel({ record, reaches = [], voices = [], title, barClass = "bg-accent" }) {
  const items = record?.items || [];
  const tracked = record?.keywords || [];
  const sketch = record?.sketch;
  const hll = reaches[0]?.sketch;

  return (
    <Panel
      title={title || "Trending now"}
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
      <ReachLine reaches={reaches} />
      <TrendChart history={record?.history} />
      {record?.window_end && items.length === 0 ? (
        // A window CAN close with nothing rankable in it: terms used by a
        // single comment are dropped at the source (TRENDING_MIN_COUNT), so
        // a near-empty window publishes no items. Say so instead of listing
        // one-off vocabulary as if it were a trend.
        <p className="text-sm text-muted">
          This window is too quiet to rank trends — no term appeared in more
          than one comment. Waiting for a busier window…
        </p>
      ) : (
        <TrendingList
          items={items}
          showKeyword={!title && tracked.length > 1}
          barClass={barClass}
        />
      )}
      <TrendVoices terms={voices} />
      {sketch && (
        <p className="mt-4 border-t border-edge pt-3 text-[11px] leading-relaxed text-muted">
          Count-Min Sketch {sketch.depth}×{sketch.width} per keyword (~
          {Math.round((sketch.depth * sketch.width * 8) / 1024)} KB each, flat)
          over ~{fmt(sketch.stream_total)} terms — estimates may slightly
          overcount, never undercount. Ranked by count × distinctiveness
          (Zipf frequency vs everyday English), so generic words need many
          times the mentions of a topical one. ▲▼ compare against the
          previous window.
          {hll && (
            <>
              {" "}
              Reach: HyperLogLog with {1 << hll.precision} registers per
              keyword (~{Math.round((1 << hll.precision) / 1024)} KB, flat), ±
              {(hll.std_error * 100).toFixed(1)}% typical error — duplicates
              from the same author are ignored by construction.
            </>
          )}
        </p>
      )}
    </Panel>
  );
}

