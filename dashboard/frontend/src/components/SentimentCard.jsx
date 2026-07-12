import { headlineStat } from "../lib/aggregate.js";

// One keyword's headline: % positive over the most recent bucket at the chart's
// chosen granularity (so it isn't a lone 1-comment window reading 100%), the
// sample size behind it, and a trend arrow versus the previous bucket.
export default function SentimentCard({
  keyword,
  points,
  bucketSeconds,
  bucketLabel,
  accentClass,
}) {
  const stat = headlineStat(points, bucketSeconds);
  const delta = stat?.deltaPct ?? null;

  const arrow = delta == null ? "" : delta > 0 ? "▲" : delta < 0 ? "▼" : "■";
  const arrowClass =
    delta == null || delta === 0
      ? "text-muted"
      : delta > 0
        ? "text-pos"
        : "text-neg";

  return (
    <div className="flex-1 rounded-xl border border-edge bg-card p-5">
      <div className={"text-sm lowercase " + (accentClass || "text-muted")}>
        {keyword || "—"}
      </div>
      <div className="mt-1.5 text-4xl font-bold">
        {stat ? `${stat.pct}%` : "--%"}
      </div>
      <div className="mt-1 flex items-center gap-2 text-xs text-muted">
        <span>positive</span>
        {stat && delta != null && (
          <span className={arrowClass}>
            {arrow} {Math.abs(delta)}%
          </span>
        )}
      </div>
      <div className="mt-0.5 text-xs text-muted">
        {stat
          ? `${stat.count} comment${stat.count === 1 ? "" : "s"} · last ${bucketLabel}`
          : "waiting for data"}
      </div>
    </div>
  );
}
