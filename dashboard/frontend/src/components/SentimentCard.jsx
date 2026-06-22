// One keyword's headline: latest % positive, comment count, and a trend arrow
// versus the previous window.
export default function SentimentCard({ keyword, points, accentClass }) {
  const last = points[points.length - 1];
  const prev = points[points.length - 2];
  const pct = last != null ? Math.round(last.positive_ratio * 100) : null;
  const delta =
    last && prev ? Math.round((last.positive_ratio - prev.positive_ratio) * 100) : 0;

  const arrow = delta > 0 ? "▲" : delta < 0 ? "▼" : "■";
  const arrowClass =
    delta > 0 ? "text-pos" : delta < 0 ? "text-neg" : "text-muted";

  return (
    <div className="flex-1 rounded-xl border border-edge bg-card p-5">
      <div className={"text-sm lowercase " + (accentClass || "text-muted")}>
        {keyword || "—"}
      </div>
      <div className="mt-1.5 text-4xl font-bold">
        {pct != null ? `${pct}%` : "--%"}
      </div>
      <div className="mt-1 flex items-center gap-2 text-xs text-muted">
        <span>positive</span>
        {last && (
          <span className={arrowClass}>
            {arrow} {Math.abs(delta)}%
          </span>
        )}
      </div>
      <div className="mt-0.5 text-xs text-muted">
        {last
          ? `${last.comment_count} comments in last window`
          : "waiting for data"}
      </div>
    </div>
  );
}
