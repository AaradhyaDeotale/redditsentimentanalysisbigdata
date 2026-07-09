function scoreColor(label) {
  if (label === "positive") return "text-pos";
  if (label === "negative") return "text-neg";
  return "text-muted";
}

// "apple" -> "apple (company)" when keyword_senses resolved it; plain
// (unambiguous, or still-ambiguous) keywords are shown as-is.
function keywordLabel(kw, senses) {
  const sense = senses?.[kw];
  return sense ? `${kw} (${sense})` : kw;
}

function CommentRow({ c }) {
  const sign =
    c.sentiment_label === "negative"
      ? "-"
      : c.sentiment_label === "positive"
        ? "+"
        : "";
  const keywords = c.matched_keywords || [];
  return (
    <li className="flex gap-3 border-b border-edge/60 px-1 py-2.5 last:border-0">
      <span
        className={"shrink-0 font-mono text-xs " + scoreColor(c.sentiment_label)}
        title={c.sentiment_label}
      >
        {sign}
        {Math.abs(c.sentiment_score ?? 0).toFixed(2)}
      </span>
      <div className="min-w-0">
        <p
          className="truncate text-sm text-text"
          title={c.body || "(no text)"}
        >
          {c.body || "(no text)"}
        </p>
        <p className="mt-0.5 flex flex-wrap items-center gap-x-1.5 gap-y-1 text-xs text-muted">
          <span>u/{c.author}</span>
          {keywords.map((kw) => (
            <span
              key={kw}
              className="rounded-full border border-edge px-1.5 py-0.5 lowercase text-muted"
            >
              {keywordLabel(kw, c.keyword_senses)}
            </span>
          ))}
        </p>
      </div>
    </li>
  );
}

// Live-scrolling feed of scored comments relevant to the selected keywords.
export default function CommentFeed({ comments, keywords }) {
  const set = new Set(keywords.map((k) => k.toLowerCase()));
  const visible = comments.filter((c) =>
    (c.matched_keywords || []).some((k) => set.has(String(k).toLowerCase())),
  );

  return (
    <div className="flex h-full flex-col rounded-xl border border-edge bg-card p-4">
      <div className="mb-2 flex items-center justify-between">
        <h3 className="text-sm font-semibold">Live comments</h3>
        <span className="text-xs text-muted">{visible.length} shown</span>
      </div>
      {visible.length === 0 ? (
        <p className="py-8 text-center text-sm text-muted">
          waiting for comments…
        </p>
      ) : (
        <ul className="overflow-y-auto pr-1">
          {visible.map((c) => (
            <CommentRow key={c.id} c={c} />
          ))}
        </ul>
      )}
    </div>
  );
}
