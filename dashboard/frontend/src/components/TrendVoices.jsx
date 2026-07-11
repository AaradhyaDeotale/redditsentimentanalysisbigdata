// "What people are saying": real comments behind the top trending terms.
// The Count-Min sketch only keeps counts - these snippets come from the
// live-feed buffer (/api/trending/examples), so each trend word is backed
// by the actual conversation that made it trend. Visual language follows
// CommentFeed: sentiment sign + score in front, muted author line below.

const scoreColor = (label) => {
  if (label === "positive") return "text-pos";
  if (label === "negative") return "text-neg";
  return "text-muted";
};

const escapeRegex = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

// Mirrors the server's matcher: whole words, any non-word separator inside
// a phrase, case-insensitive - so the highlight lands on what matched.
const termRegex = (term) =>
  new RegExp(
    "\\b" + term.split(/\s+/).map(escapeRegex).join("\\W+") + "\\b",
    "gi",
  );

// The snippet with every occurrence of the term wrapped in an accent mark.
function Highlighted({ text, term }) {
  const parts = (text || "").split(termRegex(term));
  const hits = (text || "").match(termRegex(term)) || [];
  return (
    <>
      {parts.map((part, i) => (
        <span key={i}>
          {part}
          {i < hits.length && (
            <mark className="rounded bg-accent/15 px-0.5 font-medium text-accent">
              {hits[i]}
            </mark>
          )}
        </span>
      ))}
    </>
  );
}

function VoiceRow({ comment, term }) {
  const sign =
    comment.sentiment_label === "negative"
      ? "-"
      : comment.sentiment_label === "positive"
        ? "+"
        : "";
  return (
    <li className="flex gap-3 py-1.5">
      <span
        className={
          "shrink-0 font-mono text-xs " + scoreColor(comment.sentiment_label)
        }
        title={comment.sentiment_label}
      >
        {sign}
        {Math.abs(comment.sentiment_score ?? 0).toFixed(2)}
      </span>
      <div className="min-w-0">
        <p className="text-sm leading-snug text-text">
          <Highlighted text={comment.body} term={term} />
        </p>
        <p className="mt-0.5 text-xs text-muted">u/{comment.author}</p>
      </div>
    </li>
  );
}

export default function TrendVoices({ terms }) {
  const withComments = (terms || []).filter(
    (t) => (t.comments || []).length > 0,
  );
  if (withComments.length === 0) return null;
  return (
    <div className="mt-4 border-t border-edge pt-3">
      <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted">
        What people are saying
      </p>
      {withComments.map((t) => (
        <div key={t.token} className="mb-3 last:mb-0">
          <p className="mb-0.5 text-xs text-muted">
            on <span className="font-medium text-text">{t.token}</span>
          </p>
          <ul className="divide-y divide-edge/40">
            {t.comments.map((c) => (
              <VoiceRow key={c.id} comment={c} term={t.token} />
            ))}
          </ul>
        </div>
      ))}
    </div>
  );
}
