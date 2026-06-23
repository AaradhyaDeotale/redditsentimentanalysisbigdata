import { useCallback, useEffect, useState } from "react";
import { getCompare, getComments } from "../lib/api.js";
import { useWebSocket } from "../lib/useWebSocket.js";
import {
  applyMessage,
  initialState,
  MAX_COMMENTS,
  MAX_POINTS,
} from "../lib/messages.js";
import SentimentCard from "../components/SentimentCard.jsx";
import SentimentChart from "../components/SentimentChart.jsx";
import CommentFeed from "../components/CommentFeed.jsx";

const toPoints = (pts) =>
  (pts || [])
    .map((p) => ({
      window_end: p.window_end,
      positive_ratio: p.positive_ratio,
      comment_count: p.comment_count,
    }))
    .slice(-MAX_POINTS);

export default function SentimentTab() {
  const [kw1, setKw1] = useState("apple");
  const [kw2, setKw2] = useState("android");
  const [active, setActive] = useState(["apple", "android"]);
  const [state, setState] = useState(initialState);
  const [error, setError] = useState(null);

  const onMessage = useCallback((msg) => {
    setState((s) => applyMessage(s, msg));
  }, []);
  const { subscribe, connected } = useWebSocket(onMessage);

  // On keyword change: backfill history + comments over REST, seed state, then
  // subscribe over the WebSocket so live deltas append from there on.
  useEffect(() => {
    let cancelled = false;
    const [a, b] = active;
    setState(initialState);
    setError(null);
    (async () => {
      try {
        const [cmp, c1, c2] = await Promise.all([
          getCompare(a, b),
          getComments(a),
          getComments(b),
        ]);
        if (cancelled) return;
        const merged = [...c1.comments, ...c2.comments]
          .sort((x, y) => (y.created_utc || 0) - (x.created_utc || 0))
          .slice(0, MAX_COMMENTS);
        setState({
          windows: {
            [a]: toPoints(cmp.keyword1.points),
            [b]: toPoints(cmp.keyword2.points),
          },
          comments: merged,
        });
      } catch (e) {
        if (!cancelled) setError(String(e.message || e));
      } finally {
        if (!cancelled) subscribe([a, b]);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [active, subscribe]);

  function applyCompare(e) {
    e.preventDefault();
    const a = kw1.trim().toLowerCase();
    const b = kw2.trim().toLowerCase();
    if (a && b) setActive([a, b]);
  }

  const [a, b] = active;
  const seriesA = state.windows[a] || [];
  const seriesB = state.windows[b] || [];

  return (
    <div className="space-y-5">
      <form className="flex flex-wrap items-end gap-3" onSubmit={applyCompare}>
        <Field label="Keyword 1" value={kw1} onChange={setKw1} />
        <Field label="Keyword 2" value={kw2} onChange={setKw2} />
        <button
          type="submit"
          className="rounded-lg bg-accent px-5 py-2.5 text-sm font-semibold text-bg hover:opacity-90"
        >
          Compare
        </button>
        <span className="ml-auto flex items-center gap-1.5 text-xs text-muted">
          <span
            className={
              "h-1.5 w-1.5 rounded-full " + (connected ? "bg-pos" : "bg-neg")
            }
          />
          {connected ? "streaming" : "reconnecting…"}
        </span>
      </form>

      {error && (
        <div className="rounded-lg border border-neg/40 bg-neg/10 px-4 py-2 text-sm text-neg">
          {error}
        </div>
      )}

      <div className="flex flex-wrap gap-4">
        <SentimentCard keyword={a} points={seriesA} accentClass="text-accent" />
        <SentimentCard keyword={b} points={seriesB} accentClass="text-accent2" />
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
        <div className="lg:col-span-2">
          <SentimentChart a={a} b={b} seriesA={seriesA} seriesB={seriesB} />
        </div>
        <div className="h-72 lg:h-auto">
          <CommentFeed comments={state.comments} keywords={active} />
        </div>
      </div>
    </div>
  );
}

function Field({ label, value, onChange }) {
  return (
    <label className="flex flex-col gap-1">
      <span className="text-xs text-muted">{label}</span>
      <input
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="min-w-40 rounded-lg border border-edge bg-card px-3 py-2.5 text-sm text-text outline-none focus:border-accent"
      />
    </label>
  );
}
