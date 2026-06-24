import { useEffect, useState } from "react";
import { usePoll } from "../lib/usePoll.js";
import { addKeyword, getKeywords, removeKeyword } from "../lib/api.js";
import { Panel } from "./Panel.jsx";

// The tracked-keyword manager. Writes go to Redis via the API; the Flink job
// re-reads that set every few seconds, so adds/removes change what the pipeline
// scores *live* (no restart). Polls so it reflects changes from other tabs too.
export default function TrackedKeywords({ onKeywords }) {
  const { data } = usePoll(getKeywords, 3000);
  const [list, setList] = useState([]);
  const [draft, setDraft] = useState("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);

  // Reconcile with the server poll unless we're mid-write (avoid clobbering an
  // optimistic update with a stale poll that hasn't observed it yet).
  useEffect(() => {
    if (data?.keywords && !busy) setList(data.keywords);
  }, [data, busy]);

  useEffect(() => {
    onKeywords?.(list);
  }, [list, onKeywords]);

  async function mutate(fn, optimistic) {
    setBusy(true);
    setErr(null);
    setList(optimistic);
    try {
      const res = await fn();
      setList(res.keywords);
    } catch (e) {
      setErr(String(e.message || e));
    } finally {
      setBusy(false);
    }
  }

  function onAdd(e) {
    e.preventDefault();
    const kw = draft.trim().toLowerCase();
    if (!kw || list.includes(kw)) {
      setDraft("");
      return;
    }
    setDraft("");
    mutate(() => addKeyword(kw), [...list, kw].sort());
  }

  function onRemove(kw) {
    mutate(() => removeKeyword(kw), list.filter((k) => k !== kw));
  }

  return (
    <Panel
      title="Tracked keywords"
      right={
        <span className="text-xs text-muted">
          live · the pipeline scores these
        </span>
      }
    >
      <div className="flex flex-wrap items-center gap-2">
        {list.length === 0 && (
          <span className="text-sm text-muted">none tracked yet</span>
        )}
        {list.map((kw) => (
          <span
            key={kw}
            className="flex items-center gap-1.5 rounded-full border border-edge bg-bg/40 py-1 pl-3 pr-1.5 text-sm text-text"
          >
            {kw}
            <button
              onClick={() => onRemove(kw)}
              disabled={busy}
              title={`stop tracking "${kw}"`}
              className="flex h-5 w-5 items-center justify-center rounded-full text-muted hover:bg-neg/20 hover:text-neg disabled:opacity-40"
            >
              ×
            </button>
          </span>
        ))}
      </div>

      <form className="mt-3 flex items-end gap-2" onSubmit={onAdd}>
        <input
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          placeholder="add a keyword…"
          className="min-w-44 rounded-lg border border-edge bg-bg/40 px-3 py-2 text-sm text-text outline-none focus:border-accent"
        />
        <button
          type="submit"
          disabled={busy || !draft.trim()}
          className="rounded-lg bg-accent px-4 py-2 text-sm font-semibold text-bg hover:opacity-90 disabled:opacity-40"
        >
          Add
        </button>
      </form>

      {err && (
        <p className="mt-2 text-xs text-neg">{err}</p>
      )}
      <p className="mt-2 text-xs text-muted">
        New keywords take effect in the stream within a few seconds. Removing one
        stops new data but keeps its history — re-add it to resume.
      </p>
    </Panel>
  );
}
