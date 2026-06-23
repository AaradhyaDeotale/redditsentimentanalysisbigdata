import { useState } from "react";
import { usePoll } from "../lib/usePoll.js";
import {
  getControlStatus,
  resetOffset,
  resetPipeline,
  startProducer,
  stopProducer,
} from "../lib/api.js";
import { Panel } from "./Panel.jsx";

function NumberField({ label, value, onChange, min, max, step = 1, disabled }) {
  return (
    <label className="flex flex-col gap-1">
      <span className="text-xs text-muted">{label}</span>
      <input
        type="number"
        value={value}
        min={min}
        max={max}
        step={step}
        disabled={disabled}
        onChange={(e) => onChange(Number(e.target.value))}
        className="w-28 rounded-lg border border-edge bg-bg/40 px-3 py-2 text-sm text-text outline-none focus:border-accent disabled:opacity-50"
      />
    </label>
  );
}

export default function ControlPanel() {
  const { data } = usePoll(getControlStatus, 2000);
  const [speed, setSpeed] = useState(2);
  const [limit, setLimit] = useState(60000);
  const [parallelism, setParallelism] = useState(2);
  const [windowSec, setWindowSec] = useState(60);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  const [confirmReset, setConfirmReset] = useState(false);

  if (!data) return null;

  if (!data.enabled) {
    return (
      <Panel title="Manual controls">
        <p className="text-sm text-muted">
          Disabled. Run the dashboard locally with{" "}
          <code className="rounded bg-bg/60 px-1.5 py-0.5 text-accent">
            CONTROL_ENABLED=true
          </code>{" "}
          to start/stop the producer and reset the pipeline from here.
        </p>
      </Panel>
    );
  }

  const prod = data.producer;
  const reset = data.reset;
  const resetting = reset.state === "running";
  const pct = prod.total ? Math.round((prod.sent / prod.total) * 100) : 0;

  async function call(fn) {
    setBusy(true);
    setErr(null);
    try {
      await fn();
    } catch (e) {
      setErr(String(e.message || e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="space-y-4">
      <Panel
        title="Replay producer"
        right={
          <span className="flex items-center gap-1.5 text-xs text-muted">
            <span
              className={
                "h-1.5 w-1.5 rounded-full " +
                (prod.running ? "bg-pos" : "bg-muted")
              }
            />
            {prod.running ? (prod.loading ? "loading…" : "running") : "idle"}
          </span>
        }
      >
        <div className="flex flex-wrap items-end gap-3">
          <NumberField
            label="Speed (x)"
            value={speed}
            onChange={setSpeed}
            min={0.1}
            max={1000}
            step={0.5}
            disabled={prod.running}
          />
          <NumberField
            label="Limit (records)"
            value={limit}
            onChange={setLimit}
            min={1000}
            max={5000000}
            step={10000}
            disabled={prod.running}
          />
          <button
            onClick={() => call(() => startProducer(speed, limit))}
            disabled={prod.running || busy || resetting}
            className="rounded-lg bg-accent px-4 py-2 text-sm font-semibold text-bg hover:opacity-90 disabled:opacity-40"
          >
            Start replay
          </button>
          <button
            onClick={() => call(stopProducer)}
            disabled={!prod.running || busy}
            className="rounded-lg border border-edge px-4 py-2 text-sm font-medium text-text hover:border-neg hover:text-neg disabled:opacity-40"
          >
            Stop
          </button>
        </div>

        <div className="mt-3 flex flex-wrap items-center gap-3 text-xs text-muted">
          <span>
            next slice:{" "}
            <span className="text-text">
              records {(prod.offset ?? 0).toLocaleString()}–
              {((prod.offset ?? 0) + Number(limit || 0)).toLocaleString()}
            </span>{" "}
            · each Start streams the next slice
          </span>
          <button
            onClick={() => call(resetOffset)}
            disabled={prod.running || busy}
            className="rounded border border-edge px-2 py-1 hover:text-text disabled:opacity-40"
          >
            ↺ reset offset
          </button>
        </div>

        {prod.running && (
          <div className="mt-4">
            <div className="h-2 w-full overflow-hidden rounded-full bg-bg/60">
              <div
                className="h-full bg-accent transition-all"
                style={{ width: `${prod.loading ? 8 : pct}%` }}
              />
            </div>
            <div className="mt-1.5 flex justify-between text-xs text-muted">
              <span>
                {prod.loading
                  ? "loading + filtering records…"
                  : `${prod.sent.toLocaleString()} / ${prod.total.toLocaleString()} sent`}
              </span>
              <span>{prod.loading ? "" : `${pct}%`}</span>
            </div>
          </div>
        )}
        {prod.last_log && (
          <p className="mt-2 truncate font-mono text-xs text-muted">
            {prod.last_log}
          </p>
        )}
      </Panel>

      <Panel title="Pipeline reset & Flink config">
        <div className="flex flex-wrap items-end gap-3">
          <NumberField
            label="Parallelism"
            value={parallelism}
            onChange={setParallelism}
            min={1}
            max={4}
            disabled={resetting}
          />
          <NumberField
            label="Window (s)"
            value={windowSec}
            onChange={setWindowSec}
            min={5}
            max={3600}
            step={5}
            disabled={resetting}
          />
          {!confirmReset ? (
            <button
              onClick={() => setConfirmReset(true)}
              disabled={resetting || busy}
              className="rounded-lg border border-neg/60 px-4 py-2 text-sm font-medium text-neg hover:bg-neg/10 disabled:opacity-40"
            >
              Reset pipeline…
            </button>
          ) : (
            <div className="flex items-center gap-2">
              <button
                onClick={() =>
                  call(() => resetPipeline(parallelism, windowSec)).then(() =>
                    setConfirmReset(false),
                  )
                }
                disabled={resetting || busy}
                className="rounded-lg bg-neg px-4 py-2 text-sm font-semibold text-bg hover:opacity-90 disabled:opacity-40"
              >
                Confirm — clears topics & restarts Flink
              </button>
              <button
                onClick={() => setConfirmReset(false)}
                className="rounded-lg border border-edge px-3 py-2 text-sm text-muted hover:text-text"
              >
                Cancel
              </button>
            </div>
          )}
        </div>

        {reset.state !== "idle" && (
          <div className="mt-4">
            <div className="mb-1 text-xs text-muted">
              reset: <span className="text-text">{reset.state}</span>
            </div>
            <pre className="max-h-40 overflow-y-auto rounded-lg border border-edge bg-bg/40 p-3 font-mono text-xs text-muted">
              {reset.log.join("\n") || "…"}
            </pre>
          </div>
        )}
        <p className="mt-2 text-xs text-muted">
          Flink parallelism/window apply on reset (the job restarts — they can't
          change live).
        </p>
      </Panel>

      {err && (
        <div className="rounded-lg border border-neg/40 bg-neg/10 px-4 py-2 text-sm text-neg">
          {err}
        </div>
      )}
    </div>
  );
}
