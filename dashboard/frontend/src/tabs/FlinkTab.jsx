import { usePoll } from "../lib/usePoll.js";
import { getFlinkJobs, getFlinkOverview } from "../lib/api.js";
import { Panel, Stat, Unreachable } from "../components/Panel.jsx";

function fmtDuration(ms) {
  if (ms == null) return "—";
  const s = Math.floor(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  if (h) return `${h}h ${m}m`;
  if (m) return `${m}m ${sec}s`;
  return `${sec}s`;
}

function stateClass(state) {
  if (state === "RUNNING") return "text-pos";
  if (state === "FAILED") return "text-neg";
  return "text-muted";
}

export default function FlinkTab() {
  const { data } = usePoll(async () => {
    const [overview, jobs] = await Promise.all([
      getFlinkOverview(),
      getFlinkJobs(),
    ]);
    return { overview, jobs };
  });

  if (!data) return <p className="text-sm text-muted">Loading job manager…</p>;
  const { overview, jobs } = data;

  if (!overview.available) {
    return <Unreachable what="Flink JobManager" error={overview.error} />;
  }

  const o = overview.overview;
  return (
    <div className="space-y-5">
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <Stat label="Task managers" value={o["taskmanagers"] ?? "—"} />
        <Stat
          label="Slots (free / total)"
          value={`${o["slots-available"] ?? "—"} / ${o["slots-total"] ?? "—"}`}
        />
        <Stat label="Jobs running" value={o["jobs-running"] ?? "—"} />
        <Stat label="Version" value={o["flink-version"] ?? "—"} />
      </div>

      <Panel
        title="Jobs"
        right={jobs.available ? null : <span className="text-xs text-neg">unavailable</span>}
      >
        {jobs.available && jobs.jobs.length === 0 && (
          <p className="text-sm text-muted">No jobs submitted.</p>
        )}
        {jobs.available && jobs.jobs.length > 0 && (
          <table className="w-full text-sm">
            <thead className="text-left text-xs text-muted">
              <tr>
                <th className="pb-2 font-medium">Job</th>
                <th className="pb-2 font-medium">State</th>
                <th className="pb-2 font-medium">Uptime</th>
              </tr>
            </thead>
            <tbody>
              {jobs.jobs.map((j) => (
                <tr key={j.jid} className="border-t border-edge/60">
                  <td className="py-1.5">{j.name || j.jid}</td>
                  <td className={"py-1.5 font-medium " + stateClass(j.state)}>
                    {j.state}
                  </td>
                  <td className="py-1.5 tabular-nums">
                    {fmtDuration(j.duration)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </Panel>

      <p className="text-xs text-muted">
        Full operator metrics & backpressure:{" "}
        <a
          href="http://localhost:8081"
          target="_blank"
          rel="noreferrer"
          className="text-accent hover:underline"
        >
          open the Flink web UI ↗
        </a>
      </p>
    </div>
  );
}
