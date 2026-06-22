import { usePoll } from "../lib/usePoll.js";
import {
  getKafkaGroups,
  getKafkaOverview,
  getKafkaTopics,
} from "../lib/api.js";
import { Panel, Stat, Unreachable } from "../components/Panel.jsx";

export default function KafkaTab() {
  const { data } = usePoll(async () => {
    const [overview, topics, groups] = await Promise.all([
      getKafkaOverview(),
      getKafkaTopics(),
      getKafkaGroups(),
    ]);
    return { overview, topics, groups };
  });

  if (!data) return <p className="text-sm text-muted">Loading cluster…</p>;
  const { overview, topics, groups } = data;

  if (!overview.available) {
    return <Unreachable what="Kafka cluster" error={overview.error} />;
  }

  return (
    <div className="space-y-5">
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <Stat label="Brokers" value={overview.brokers.length} />
        <Stat label="Controller" value={`#${overview.controller_id}`} />
        <Stat label="Topics" value={overview.topic_count} />
        <Stat
          label="Consumer groups"
          value={groups.available ? groups.groups.length : "—"}
        />
      </div>

      <Panel title="Brokers">
        <div className="flex flex-wrap gap-2">
          {overview.brokers.map((b) => (
            <span
              key={b.id}
              className="rounded-md border border-edge bg-bg/40 px-3 py-1.5 text-xs"
            >
              <span className="text-muted">#{b.id}</span> {b.host}:{b.port}
              {b.id === overview.controller_id && (
                <span className="ml-1.5 text-accent">★</span>
              )}
            </span>
          ))}
        </div>
      </Panel>

      <Panel title="Topics" right={topics.available ? null : <span className="text-xs text-neg">unavailable</span>}>
        {topics.available && (
          <table className="w-full text-sm">
            <thead className="text-left text-xs text-muted">
              <tr>
                <th className="pb-2 font-medium">Topic</th>
                <th className="pb-2 font-medium">Partitions</th>
              </tr>
            </thead>
            <tbody>
              {topics.topics.map((t) => (
                <tr key={t.name} className="border-t border-edge/60">
                  <td className="py-1.5">
                    {t.name}
                    {t.internal && (
                      <span className="ml-2 text-xs text-muted">internal</span>
                    )}
                  </td>
                  <td className="py-1.5 tabular-nums">{t.partitions}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </Panel>

      {groups.available && groups.groups.length > 0 && (
        <Panel title="Consumer groups">
          <ul className="space-y-1.5 text-sm">
            {groups.groups.map((g) => (
              <li key={g.id} className="flex items-center justify-between">
                <span>{g.id}</span>
                <span className="text-xs text-muted">{g.state}</span>
              </li>
            ))}
          </ul>
        </Panel>
      )}
    </div>
  );
}
