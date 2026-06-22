import { usePoll } from "../lib/usePoll.js";
import {
  getFlinkOverview,
  getKafkaOverview,
  getKafkaTopics,
} from "../lib/api.js";

const DOT = {
  ok: "bg-pos",
  warn: "bg-accent2",
  down: "bg-neg",
  unknown: "bg-muted",
};
const RING = {
  ok: "border-pos/40",
  warn: "border-accent2/40",
  down: "border-neg/40",
  unknown: "border-edge",
};

function Node({ name, status, detail }) {
  return (
    <div
      className={"flex-1 rounded-xl border bg-card p-4 text-center " + RING[status]}
    >
      <div className="flex items-center justify-center gap-2">
        <span className={"h-2 w-2 rounded-full " + DOT[status]} />
        <span className="text-sm font-semibold">{name}</span>
      </div>
      <div className="mt-1 text-xs text-muted">{detail}</div>
    </div>
  );
}

export default function OverviewTab({ meta }) {
  const { data } = usePoll(async () => {
    const [kafka, topics, flink] = await Promise.all([
      getKafkaOverview(),
      getKafkaTopics(),
      getFlinkOverview(),
    ]);
    return { kafka, topics, flink };
  });

  const kafka = data?.kafka;
  const topics = data?.topics;
  const flink = data?.flink;

  const topicNames = new Set(
    topics?.available ? topics.topics.map((t) => t.name) : [],
  );
  const has = (name) => topicNames.has(name);

  const kafkaOk = !!kafka?.available;
  const flinkAvail = !!flink?.available;
  const jobsRunning = flinkAvail ? flink.overview["jobs-running"] ?? 0 : 0;

  const nodes = [
    {
      name: "Producer",
      status: !kafkaOk ? "unknown" : has("reddit-comments") ? "ok" : "warn",
      detail: !kafkaOk
        ? "no signal"
        : has("reddit-comments")
          ? "reddit-comments present"
          : "topic missing",
    },
    {
      name: "Kafka",
      status: kafkaOk ? "ok" : "down",
      detail: kafkaOk
        ? `${kafka.brokers.length} brokers · ${kafka.topic_count} topics`
        : "unreachable",
    },
    {
      name: "Flink",
      status: !flinkAvail ? "down" : jobsRunning > 0 ? "ok" : "warn",
      detail: !flinkAvail
        ? "unreachable"
        : `${jobsRunning} job(s) running`,
    },
    {
      name: "ML scorer",
      status: !kafkaOk
        ? "unknown"
        : has("reddit-comments-cleaned")
          ? "ok"
          : "warn",
      detail: has("reddit-comments-cleaned")
        ? "scored stream present"
        : "no scored output",
    },
    {
      name: "Dashboard",
      status: meta ? "ok" : "warn",
      detail: meta
        ? `${meta.mode} · ${meta.known_keywords.length} keywords`
        : "starting…",
    },
  ];

  return (
    <div className="space-y-4">
      <p className="text-sm text-muted">
        End-to-end pipeline health. Each node reflects the best signal available
        from Kafka, Flink, and the dashboard itself.
      </p>
      <div className="flex flex-col items-stretch gap-2 sm:flex-row sm:items-center">
        {nodes.map((n, i) => (
          <div key={n.name} className="flex flex-1 items-center gap-2">
            <Node {...n} />
            {i < nodes.length - 1 && (
              <span className="hidden text-muted sm:inline">→</span>
            )}
          </div>
        ))}
      </div>
      {!data && <p className="text-xs text-muted">polling…</p>}
    </div>
  );
}
