import { usePoll } from "../lib/usePoll.js";
import {
  getControlStatus,
  getFlinkOverview,
  getKafkaOverview,
  getKafkaTopics,
  getKeywords,
} from "../lib/api.js";
import ControlPanel from "../components/ControlPanel.jsx";

const COLORS = {
  flowing: "text-pos border-pos",
  ok: "text-pos border-pos",
  warn: "text-accent2 border-accent2",
  down: "text-neg border-neg",
  idle: "text-muted border-edge",
};
const BG = {
  flowing: "border-pos bg-pos/10 shadow-[0_0_10px_rgba(63,185,80,0.25)]",
  ok: "border-pos/60 bg-pos/5",
  warn: "border-accent2/60 bg-accent2/5",
  down: "border-neg/60 bg-neg/5",
  idle: "border-edge bg-card",
};
const DOT = {
  flowing: "bg-pos animate-pulse",
  ok: "bg-pos",
  warn: "bg-accent2",
  down: "bg-neg",
  idle: "bg-muted",
};

function deriveFlow({ kafka, topics, flink, control, keywords, meta }) {
  const topicNames = new Set(
    topics?.available ? topics.topics.map((t) => t.name) : [],
  );
  const has = (name) => topicNames.has(name);

  const kafkaOk = !!kafka?.available;
  const flinkAvail = !!flink?.available;
  const jobsRunning = flinkAvail ? flink.overview["jobs-running"] ?? 0 : 0;
  const flinkRunning = flinkAvail && jobsRunning > 0;
  const prodRunning = !!control?.producer?.running;
  const prodSent = control?.producer?.sent ?? 0;
  const hasProduced = prodRunning || prodSent > 0;

  const hasInput = has("reddit-comments");
  const hasCleaned = has("reddit-comments-cleaned");
  const hasResults = has("sentiment-results");
  const hasMalformed = has("reddit-comments-malformed");
  const kwCount = keywords?.keywords?.length ?? meta?.known_keywords?.length ?? 0;
  const hasKeywords = kwCount > 0;
  const live = meta?.mode === "live";
  const hasDashData = live && (meta?.known_keywords?.length ?? 0) > 0;

  const edge = (upstream, downstream) => {
    if (!upstream) return "down";
    if (upstream === "flowing") return "flowing";
    if (downstream) return "ok";
    if (upstream === "warn") return "warn";
    return "warn";
  };

  const nodes = {
    source: prodRunning ? "flowing" : hasProduced ? "ok" : "idle",
    producer: !kafkaOk ? "down" : prodRunning ? "flowing" : hasInput ? "ok" : "warn",
    input: !kafkaOk ? "down" : hasInput ? (prodRunning ? "flowing" : "ok") : "warn",
    flink: !kafkaOk ? "down" : !flinkAvail ? "down" : flinkRunning ? "ok" : "warn",
    malformed: !kafkaOk ? "down" : hasMalformed ? "ok" : flinkRunning ? "warn" : "idle",
    cleaned: !kafkaOk ? "down" : hasCleaned ? "ok" : flinkRunning ? "warn" : "idle",
    results: !kafkaOk ? "down" : hasResults ? "ok" : flinkRunning ? "warn" : "idle",
    dash: !live ? "warn" : hasDashData ? "ok" : hasResults || hasCleaned ? "warn" : "idle",
    ui: meta ? "ok" : "warn",
    model: hasCleaned ? "ok" : flinkRunning ? "warn" : "idle",
    redis: hasKeywords ? "ok" : "warn",
  };

  const edges = {
    sourceProducer: edge(nodes.source !== "idle" ? "ok" : "warn", prodRunning),
    producerInput: edge(
      !kafkaOk ? false : prodRunning ? "flowing" : hasInput ? "ok" : "warn",
      hasInput,
    ),
    inputFlink: edge(
      !kafkaOk ? false : hasInput && flinkRunning ? "ok" : hasInput ? "warn" : false,
      flinkRunning,
    ),
    flinkMalformed: edge(flinkRunning, hasMalformed),
    flinkCleaned: edge(flinkRunning, hasCleaned),
    flinkResults: edge(flinkRunning, hasResults),
    cleanedDash: edge(hasCleaned, hasDashData),
    resultsDash: edge(hasResults, hasDashData),
    dashUi: edge(meta != null, true),
    modelFlink: edge(hasCleaned ? "ok" : flinkRunning, flinkRunning),
    redisFlink: edge(hasKeywords, flinkRunning),
    redisDash: edge(hasKeywords, hasKeywords),
  };

  if (prodRunning) {
    edges.sourceProducer = "flowing";
    edges.producerInput = "flowing";
    if (flinkRunning) edges.inputFlink = "flowing";
  }

  const critical = [
    { label: "Producer", node: nodes.producer, hint: "Start replay or check Kafka" },
    { label: "reddit-comments topic", node: nodes.input, hint: "Producer has not written yet" },
    { label: "Flink Job", node: nodes.flink, hint: "Restart Flink job (see Flink tab)" },
    { label: "reddit-comments-cleaned", node: nodes.cleaned, hint: "Flink not scoring — check ML model" },
    { label: "sentiment-results", node: nodes.results, hint: "Wait for event-time windows to close" },
    { label: "Dashboard API", node: nodes.dash, hint: "Dashboard not receiving Kafka data" },
  ];

  const issue = critical.find((c) => c.node === "down" || c.node === "warn") ?? null;
  const allGreen =
    nodes.flink === "ok" && nodes.cleaned === "ok" && nodes.results === "ok" &&
    nodes.dash === "ok" && hasProduced;

  return {
    nodes, edges, issue, allGreen, prodRunning, prodSent, kwCount,
    flinkRunning, live, jobCount: jobsRunning,
  };
}

function StatusDot({ status }) {
  return (
    <span className={"absolute -right-1 -top-1 h-2 w-2 rounded-full border border-bg " + DOT[status]} />
  );
}

function Cylinder({ label, status, detail }) {
  return (
    <div className="relative flex flex-col items-center">
      <div className={"h-3 w-28 rounded-[50%] border-2 " + BG[status]} />
      <div className={"-mt-0.5 flex min-h-[2.5rem] w-28 flex-col items-center justify-center rounded-b-lg border-2 border-t-0 px-2 py-1 " + BG[status]}>
        <span className="text-center font-mono text-[9px] leading-tight">{label}</span>
        {detail && <span className="mt-0.5 text-[8px] text-muted">{detail}</span>}
      </div>
      <StatusDot status={status} />
    </div>
  );
}

function Box({ label, status, detail }) {
  return (
    <div className={"relative rounded-lg border-2 px-3 py-2 text-center " + BG[status]}>
      <div className="text-xs font-semibold">{label}</div>
      {detail && <div className="mt-0.5 text-[9px] text-muted">{detail}</div>}
      <StatusDot status={status} />
    </div>
  );
}

function Tag({ label, status }) {
  const cls =
    status === "ok" || status === "flowing" ? "bg-pos text-bg"
    : status === "warn" ? "bg-accent2 text-bg"
    : status === "down" ? "bg-neg text-bg"
    : "bg-muted text-bg";
  return <span className={"rounded px-2 py-0.5 text-[9px] font-semibold " + cls}>{label}</span>;
}

function HArrow({ status, label, dashed }) {
  const s = status ?? "idle";
  const pulse = s === "flowing" ? "animate-pulse" : "";
  const style = dashed ? "border-dashed" : "border-solid";
  return (
    <div className={"flex shrink-0 flex-col items-center gap-0.5 " + pulse}>
      <div className={"flex items-center " + COLORS[s]}>
        <div className={"h-0 w-8 border-t-2 " + style + " " + (s === "idle" ? "border-edge" : "border-current")} />
        <span className="text-[10px] leading-none">▶</span>
      </div>
      {label && <span className="max-w-[4rem] text-center text-[8px] text-muted">{label}</span>}
    </div>
  );
}

function UArrow({ status, dashed }) {
  const s = status ?? "idle";
  const style = dashed ? "border-dashed" : "border-solid";
  return (
    <div className={"flex flex-col items-center " + COLORS[s]}>
      <span className="text-[9px] leading-none">▲</span>
      <div className={"w-0 h-6 border-l-2 " + style + " " + (s === "idle" ? "border-edge" : "border-current")} />
    </div>
  );
}
function VArrow({ status, label, tall, dashed }) {
  const s = status ?? "idle";
  const style = dashed ? "border-dashed" : "border-solid";
  return (
    <div className={"flex flex-col items-center " + COLORS[s]}>
      <div className={"w-0 " + (tall ? "h-8" : "h-5") + " border-l-2 " + style + " " + (s === "idle" ? "border-edge" : "border-current")} />
      <span className="text-[9px] leading-none">▼</span>
      {label && <span className="mt-0.5 text-[8px] text-muted">{label}</span>}
    </div>
  );
}

function Section({ title, subtitle, children }) {
  return (
    <div className="rounded-xl border border-edge/60 bg-bg/20 p-4">
      <div className="mb-3 flex items-baseline gap-2">
        <h3 className="text-xs font-semibold uppercase tracking-wide text-muted">{title}</h3>
        {subtitle && <span className="text-[10px] text-muted">· {subtitle}</span>}
      </div>
      {children}
    </div>
  );
}

function IssueBanner({ issue, allGreen, prodRunning }) {
  if (allGreen && !prodRunning) {
    return (
      <div className="rounded-lg border border-pos/40 bg-pos/10 px-4 py-2.5 text-sm text-pos">
        Pipeline healthy — all stages green. Start a replay to see live flow.
      </div>
    );
  }
  if (allGreen && prodRunning) {
    return (
      <div className="rounded-lg border border-pos/40 bg-pos/10 px-4 py-2.5 text-sm text-pos">
        Data flowing — replay active, all stages green.
      </div>
    );
  }
  if (!issue) return null;
  const color = issue.node === "down"
    ? "border-neg/40 bg-neg/10 text-neg"
    : "border-accent2/40 bg-accent2/10 text-accent2";
  return (
    <div className={"rounded-lg border px-4 py-2.5 text-sm " + color}>
      <span className="font-semibold">Issue at {issue.label}</span>
      <span className="text-muted"> — </span>{issue.hint}
    </div>
  );
}

function FlowLegend() {
  return (
    <div className="flex flex-wrap gap-x-5 gap-y-1 text-[10px] text-muted">
      <span className="flex items-center gap-1.5"><span className="h-0.5 w-4 bg-pos" /> healthy / flowing</span>
      <span className="flex items-center gap-1.5"><span className="h-0.5 w-4 bg-accent2" /> waiting</span>
      <span className="flex items-center gap-1.5"><span className="h-0.5 w-4 bg-neg" /> blocked</span>
      <span className="flex items-center gap-1.5"><span className="h-0.5 w-4 border-t border-dashed border-muted" /> side-input</span>
    </div>
  );
}

function PipelineDiagram({ flow }) {
  const { nodes: n, edges: e } = flow;

  return (
    <div className="space-y-3">
      {/* 1 — Ingestion */}
      <Section title="1 · Ingestion" subtitle="solid data flow">
        <div className="flex flex-wrap items-center justify-center gap-2">
          <Box label="RC_2019-04.zst" status={n.source} detail={flow.prodRunning ? "replaying" : "dataset"} />
          <HArrow status={e.sourceProducer} />
          <Box label="Producer" status={n.producer} detail={flow.prodSent ? `${flow.prodSent.toLocaleString()} sent` : "P1"} />
          <HArrow status={e.producerInput} label="publish" />
          <Cylinder label="reddit-comments" status={n.input} detail="Kafka topic" />
        </div>
      </Section>

      <div className="flex justify-center">
        <VArrow status={e.inputFlink} label="consume" />
      </div>

      {/* 2 — Processing */}
      <Section title="2 · Stream processing" subtitle="Flink + side-inputs">
        <div className="flex flex-col items-center gap-3">
          <Box label="Flink Job" status={n.flink} detail={flow.flinkRunning ? `${flow.jobCount} running` : "stopped · P3"} />
          <span className="text-[9px] text-muted">parse → keyword → ML → window</span>

          <div className="grid w-full max-w-lg grid-cols-2 gap-6">
            <div className="flex flex-col items-center gap-2">
              <UArrow status={e.modelFlink} dashed />
              <Tag label="hot-reload" status={e.modelFlink} />
              <Cylinder label="ml-model store" status={n.model} detail={n.model === "ok" ? "scoring" : "P4"} />
            </div>
            <div className="flex flex-col items-center gap-2">
              <UArrow status={e.redisFlink} dashed />
              <Tag label="tracked keywords" status={e.redisFlink} />
              <Cylinder label="Redis keywords" status={n.redis} detail={`${flow.kwCount} tracked`} />
            </div>
          </div>
        </div>
      </Section>

      <div className="flex justify-center">
        <VArrow status={e.flinkCleaned} label="sink to Kafka" />
      </div>

      {/* 3 — Kafka outputs */}
      <Section title="3 · Kafka outputs" subtitle="Flink writes back to Kafka">
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
          <div className="flex flex-col items-center gap-2">
            <HArrow status={e.flinkMalformed} />
            <Cylinder label="reddit-comments-malformed" status={n.malformed} detail="bad records" />
          </div>
          <div className="flex flex-col items-center gap-2">
            <HArrow status={e.flinkCleaned} />
            <Cylinder label="reddit-comments-cleaned" status={n.cleaned} detail="scored comments" />
          </div>
          <div className="flex flex-col items-center gap-2">
            <HArrow status={e.flinkResults} />
            <Cylinder label="sentiment-results" status={n.results} detail="window aggregates" />
          </div>
        </div>
      </Section>

      <div className="flex justify-center gap-8">
        <VArrow status={e.cleanedDash} label="cleaned" />
        <VArrow status={e.resultsDash} label="results" />
      </div>

      {/* 4 — Dashboard */}
      <Section title="4 · Dashboard" subtitle="consume & display">
        <div className="flex flex-col items-center gap-4">
          <div className="flex flex-wrap items-end justify-center gap-6">
            <div className="flex flex-col items-center gap-1">
              <span className={"rounded border px-2 py-1 font-mono text-[9px] " + BG[n.cleaned]}>reddit-comments-cleaned</span>
              <VArrow status={e.cleanedDash} />
            </div>
            <div className="flex flex-col items-center gap-1">
              <span className={"rounded border px-2 py-1 font-mono text-[9px] " + BG[n.results]}>sentiment-results</span>
              <VArrow status={e.resultsDash} />
            </div>
            <div className="flex flex-col items-center gap-1">
              <span className={"rounded border px-2 py-1 font-mono text-[9px] " + BG[n.redis]}>Redis keywords</span>
              <VArrow status={e.redisDash} dashed />
            </div>
          </div>

          <div className="flex flex-wrap items-center justify-center gap-2">
            <Box label="DASH" status={n.dash} detail={flow.live ? "FastAPI · live" : "mock"} />
            <HArrow status={e.dashUi} label="REST + WS" />
            <Box label="React UI" status={n.ui} detail="P5 SPA" />
          </div>
        </div>
      </Section>

      <FlowLegend />
    </div>
  );
}

export default function OverviewTab({ meta }) {
  const { data } = usePoll(async () => {
    const [kafka, topics, flink, control, keywords] = await Promise.all([
      getKafkaOverview(),
      getKafkaTopics(),
      getFlinkOverview(),
      getControlStatus(),
      getKeywords(),
    ]);
    return { kafka, topics, flink, control, keywords };
  });

  const flow = deriveFlow({ ...data, meta });

  return (
    <div className="space-y-4">
      <p className="text-sm text-muted">
        Each section shows one pipeline stage top-to-bottom. Green arrows mean
        data is flowing; amber means waiting; red means blocked.
      </p>

      <IssueBanner issue={flow.issue} allGreen={flow.allGreen} prodRunning={flow.prodRunning} />

      <div className="rounded-xl border border-edge bg-card/40 p-4">
        <PipelineDiagram flow={flow} />
      </div>

      {!data && <p className="text-xs text-muted">polling…</p>}

      <div className="pt-2">
        <h2 className="mb-3 text-sm font-semibold text-muted">Manual controls</h2>
        <ControlPanel />
      </div>
    </div>
  );
}
