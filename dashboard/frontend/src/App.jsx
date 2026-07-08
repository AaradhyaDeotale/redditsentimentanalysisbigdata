import { useEffect, useState } from "react";
import { getMeta } from "./lib/api.js";
import Tabs from "./components/Tabs.jsx";
import StatusBadge from "./components/StatusBadge.jsx";
import SentimentTab from "./tabs/SentimentTab.jsx";
import TrendsTab from "./tabs/TrendsTab.jsx";
import KafkaTab from "./tabs/KafkaTab.jsx";
import FlinkTab from "./tabs/FlinkTab.jsx";
import OverviewTab from "./tabs/OverviewTab.jsx";

const TABS = [
  { id: "sentiment", label: "Sentiment" },
  { id: "trends", label: "Trends" },
  { id: "kafka", label: "Kafka" },
  { id: "flink", label: "Flink" },
  { id: "overview", label: "Pipeline" },
];

export default function App() {
  const [tab, setTab] = useState("sentiment");
  const [meta, setMeta] = useState(null);
  // Compare selection lives here (not in SentimentTab) so it survives tab
  // switches - SentimentTab unmounts when you leave the tab, which would
  // otherwise reset the picks back to the first two tracked keywords.
  const [sel, setSel] = useState({ kw1: "", kw2: "", active: null });

  useEffect(() => {
    getMeta()
      .then(setMeta)
      .catch(() => setMeta(null));
  }, []);

  return (
    <div className="flex min-h-full flex-col">
      <header className="flex items-center gap-3 px-6 pb-4 pt-6">
        <div>
          <h1 className="text-lg font-semibold">
            Reddit Sentiment Dashboard
          </h1>
          <p className="text-xs text-muted">Group 11 · W3/A</p>
        </div>
        <div className="ml-auto">
          <StatusBadge mode={meta?.mode} />
        </div>
      </header>

      <Tabs tabs={TABS} active={tab} onChange={setTab} />

      <main className="mx-auto w-full max-w-6xl flex-1 px-6 py-6">
        {tab === "sentiment" && <SentimentTab sel={sel} setSel={setSel} />}
        {tab === "trends" && <TrendsTab compared={sel.active} />}
        {tab === "kafka" && <KafkaTab />}
        {tab === "flink" && <FlinkTab />}
        {tab === "overview" && <OverviewTab meta={meta} />}
      </main>
    </div>
  );
}