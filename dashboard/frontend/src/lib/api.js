// REST wrappers used for the initial snapshot (history backfill) and for the
// polling monitoring tabs. Live updates come over the WebSocket, not these.

async function getJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText} (${url})`);
  return res.json();
}

async function postJSON(url, body) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  if (!res.ok) {
    let detail = `${res.status} ${res.statusText}`;
    try {
      detail = (await res.json()).detail || detail;
    } catch {
      /* keep status text */
    }
    throw new Error(detail);
  }
  return res.json();
}

const q = encodeURIComponent;

export const getMeta = () => getJSON("/api/meta");

export const getCompare = (k1, k2) =>
  getJSON(`/api/compare?keyword1=${q(k1)}&keyword2=${q(k2)}`);

export const getComments = (keyword, limit = 50) =>
  getJSON(`/api/comments?keyword=${q(keyword)}&limit=${limit}`);

export const getKafkaOverview = () => getJSON("/api/kafka/overview");
export const getKafkaTopics = () => getJSON("/api/kafka/topics");
export const getKafkaGroups = () => getJSON("/api/kafka/groups");

export const getFlinkOverview = () => getJSON("/api/flink/overview");
export const getFlinkJobs = () => getJSON("/api/flink/jobs");

// Manual-mode controls (local dev only; gated by CONTROL_ENABLED on the server).
export const getControlStatus = () => getJSON("/api/control/status");
export const startProducer = (speed, limit) =>
  postJSON("/api/control/producer/start", { speed, limit });
export const stopProducer = () => postJSON("/api/control/producer/stop");
export const resetPipeline = (parallelism, window_sec) =>
  postJSON("/api/control/pipeline/reset", { parallelism, window_sec });
