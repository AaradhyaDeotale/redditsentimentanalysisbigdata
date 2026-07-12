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

async function putJSON(url, body) {
  const res = await fetch(url, {
    method: "PUT",
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

// Tracked-keyword set (the keywords the pipeline actively scores; lives in Redis,
// read live by the Flink job). The Compare view is populated from this set.
export const getKeywords = () => getJSON("/api/keywords");
export const addKeyword = (keyword) =>
  postJSON("/api/keywords", { keyword });
export const removeKeyword = (keyword) =>
  fetch(`/api/keywords/${encodeURIComponent(keyword)}`, {
    method: "DELETE",
  }).then(async (res) => {
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
  });

// Per-keyword sub-keywords (staged): user-typed terms the Flink resolver will
// classify a keyword's comments against once Stage 3 lands. Backend is Stage 2.
export const getSubkeywords = (keyword) =>
  getJSON(`/api/keywords/${q(keyword)}/subkeywords`);
export const setSubkeywords = (keyword, subkeywords) =>
  putJSON(`/api/keywords/${q(keyword)}/subkeywords`, { subkeywords });

// Sketch analytics (P1): Count-Min trending tokens + HyperLogLog reach.
// `keyword`: one keyword to scope to, or nothing to merge all tracked ones.
export const getTrending = (keyword) =>
  getJSON(keyword ? `/api/trending?keyword=${q(keyword)}` : "/api/trending");

// With `keyword`: that keyword's full reach history ({keyword, points}).
// Without: the latest window per tracked keyword ({keywords: [...]}).
export const getReach = (keyword) =>
  getJSON(keyword ? `/api/reach?keyword=${q(keyword)}` : "/api/reach");

// Real comments behind the top trending terms - what people actually said.
// Same `keyword` scoping semantics as getTrending.
export const getTrendingExamples = (keyword) =>
  getJSON(
    keyword
      ? `/api/trending/examples?keyword=${q(keyword)}`
      : "/api/trending/examples",
  );

export const getKafkaOverview = () => getJSON("/api/kafka/overview");
export const getKafkaTopics = () => getJSON("/api/kafka/topics");
export const getKafkaGroups = () => getJSON("/api/kafka/groups");

export const getFlinkOverview = () => getJSON("/api/flink/overview");
export const getFlinkJobs = () => getJSON("/api/flink/jobs");

// Manual-mode controls (local dev only; gated by CONTROL_ENABLED on the server).
export const getControlStatus = () => getJSON("/api/control/status");
// skip omitted -> server auto-advances; pass skip:0 to replay from the start.
export const startProducer = (speed, limit, skip) =>
  postJSON("/api/control/producer/start", { speed, limit, skip });
export const stopProducer = () => postJSON("/api/control/producer/stop");
export const resetOffset = () =>
  postJSON("/api/control/producer/reset-offset");
export const resetPipeline = (parallelism, window_sec) =>
  postJSON("/api/control/pipeline/reset", { parallelism, window_sec });
