// Client-side re-bucketing of already-fetched sentiment window points, so the
// chart can show coarser granularity (e.g. 5m, 1h) than whatever the Flink
// pipeline actually emits (1 min by default), without touching the backend.

export const AGGREGATION_OPTIONS = [
  { label: "1s", seconds: 1 },
  { label: "10s", seconds: 10 },
  { label: "30s", seconds: 30 },
  { label: "1m", seconds: 60 },
  { label: "5m", seconds: 300 },
  { label: "15m", seconds: 900 },
  { label: "1h", seconds: 3600 },
];

export const DEFAULT_BUCKET_SECONDS = 60;

// Groups points into fixed-size time buckets, aggregating positive_ratio as a
// comment-count-weighted average (so a 50-comment window counts more than a
// 1-comment one) and summing comment_count. Buckets finer than the underlying
// data granularity are a no-op (nothing to merge).
export function bucketPoints(points, bucketSeconds) {
  if (!points || points.length === 0) return [];
  if (!bucketSeconds || bucketSeconds <= 1) return points;

  const buckets = new Map();
  for (const p of points) {
    const key = Math.floor(p.window_end / bucketSeconds) * bucketSeconds;
    const weight = Math.max(p.comment_count || 0, 1);
    const bucket = buckets.get(key);
    if (!bucket) {
      buckets.set(key, {
        window_end: key,
        weightedSum: p.positive_ratio * weight,
        weight,
        comment_count: p.comment_count || 0,
      });
    } else {
      bucket.weightedSum += p.positive_ratio * weight;
      bucket.weight += weight;
      bucket.comment_count += p.comment_count || 0;
    }
  }

  return [...buckets.values()]
    .map((b) => ({
      window_end: b.window_end,
      positive_ratio: b.weightedSum / b.weight,
      comment_count: b.comment_count,
    }))
    .sort((x, y) => x.window_end - y.window_end);
}

const EMPTY_SENTIMENT = { positive: 0, negative: 0, neutral: 0 };

function normalizeLabel(label) {
  if (label === "positive" || label === "negative") return label;
  return "neutral";
}

/** Bucket scored comments into positive / negative / neutral counts per keyword. */
export function bucketCommentsBySentiment(comments, keyword, bucketSeconds) {
  if (!comments?.length || !keyword || !bucketSeconds) return [];
  const kw = keyword.toLowerCase();
  const buckets = new Map();

  for (const c of comments) {
    const matches = (c.matched_keywords || []).some(
      (k) => String(k).toLowerCase() === kw,
    );
    if (!matches) continue;
    const t =
      Math.floor((c.created_utc || 0) / bucketSeconds) * bucketSeconds;
    const bucket = buckets.get(t) || {
      window_end: t,
      ...EMPTY_SENTIMENT,
    };
    const label = normalizeLabel(c.sentiment_label);
    bucket[label] += 1;
    buckets.set(t, bucket);
  }

  return [...buckets.values()].sort((x, y) => x.window_end - y.window_end);
}

/** Fill time buckets that have window aggregates but no comments in the feed. */
function supplementFromWindows(buckets, points, bucketSeconds, prefix) {
  const map = new Map(buckets.map((b) => [b.t, { ...b }]));

  for (const p of points || []) {
    const t = Math.floor(p.window_end / bucketSeconds) * bucketSeconds;
    const existing = map.get(t);
    const existingTotal = existing
      ? (existing[`${prefix}_positive`] || 0) +
        (existing[`${prefix}_negative`] || 0) +
        (existing[`${prefix}_neutral`] || 0)
      : 0;
    if (existingTotal > 0) continue;

    const count = p.comment_count || 0;
    if (count <= 0) continue;
    const positive = Math.round((p.positive_ratio || 0) * count);
    const negative = Math.max(0, count - positive);
    const row = existing || { t };
    row[`${prefix}_positive`] = positive;
    row[`${prefix}_negative`] = negative;
    row[`${prefix}_neutral`] = 0;
    map.set(t, row);
  }

  return map;
}

/**
 * Build merged chart rows: stacked sentiment bars (per keyword) + positive %
 * lines from window series.
 */
export function buildSentimentChartRows(
  comments,
  seriesA,
  seriesB,
  keywordA,
  keywordB,
  bucketSeconds,
  namedSeriesA = [],
  namedSeriesB = [],
) {
  const aComments = bucketCommentsBySentiment(comments, keywordA, bucketSeconds);
  const bComments = bucketCommentsBySentiment(comments, keywordB, bucketSeconds);
  const aWindows = bucketPoints(seriesA, bucketSeconds);
  const bWindows = bucketPoints(seriesB, bucketSeconds);

  const rows = new Map();

  const ensure = (t) => {
    if (!rows.has(t)) {
      rows.set(t, {
        t,
        a_positive: 0,
        a_negative: 0,
        a_neutral: 0,
        b_positive: 0,
        b_negative: 0,
        b_neutral: 0,
        [keywordA]: null,
        [keywordB]: null,
      });
    }
    return rows.get(t);
  };

  for (const b of aComments) {
    const row = ensure(b.window_end);
    row.a_positive = b.positive;
    row.a_negative = b.negative;
    row.a_neutral = b.neutral;
  }
  for (const b of bComments) {
    const row = ensure(b.window_end);
    row.b_positive = b.positive;
    row.b_negative = b.negative;
    row.b_neutral = b.neutral;
  }

  let map = supplementFromWindows([...rows.values()], aWindows, bucketSeconds, "a");
  map = supplementFromWindows([...map.values()], bWindows, bucketSeconds, "b");
  for (const row of map.values()) rows.set(row.t, row);

  for (const p of aWindows) {
    const t = Math.floor(p.window_end / bucketSeconds) * bucketSeconds;
    const row = ensure(t);
    row[keywordA] = Math.round(p.positive_ratio * 100);
  }
  for (const p of bWindows) {
    const t = Math.floor(p.window_end / bucketSeconds) * bucketSeconds;
    const row = ensure(t);
    row[keywordB] = Math.round(p.positive_ratio * 100);
  }

  // Per-sense % positive lines (e.g. "apple:company") - same time-bucketing
  // as above, just keyed by each sense's series key instead of the bare
  // keyword, so an ambiguous keyword can render one line per resolved sense.
  for (const s of namedSeriesA) {
    for (const p of bucketPoints(s.points, bucketSeconds)) {
      const t = Math.floor(p.window_end / bucketSeconds) * bucketSeconds;
      ensure(t)[s.key] = Math.round(p.positive_ratio * 100);
    }
  }
  for (const s of namedSeriesB) {
    for (const p of bucketPoints(s.points, bucketSeconds)) {
      const t = Math.floor(p.window_end / bucketSeconds) * bucketSeconds;
      ensure(t)[s.key] = Math.round(p.positive_ratio * 100);
    }
  }

  return [...rows.values()].sort((x, y) => x.t - y.t);
}

/**
 * Headline stat for a keyword's card: the % positive of the most recent bucket
 * at the chart's chosen granularity (so a lone 1s window reading "100% from 1
 * comment" becomes a count-weighted % over the whole bucket), its comment count,
 * and the change versus the previous bucket. null when there is nothing to show.
 */
export function headlineStat(points, bucketSeconds) {
  const bucketed = bucketPoints(points || [], bucketSeconds);
  if (bucketed.length === 0) return null;
  const last = bucketed[bucketed.length - 1];
  const prev = bucketed[bucketed.length - 2];
  return {
    pct: Math.round(last.positive_ratio * 100),
    count: last.comment_count,
    deltaPct: prev
      ? Math.round((last.positive_ratio - prev.positive_ratio) * 100)
      : null,
  };
}

/**
 * One dot per scored comment matching `keyword`: x = created_utc, y = a signed
 * sentiment in −1..+1. No aggregation — the "as it arrives" view.
 *
 * `sentiment_score` is an UNSIGNED confidence (0..1); the direction lives in
 * `sentiment_label`, so we sign it here the same way the live feed does
 * (negative → below zero, positive → above, neutral → on the line). Comments
 * without a numeric score are skipped; output is sorted oldest-first.
 */
export function scatterSeries(comments, keyword) {
  if (!comments?.length || !keyword) return [];
  const kw = keyword.toLowerCase();
  const out = [];
  for (const c of comments) {
    const matches = (c.matched_keywords || []).some(
      (k) => String(k).toLowerCase() === kw,
    );
    if (!matches) continue;
    if (c.sentiment_score == null) continue;
    const mag = Math.abs(Number(c.sentiment_score));
    if (!Number.isFinite(mag)) continue;
    const label = c.sentiment_label;
    const score = label === "negative" ? -mag : label === "positive" ? mag : 0;
    out.push({
      t: c.created_utc,
      score,
      label,
      author: c.author,
      body: c.body,
    });
  }
  return out.sort((x, y) => x.t - y.t);
}
