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
