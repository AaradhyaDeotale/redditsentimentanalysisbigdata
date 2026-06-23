// Pure reducer for incoming WebSocket messages. Kept framework-free so it can
// be unit-tested without a DOM or socket. This is also where the chart-flicker
// fix lives conceptually: we *append* points to bounded arrays instead of
// replacing the whole dataset, so React only re-renders the new segment.

export const MAX_POINTS = 120; // per keyword, on the chart
export const MAX_COMMENTS = 60; // in the live feed

export const initialState = { windows: {}, comments: [] };

export function applyMessage(
  state,
  msg,
  { maxPoints = MAX_POINTS, maxComments = MAX_COMMENTS } = {},
) {
  if (!msg || !msg.type) return state;

  if (msg.type === "window") {
    const kw = (msg.keyword || "").toLowerCase();
    if (!kw) return state;
    const prev = state.windows[kw] || [];
    const point = {
      window_end: msg.window_end,
      positive_ratio: msg.positive_ratio,
      comment_count: msg.comment_count,
    };
    // If the same window arrives again, replace it rather than duplicate.
    const sameAsLast =
      prev.length && prev[prev.length - 1].window_end === point.window_end;
    let next = sameAsLast ? [...prev.slice(0, -1), point] : [...prev, point];
    if (next.length > maxPoints) next = next.slice(next.length - maxPoints);
    return { ...state, windows: { ...state.windows, [kw]: next } };
  }

  if (msg.type === "comment") {
    if (state.comments.some((c) => c.id === msg.id)) return state; // dedup
    const comment = {
      id: msg.id,
      author: msg.author,
      body: msg.body,
      created_utc: msg.created_utc,
      matched_keywords: msg.matched_keywords || [],
      sentiment_label: msg.sentiment_label,
      sentiment_score: msg.sentiment_score,
    };
    return {
      ...state,
      comments: [comment, ...state.comments].slice(0, maxComments),
    };
  }

  return state;
}
