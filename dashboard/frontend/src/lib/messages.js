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
      // keyword -> sense (e.g. {apple: "company"}), only set for ambiguous
      // keywords; matched_keywords itself stays plain either way.
      keyword_senses: msg.keyword_senses || {},
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

// Every series belonging to a base (tracked) keyword: the plain key itself
// (if present) and any "base:sense" keys - a component doesn't need to know
// ahead of time whether/how a keyword resolved into senses.
export function seriesForBase(windows, base) {
  const b = (base || "").toLowerCase();
  if (!b) return [];
  const prefix = `${b}:`;
  return Object.keys(windows)
    .filter((k) => k === b || k.startsWith(prefix))
    .sort()
    .map((k) => ({
      key: k,
      base: b,
      sense: k === b ? null : k.slice(prefix.length),
      points: windows[k],
    }));
}

// Weighted merge of a keyword's sense series into one flat series (for the
// headline card, which shouldn't have to care about senses) - each window's
// positive_ratio is weighted by its comment_count so busier senses count
// more toward the blended number.
export function mergeSeries(namedSeries) {
  const rows = new Map();
  for (const { points } of namedSeries) {
    for (const p of points) {
      const row =
        rows.get(p.window_end) ||
        { window_end: p.window_end, weighted: 0, comment_count: 0 };
      row.weighted += p.positive_ratio * p.comment_count;
      row.comment_count += p.comment_count;
      rows.set(p.window_end, row);
    }
  }
  return [...rows.values()]
    .sort((x, y) => x.window_end - y.window_end)
    .map((r) => ({
      window_end: r.window_end,
      comment_count: r.comment_count,
      positive_ratio: r.comment_count ? r.weighted / r.comment_count : 0,
    }));
}

// Narrows a keyword's named sense series (from seriesForBase) down to the
// chosen sense, for the sense-filter chart view. "all" (or falsy) is a no-op
// so the chart shows every resolved sense, same as today.
export function filterSeriesBySense(namedSeries, sense) {
  if (!sense || sense === "all") return namedSeries;
  return namedSeries.filter((s) => s.sense === sense);
}

// Narrows the live comment feed to comments matching at least one of
// `keywords`, honoring a per-keyword sense filter. senseFilters is
// { [keyword]: selectedSense }; a keyword with no entry (or "all") behaves
// like today - any comment matching that keyword counts, regardless of sense.
export function filterCommentsBySense(comments, keywords, senseFilters = {}) {
  const active = keywords.map((k) => String(k).toLowerCase());
  return comments.filter((c) =>
    (c.matched_keywords || []).some((kRaw) => {
      const k = String(kRaw).toLowerCase();
      if (!active.includes(k)) return false;
      const filter = senseFilters[k];
      if (!filter || filter === "all") return true;
      return c.keyword_senses?.[k] === filter;
    }),
  );
}
