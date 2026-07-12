// Parses the comma-separated sub-keyword input into a normalized list:
// trimmed, lowercased (matching main-keyword normalization), empties and
// duplicates dropped, order preserved.
export function parseSubkeywords(text) {
  const seen = new Set();
  const out = [];
  for (const raw of (text || "").split(",")) {
    const kw = raw.trim().toLowerCase();
    if (!kw || seen.has(kw)) continue;
    seen.add(kw);
    out.push(kw);
  }
  return out;
}

// Inverse of parseSubkeywords, for populating the input from server data.
export function formatSubkeywords(subkeywords) {
  return (subkeywords || []).join(", ");
}
