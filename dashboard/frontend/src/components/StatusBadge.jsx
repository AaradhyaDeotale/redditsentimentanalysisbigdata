// Small pill showing whether the backend is serving LIVE or MOCK data.
// This replaces the old hardcoded "mock data keywords" sentence: the truth now
// comes from /api/meta, so it can never lie about the data source again.
export default function StatusBadge({ mode }) {
  if (!mode) {
    return (
      <span className="rounded-full border border-edge px-2.5 py-1 text-xs text-muted">
        connecting…
      </span>
    );
  }
  const live = mode === "live";
  return (
    <span
      className={
        "inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-xs font-semibold " +
        (live
          ? "bg-pos/15 text-pos"
          : "bg-accent2/15 text-accent2")
      }
    >
      <span
        className={
          "h-1.5 w-1.5 rounded-full " + (live ? "bg-pos" : "bg-accent2")
        }
      />
      {live ? "LIVE" : "MOCK"}
    </span>
  );
}
