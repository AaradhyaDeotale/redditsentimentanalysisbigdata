// Shared panel chrome + a friendly "unreachable" state for the monitoring tabs.

export function Panel({ title, right, children }) {
  return (
    <section className="rounded-xl border border-edge bg-card p-5">
      {(title || right) && (
        <div className="mb-3 flex items-center justify-between">
          {title && <h3 className="text-sm font-semibold">{title}</h3>}
          {right}
        </div>
      )}
      {children}
    </section>
  );
}

export function Unreachable({ what, error }) {
  return (
    <div className="rounded-xl border border-neg/40 bg-neg/10 p-5">
      <p className="text-sm font-medium text-neg">{what} unreachable</p>
      {error && <p className="mt-1 text-xs text-muted">{error}</p>}
    </div>
  );
}

export function Stat({ label, value }) {
  return (
    <div className="rounded-lg border border-edge bg-bg/40 px-4 py-3">
      <div className="text-xs text-muted">{label}</div>
      <div className="mt-0.5 text-2xl font-semibold">{value}</div>
    </div>
  );
}
