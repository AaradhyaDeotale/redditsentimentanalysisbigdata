// Shared renderer for a ranked list of trending terms (Count-Min estimates),
// used by the Trends tab for both the compared-pair and merged views.

export const fmt = (n) => {
  if (n == null) return "—";
  if (n >= 999500) return `${(n / 1e6).toFixed(1)}M`; // rounds to >= 1000K
  if (n >= 1e4) return `${Math.round(n / 1e3)}K`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)}K`;
  return String(n);
};

// Momentum vs the previous window, straight from the API:
//   "new"  -> the term was absent last window (a genuinely fresh trend)
//   "up"   -> estimate rose >10%   "down" -> fell >10%   "flat" -> quiet
function MomentumBadge({ momentum, change }) {
  if (momentum === "new")
    return (
      <span className="rounded-full bg-accent2/15 px-1.5 py-px text-[10px] font-semibold uppercase tracking-wide text-accent2">
        new
      </span>
    );
  if (momentum === "up")
    return (
      <span className="text-xs tabular-nums text-pos" title="rising vs previous window">
        ▲{change != null ? ` ${Math.round(change * 100)}%` : ""}
      </span>
    );
  if (momentum === "down")
    return (
      <span className="text-xs tabular-nums text-neg" title="cooling vs previous window">
        ▼{change != null ? ` ${Math.round(Math.abs(change) * 100)}%` : ""}
      </span>
    );
  return null;
}

export function TrendingList({ items, showKeyword = false, top = 20, barClass = "bg-accent" }) {
  const list = (items || []).slice(0, top);
  if (list.length === 0)
    return (
      <p className="text-sm text-muted">
        No trending terms yet — they appear when the first analytics window
        closes (5 min of replayed event time).
      </p>
    );
  const max = list[0].count;
  return (
    <ol className="space-y-2">
      {list.map((item, i) => (
        <li key={item.token} className="flex items-center gap-3 text-sm">
          <span className="w-5 text-right text-xs tabular-nums text-muted">
            {i + 1}
          </span>
          <span className="w-40 min-w-0">
            <span className="block truncate font-medium" title={item.token}>
              {item.token}
            </span>
            {showKeyword && (
              <span
                className="block truncate text-[10px] text-muted"
                title={`around: ${(item.keywords || []).join(", ")}`}
              >
                {(item.keywords || []).join(" · ")}
              </span>
            )}
          </span>
          <span className="h-2 flex-1" title={`~${item.count.toLocaleString()} comments`}>
            <span
              className={"block h-2 rounded-full " + barClass}
              style={{ width: `${Math.max(2, (item.count / max) * 100)}%` }}
            />
          </span>
          <span className="w-14 text-right">
            <MomentumBadge momentum={item.momentum} change={item.change} />
          </span>
          <span className="w-12 whitespace-nowrap text-right text-xs tabular-nums text-muted">
            ~{fmt(item.count)}
          </span>
        </li>
      ))}
    </ol>
  );
}
