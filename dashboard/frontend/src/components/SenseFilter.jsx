// Per-keyword sense chips (All / technology / fruit / ambiguous, ...). Renders
// nothing for a keyword that hasn't resolved into any senses yet - the chart
// and feed just show everything for it, same as before this filter existed.
export default function SenseFilter({
  keyword,
  senses,
  value,
  onChange,
  accentClass,
  activeBgClass,
}) {
  if (!senses.length) return null;
  const options = ["all", ...senses];

  return (
    <div className="flex flex-wrap items-center gap-1.5">
      <span className={"text-xs lowercase " + (accentClass || "text-muted")}>
        {keyword}
      </span>
      {options.map((s) => (
        <button
          key={s}
          type="button"
          onClick={() => onChange(s)}
          className={
            "rounded-full border px-2.5 py-1 text-xs capitalize transition-colors " +
            (value === s
              ? `border-transparent text-bg ${activeBgClass || "bg-accent"}`
              : "border-edge text-muted hover:border-accent hover:text-text")
          }
        >
          {s === "all" ? "All" : s}
        </button>
      ))}
    </div>
  );
}
