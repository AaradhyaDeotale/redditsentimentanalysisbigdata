export default function Tabs({ tabs, active, onChange }) {
  return (
    <nav className="flex gap-1 border-b border-edge px-6">
      {tabs.map((t) => {
        const isActive = t.id === active;
        return (
          <button
            key={t.id}
            onClick={() => onChange(t.id)}
            className={
              "relative px-4 py-3 text-sm font-medium transition-colors " +
              (isActive
                ? "text-text"
                : "text-muted hover:text-text")
            }
          >
            {t.label}
            {isActive && (
              <span className="absolute inset-x-2 -bottom-px h-0.5 rounded bg-accent" />
            )}
          </button>
        );
      })}
    </nav>
  );
}
