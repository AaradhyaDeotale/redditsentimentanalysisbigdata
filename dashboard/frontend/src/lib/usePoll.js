import { useEffect, useRef, useState } from "react";

// Poll a fetcher on an interval. Used by the monitoring tabs: cluster metadata
// is request/response state, not a stream, so polling (not the WebSocket) is
// the right tool here. Re-polls only after the previous call settles, so a slow
// backend can't pile up overlapping requests.
export function usePoll(fetcher, intervalMs = 4000) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;

  useEffect(() => {
    let cancelled = false;
    let timer;
    async function tick() {
      try {
        const d = await fetcherRef.current();
        if (!cancelled) {
          setData(d);
          setError(null);
        }
      } catch (e) {
        if (!cancelled) setError(String(e.message || e));
      } finally {
        if (!cancelled) timer = setTimeout(tick, intervalMs);
      }
    }
    tick();
    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
  }, [intervalMs]);

  return { data, error };
}
