import { useCallback, useEffect, useRef, useState } from "react";

// Build the WebSocket URL from a location-like object (injectable for tests).
export function wsUrl(loc, path = "/ws") {
  const proto = loc.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${loc.host}${path}`;
}

// A resilient WebSocket: auto-reconnects with capped exponential backoff and
// re-sends the latest subscription on every (re)connect, so a dropped socket
// silently heals without the UI losing its keyword subscription.
export function useWebSocket(onMessage) {
  const wsRef = useRef(null);
  const subsRef = useRef([]);
  const onMessageRef = useRef(onMessage);
  const reconnectRef = useRef(null);
  const [connected, setConnected] = useState(false);
  onMessageRef.current = onMessage;

  const sendSubscribe = useCallback(() => {
    const ws = wsRef.current;
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ subscribe: subsRef.current }));
    }
  }, []);

  const subscribe = useCallback(
    (keywords) => {
      subsRef.current = keywords;
      sendSubscribe();
    },
    [sendSubscribe],
  );

  useEffect(() => {
    let closedByUs = false;
    let attempt = 0;

    function connect() {
      const ws = new WebSocket(wsUrl(window.location));
      wsRef.current = ws;
      ws.onopen = () => {
        attempt = 0;
        setConnected(true);
        sendSubscribe();
      };
      ws.onmessage = (ev) => {
        try {
          onMessageRef.current(JSON.parse(ev.data));
        } catch {
          /* ignore malformed frames */
        }
      };
      ws.onclose = () => {
        setConnected(false);
        if (closedByUs) return;
        attempt += 1;
        const delay = Math.min(1000 * 2 ** attempt, 10000);
        reconnectRef.current = setTimeout(connect, delay);
      };
      ws.onerror = () => ws.close();
    }

    connect();
    return () => {
      closedByUs = true;
      clearTimeout(reconnectRef.current);
      if (wsRef.current) wsRef.current.close();
    };
  }, [sendSubscribe]);

  return { subscribe, connected };
}
