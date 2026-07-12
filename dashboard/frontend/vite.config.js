import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// API/WebSocket proxy target: local uvicorn by default; the docker dev compose
// overrides it to the backend service via VITE_API_PROXY. VITE_POLL turns on
// filesystem polling so HMR fires across the macOS<->Linux bind mount.
const apiTarget = process.env.VITE_API_PROXY || "http://127.0.0.1:8000";
const wsTarget = apiTarget.replace(/^http/, "ws");

// The SPA is served by FastAPI in production:
//   - assets live under /static/  (base)
//   - build output goes to ../src/web, which FastAPI mounts
// In dev, `vite` serves on :5173 and proxies API + WebSocket to uvicorn :8000.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  base: "/static/",
  build: {
    outDir: "../src/web",
    emptyOutDir: true,
  },
  server: {
    host: true,
    watch: process.env.VITE_POLL ? { usePolling: true } : undefined,
    proxy: {
      "/api": apiTarget,
      "/ws": { target: wsTarget, ws: true },
    },
  },
  test: {
    environment: "node",
  },
});
