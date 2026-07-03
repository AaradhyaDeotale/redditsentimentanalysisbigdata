import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

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
    proxy: {
      "/api": "http://127.0.0.1:8000",
      "/ws": { target: "ws://127.0.0.1:8000", ws: true },
    },
  },
  test: {
    environment: "node",
  },
});
