# Dashboard service (P5 P6) - FastAPI backend + React/Vite SPA.
# Build context is the dashboard/ folder, so paths below are relative to it.
#
#   docker build -f docker/Dockerfile -t sentiment-dashboard .
#   docker run -p 8000:8000 --env-file .env sentiment-dashboard

# --- stage 1: build the SPA -------------------------------------------------
FROM node:20-slim AS frontend
WORKDIR /build/frontend
# install deps first so this layer caches across source-only changes
COPY frontend/package.json ./
RUN npm install
COPY frontend/ ./
# vite build writes to ../src/web (see vite.config.js base/outDir)
RUN npm run build

# --- stage 2: python runtime ------------------------------------------------
FROM python:3.11-slim
WORKDIR /app

# Install dependencies first so Docker can cache this layer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code + the built SPA bundle
COPY src/ ./src/
COPY --from=frontend /build/src/web ./src/web

EXPOSE 8000

# host 0.0.0.0 so it is reachable from outside the container
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
