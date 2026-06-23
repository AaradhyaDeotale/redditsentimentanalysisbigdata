"""
main.py
-------
The P5 backend: a small FastAPI app that

  1. serves the dashboard web page (static/index.html), and
  2. answers keyword sentiment queries used by that page.

It also starts a background Kafka consumer (see consumer.py) that fills the
in-memory store with sentiment results coming from the ML model (P4).

Run locally:
    cd dashboard
    pip install -r requirements.txt
    # mock data, no Kafka needed:
    USE_MOCK_DATA=true uvicorn src.main:app --reload
    # then open http://localhost:8000
"""

import asyncio
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import Body, FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from . import control, flink_proxy, kafka_admin
from .comment_store import comment_buffer
from .consumer import data_mode, set_sinks, start_background_consumer
from .store import store
from .ws_hub import hub

load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # bridge the (threaded) Kafka consumers to the async WebSocket layer:
    # the broadcaster must be running before records start flowing.
    hub.bind_loop(asyncio.get_running_loop())
    broadcaster = asyncio.create_task(hub.broadcaster())
    set_sinks(
        window_sink=lambda r: hub.publish_threadsafe({"type": "window", **r}),
        comment_sink=lambda c: hub.publish_threadsafe({"type": "comment", **c}),
    )
    # begin pulling sentiment-results + comments (or mock data) on boot
    start_background_consumer()
    yield
    broadcaster.cancel()


app = FastAPI(title="Sentiment Dashboard API (P5)", lifespan=lifespan)

WEB_DIR = Path(__file__).parent / "web"  # built Vite SPA (vite build output)
STATIC_DIR = Path(__file__).parent / "static"  # legacy fallback before a build
# Serve the SPA if it's been built; otherwise fall back so the API still runs.
SPA_DIR = WEB_DIR if (WEB_DIR / "index.html").exists() else STATIC_DIR


@app.get("/health")
def health():
    """Simple liveness check (handy for Docker / cloud health probes)."""
    return {"status": "ok", "known_keywords": store.keywords()}


@app.get("/api/meta")
def meta():
    """Tell the UI whether data is mock or live, and which keywords exist.

    Replaces the old hardcoded 'mock data keywords' line in the page.
    """
    return {"mode": data_mode(), "known_keywords": store.keywords()}


@app.get("/api/sentiment")
def sentiment(keyword: str = Query(..., min_length=1)):
    """Latest sentiment for a single keyword."""
    latest = store.latest(keyword)
    if latest is None:
        raise HTTPException(status_code=404, detail=f"No data yet for '{keyword}'")
    return latest


@app.get("/api/timeseries")
def timeseries(keyword: str = Query(..., min_length=1)):
    """Full sentiment-over-time series for a single keyword."""
    return {"keyword": keyword.lower(), "points": store.timeseries(keyword)}


@app.get("/api/comments")
def comments(
    keyword: str = Query(..., min_length=1),
    limit: int = Query(50, ge=1, le=200),
):
    """Recent scored comments for a keyword - backfills the live feed on load."""
    return {
        "keyword": keyword.lower(),
        "comments": comment_buffer.recent(keyword, limit),
    }


@app.get("/api/compare")
def compare(
    keyword1: str = Query(..., min_length=1),
    keyword2: str = Query(..., min_length=1),
):
    """Timeseries for two keywords at once - what the dashboard chart uses."""
    return {
        "keyword1": {
            "keyword": keyword1.lower(),
            "points": store.timeseries(keyword1),
        },
        "keyword2": {
            "keyword": keyword2.lower(),
            "points": store.timeseries(keyword2),
        },
    }


@app.get("/api/kafka/overview")
def kafka_overview():
    return kafka_admin.overview()


@app.get("/api/kafka/topics")
def kafka_topics():
    return kafka_admin.topics()


@app.get("/api/kafka/groups")
def kafka_groups():
    return kafka_admin.groups()


@app.get("/api/flink/overview")
async def flink_overview():
    return await flink_proxy.overview()


@app.get("/api/flink/jobs")
async def flink_jobs():
    return await flink_proxy.jobs()


@app.get("/api/flink/jobs/{job_id}")
async def flink_job(job_id: str):
    return await flink_proxy.job_detail(job_id)


def _require_control():
    if not control.CONTROL_ENABLED:
        raise HTTPException(
            status_code=403, detail="controls disabled (set CONTROL_ENABLED=true)"
        )


@app.get("/api/control/status")
def control_status():
    """Manual-mode status (always reachable so the UI can show enabled/disabled)."""
    return {
        "enabled": control.CONTROL_ENABLED,
        "producer": control.producer.status(),
        "reset": control.pipeline.status(),
    }


@app.post("/api/control/producer/start")
def control_producer_start(body: dict = Body(default={})):
    _require_control()
    try:
        return control.producer.start(body.get("speed", 2), body.get("limit", 60000))
    except (RuntimeError, ValueError) as e:
        raise HTTPException(status_code=409, detail=str(e))


@app.post("/api/control/producer/stop")
def control_producer_stop():
    _require_control()
    return control.producer.stop()


@app.post("/api/control/pipeline/reset")
def control_pipeline_reset(body: dict = Body(default={})):
    _require_control()
    try:
        return control.pipeline.reset(
            body.get("parallelism", 2), body.get("window_sec", 60)
        )
    except (RuntimeError, ValueError) as e:
        raise HTTPException(status_code=409, detail=str(e))


@app.websocket("/ws")
async def ws(websocket: WebSocket):
    """Live data plane. Client sends {"subscribe": [...]}; server pushes
    matching window + (rate-limited) comment messages."""
    client = await hub.connect(websocket)
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                message = json.loads(raw)
            except ValueError:
                continue  # ignore malformed control messages, keep the socket
            hub.update_subscription(client, message)
    except WebSocketDisconnect:
        pass
    finally:
        hub.disconnect(client)


@app.get("/")
def index():
    """Serve the dashboard single-page app."""
    return FileResponse(SPA_DIR / "index.html")


# Built SPA assets (vite emits them under /static/ because of `base`).
app.mount("/static", StaticFiles(directory=SPA_DIR), name="static")
