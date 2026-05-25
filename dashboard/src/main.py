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

import os
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .consumer import start_background_consumer
from .store import store

load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # begin pulling sentiment-results (or mock data) as soon as the app boots
    start_background_consumer()
    yield


app = FastAPI(title="Sentiment Dashboard API (P5)", lifespan=lifespan)

STATIC_DIR = Path(__file__).parent / "static"


@app.get("/health")
def health():
    """Simple liveness check (handy for Docker / cloud health probes)."""
    return {"status": "ok", "known_keywords": store.keywords()}


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


@app.get("/")
def index():
    """Serve the dashboard page."""
    return FileResponse(STATIC_DIR / "index.html")


# serve any other static assets (none needed yet, but ready for them)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
