"""
flink_proxy.py
--------------
Thin async proxy to the Flink JobManager REST API for the dashboard's Flink
tab. The browser can't reach the JobManager directly (different host/network),
so the backend fetches and forwards. Like kafka_admin, every call degrades to
{"available": False, "error": ...} when Flink is unreachable.

Flink's modern REST API is unversioned at the root (e.g. /overview, /jobs/overview,
/jobs/{id}) - no /v1 prefix.
"""

import os

import httpx

FLINK_API_URL = os.getenv("FLINK_API_URL", "http://localhost:8081")

_client: httpx.AsyncClient | None = None


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(base_url=FLINK_API_URL, timeout=3.0)
    return _client


async def _get_json(path: str):
    r = await _get_client().get(path)
    r.raise_for_status()
    return r.json()


async def overview() -> dict:
    """Cluster overview: slots, task managers, job counts."""
    try:
        return {"available": True, "overview": await _get_json("/overview")}
    except Exception as e:  # noqa: BLE001
        return {"available": False, "error": str(e)}


async def jobs() -> dict:
    """List of jobs with state + timestamps."""
    try:
        data = await _get_json("/jobs/overview")
        return {"available": True, "jobs": data.get("jobs", [])}
    except Exception as e:  # noqa: BLE001
        return {"available": False, "error": str(e)}


async def job_detail(job_id: str) -> dict:
    """Per-vertex detail (throughput, state) for one job."""
    try:
        return {"available": True, "job": await _get_json(f"/jobs/{job_id}")}
    except Exception as e:  # noqa: BLE001
        return {"available": False, "error": str(e)}
