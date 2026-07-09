"""
control.py
----------
Local-dev "manual mode": lets the dashboard start/stop the Kafka producer and
reset the pipeline (clear topics + restart Flink) straight from the UI.

This spawns REAL processes (the producer subprocess and `docker compose`), so it
is gated behind CONTROL_ENABLED and is meant for local development only - never
enable it in the deployed image, which has neither the producer nor Docker.

The pipeline reset has two modes, picked automatically:
  - compose mode (running on the host): `docker compose down/up` + topic
    recreation via `docker exec`, exactly like the manual quick-start steps.
  - docker-api mode (running inside the dashboard container): the Flink
    containers are stopped/started through the mounted /var/run/docker.sock
    and topics are recreated with confluent-kafka's AdminClient - the
    container has neither the docker CLI nor the flink-streaming folder.

Safety model: endpoints never run arbitrary commands. They run fixed command
templates with numeric, range-validated parameters only.
"""

import http.client
import os
import re
import socket
import subprocess
import threading
import time
from pathlib import Path

from . import consumer as consumer_mod
from .analytics_store import analytics_store
from .comment_store import comment_buffer
from .keywords import connect_redis
from .store import store

CONTROL_ENABLED = os.getenv("CONTROL_ENABLED", "false").lower() == "true"

# Repo layout: dashboard/src/control.py -> repo root is parents[2].
REPO_ROOT = Path(os.getenv("REPO_ROOT", str(Path(__file__).resolve().parents[2])))
PRODUCER_PYTHON = Path(
    os.getenv("PRODUCER_PYTHON", str(REPO_ROOT / "kafka-producer/.venv/bin/python"))
)
PRODUCER_SCRIPT = REPO_ROOT / "kafka-producer/src/producer/producer.py"
PRODUCER_CWD = REPO_ROOT / "kafka-producer"
ZST_FILE = Path(os.getenv("ZST_FILE_PATH", str(REPO_ROOT / "RC_2019-04.zst")))
FLINK_COMPOSE = Path(
    os.getenv("FLINK_COMPOSE", str(REPO_ROOT / "flink-streaming/docker/docker-compose.yml"))
)

PRODUCER_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092,localhost:9095,localhost:9096")
PRODUCER_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "reddit-comments")
# Replay position, persisted in Redis so a dashboard restart doesn't silently
# rewind the auto-advance to 0 and re-replay records whose event time the
# Flink watermark already passed (windows would drop them all as late).
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
PRODUCER_CURSOR_KEY = os.getenv("PRODUCER_CURSOR_KEY", "producer:replay_cursor")
KAFKA_CONTAINER = os.getenv("KAFKA_CONTAINER", "kafka-1")
KAFKA_INTERNAL_BS = os.getenv("KAFKA_INTERNAL_BS", "kafka-1:9094,kafka-2:9094,kafka-3:9094")
TOPICS = [
    "reddit-comments",
    "reddit-comments-cleaned",
    "reddit-comments-malformed",
    "sentiment-results",
    "analytics-results",
]
DOCKER_SOCK = os.getenv("DOCKER_SOCK", "/var/run/docker.sock")
# Stop order matters: job submitter first, jobmanager last (start is reversed)
FLINK_CONTAINERS = [
    c.strip() for c in os.getenv(
        "FLINK_CONTAINERS", "flink-reddit-job,flink-taskmanager,flink-jobmanager"
    ).split(",") if c.strip()
]


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


class _UnixHTTPConnection(http.client.HTTPConnection):
    """HTTP over the Docker unix socket (stdlib only - no docker SDK)."""

    def __init__(self, sock_path: str, timeout: float = 60.0):
        super().__init__("localhost", timeout=timeout)
        self._sock_path = sock_path

    def connect(self):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(self.timeout)
        s.connect(self._sock_path)
        self.sock = s


def _docker_api(method: str, path: str) -> tuple[int, str]:
    """One Docker Engine API call over the mounted socket."""
    conn = _UnixHTTPConnection(DOCKER_SOCK)
    try:
        conn.request(method, path)
        resp = conn.getresponse()
        return resp.status, resp.read().decode("utf-8", "replace")
    finally:
        conn.close()


class ProducerController:
    """Manages a single producer replay subprocess + parses its progress."""

    def __init__(self):
        self._proc: subprocess.Popen | None = None
        self._lock = threading.Lock()
        self._sent = 0
        self._total = 0
        self._speed: float | None = None
        self._limit: int | None = None
        self._loading = False
        self._last_log = ""
        self._skip = 0      # records skipped by the current/last run
        self._cursor = 0    # where the NEXT auto-advancing run will start
        self._high_water = 0  # furthest record ever replayed (since last reset)
        self._replay_warning: str | None = None
        # Redis is connected lazily on first use so importing this module
        # (which happens at app startup) never blocks on a network round-trip.
        self._redis = None
        self._position_loaded = False

    def _ensure_position(self) -> None:
        """Connect to Redis and load the persisted cursor, once."""
        if self._position_loaded:
            return
        self._position_loaded = True
        self._redis = connect_redis(REDIS_URL)
        if self._redis is None:
            return
        try:
            data = self._redis.hgetall(PRODUCER_CURSOR_KEY)
            self._cursor = int(data.get("cursor", 0))
            self._high_water = int(data.get("high_water", 0))
        except Exception:  # noqa: BLE001 - start from 0, like before
            pass

    def _save_position(self) -> None:
        if self._redis is None:
            return
        try:
            self._redis.hset(PRODUCER_CURSOR_KEY, mapping={
                "cursor": self._cursor, "high_water": self._high_water,
            })
        except Exception:  # noqa: BLE001 - keep running with in-memory cursor
            pass

    def start(self, speed, limit, skip=None) -> dict:
        with self._lock:
            self._ensure_position()
            if self._proc and self._proc.poll() is None:
                raise RuntimeError("producer already running")
            speed = _clamp(float(speed), 0.1, 1000.0)
            limit = int(_clamp(int(limit), 1000, 5_000_000))
            # auto-advance: each run starts where the last left off unless an
            # explicit skip is given (e.g. skip=0 to replay from the start).
            skip_val = self._cursor if skip is None else int(_clamp(int(skip), 0, 50_000_000))
            cmd = [
                str(PRODUCER_PYTHON), str(PRODUCER_SCRIPT),
                "--file", str(ZST_FILE),
                "--broker", PRODUCER_BROKER,
                "--topic", PRODUCER_TOPIC,
                "--speed", str(speed),
                "--limit", str(limit),
                "--skip", str(skip_val),
            ]
            self._sent, self._total = 0, 0
            self._speed, self._limit = speed, limit
            self._skip = skip_val
            # Replaying records the pipeline already windowed puts their event
            # time BEHIND the Flink watermark: every windowed operator drops
            # them as late, so trends and the sentiment graph won't update.
            self._replay_warning = (
                f"records {skip_val:,}+ were already replayed - their event time "
                "is behind Flink's watermark, so the trends and sentiment graph "
                "will drop them as late. Reset the pipeline first to re-window them."
            ) if skip_val < self._high_water else None
            self._cursor = skip_val + limit  # advance for the next run
            self._high_water = max(self._high_water, self._cursor)
            self._save_position()
            self._loading, self._last_log = True, "starting…"
            self._proc = subprocess.Popen(
                cmd, cwd=str(PRODUCER_CWD),
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1,
            )
            threading.Thread(target=self._tail, args=(self._proc,), daemon=True).start()
        return self.status()

    def _tail(self, proc: subprocess.Popen) -> None:
        sent_re = re.compile(r"Sent (\d+) / (\d+)")
        total_re = re.compile(r"Total records in window: (\d+)")
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.rstrip()
            if not line:
                continue
            self._last_log = line
            m = total_re.search(line)
            if m:
                self._total = int(m.group(1))
                self._loading = False
            m = sent_re.search(line)
            if m:
                self._sent, self._total = int(m.group(1)), int(m.group(2))
                self._loading = False
            if "Replay complete" in line and self._total:
                self._sent = self._total

    def stop(self) -> dict:
        with self._lock:
            if self._proc and self._proc.poll() is None:
                self._proc.terminate()
                try:
                    self._proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self._proc.kill()
        return self.status()

    def reset_offset(self, clear_high_water: bool = False) -> None:
        """Rewind the auto-advance cursor so the next run starts from the top.

        `clear_high_water=True` also forgets how far replays ever got - only
        correct after a pipeline reset (fresh Flink watermark + empty topics),
        when replaying from 0 is genuinely not late data.
        """
        # Locked: the pipeline-reset thread calls this while the UI may be
        # starting a replay - an unsynchronized interleave could persist a
        # cleared high_water over the one start() just raised.
        with self._lock:
            self._ensure_position()
            self._cursor = 0
            if clear_high_water:
                self._high_water = 0
                self._replay_warning = None
            self._save_position()

    def status(self) -> dict:
        with self._lock:
            self._ensure_position()  # UI shows the persisted cursor pre-start
        running = self._proc is not None and self._proc.poll() is None
        return {
            "running": running,
            "loading": self._loading and running,
            "sent": self._sent,
            "total": self._total,
            "speed": self._speed,
            "limit": self._limit,
            "skip": self._skip,
            "offset": self._cursor,
            "last_log": self._last_log,
            "replay_warning": self._replay_warning,
        }


class PipelineController:
    """Runs the reset sequence (down Flink -> clear topics -> up Flink) in the
    background, exposing a state + rolling log for the UI to poll."""

    def __init__(self):
        self._state = "idle"  # idle | running | done | error
        self._log: list[str] = []
        self._lock = threading.Lock()

    def status(self) -> dict:
        return {"state": self._state, "log": self._log[-12:]}

    def reset(self, parallelism, window_sec) -> dict:
        with self._lock:
            if self._state == "running":
                raise RuntimeError("reset already in progress")
            self._state, self._log = "running", []
        parallelism = int(_clamp(int(parallelism), 1, 4))
        window_sec = int(_clamp(int(window_sec), 5, 3600))
        threading.Thread(
            target=self._run_reset, args=(parallelism, window_sec), daemon=True
        ).start()
        return self.status()

    def _say(self, msg: str) -> None:
        self._log.append(msg)

    def _run(self, args: list[str], env=None) -> None:
        self._say("$ " + " ".join(args[:6]) + ("…" if len(args) > 6 else ""))
        r = subprocess.run(args, capture_output=True, text=True, env=env)
        if r.returncode != 0:
            self._say((r.stderr or r.stdout or "").strip()[:300])
            raise RuntimeError(f"command failed: {args[0]} {args[1] if len(args) > 1 else ''}")

    def _run_reset(self, parallelism: int, window_sec: int) -> None:
        try:
            if FLINK_COMPOSE.exists():
                self._reset_via_compose(parallelism, window_sec)
            elif os.path.exists(DOCKER_SOCK):
                self._reset_via_docker_api()
            else:
                raise RuntimeError(
                    "cannot reset: no flink compose file on disk and no "
                    f"docker socket at {DOCKER_SOCK}"
                )
            # Only NOW is replaying from 0 genuinely not late data (fresh
            # watermark + empty topics) - clearing the replay guard on a
            # failed reset would let a re-replay be dropped without warning.
            producer.reset_offset(clear_high_water=True)
            # The in-memory stores are materialized views of the topics we
            # just wiped: empty them too, or the chart/trends/feed keep
            # showing pre-reset windows until new ones happen to overwrite.
            analytics_store.clear()
            store.clear()
            comment_buffer.clear()
            # The recreated topics have new Kafka topic ids; live consumers
            # do not follow a name across ids and would poll dead handles
            # forever. Tell the loops to rebuild their Consumers.
            consumer_mod.bump_topic_generation()
            self._say("Done - Flink restarting, job submits in ~25s.")
            self._state = "done"
        except Exception as e:  # noqa: BLE001 - surface any failure to the UI
            self._say(f"ERROR: {e}")
            self._state = "error"

    def _reset_via_compose(self, parallelism: int, window_sec: int) -> None:
        """Host / local-dev path: docker compose + docker exec, like the docs."""
        compose = str(FLINK_COMPOSE)
        self._say("Stopping Flink…")
        self._run(["docker", "compose", "-f", compose, "down"])

        self._say("Clearing topics…")
        topics = " ".join(TOPICS)
        script = (
            f"BS={KAFKA_INTERNAL_BS}; "
            f"for t in {topics}; do /opt/kafka/bin/kafka-topics.sh "
            f"--bootstrap-server $BS --delete --topic $t >/dev/null 2>&1; done; "
            f"sleep 5; "
            f"for t in {topics}; do /opt/kafka/bin/kafka-topics.sh "
            f"--bootstrap-server $BS --create --topic $t --partitions 1 "
            f"--replication-factor 3 >/dev/null 2>&1; done"
        )
        self._run(["docker", "exec", KAFKA_CONTAINER, "sh", "-c", script])

        self._say(f"Restarting Flink (parallelism={parallelism}, window={window_sec}s)…")
        env = {
            **os.environ,
            "FLINK_PARALLELISM": str(parallelism),
            "WINDOW_SIZE_SEC": str(window_sec),
        }
        self._run(["docker", "compose", "-f", compose, "up", "-d"], env=env)

    def _reset_via_docker_api(self) -> None:
        """In-container path: the dashboard image has neither the docker CLI
        nor the flink-streaming folder, but /var/run/docker.sock is mounted.
        Restarting keeps each container's existing env, so parallelism/window
        stay at whatever the compose stack was started with."""
        self._say("Stopping Flink (docker API)…")
        for name in FLINK_CONTAINERS:
            status, body = _docker_api("POST", f"/containers/{name}/stop?t=20")
            if status not in (204, 304, 404):  # stopped / already / missing
                raise RuntimeError(f"stop {name}: HTTP {status} {body[:120]}")
            self._say(f"  {name}: {'stopped' if status == 204 else 'already stopped'}")

        self._say("Clearing topics (AdminClient)…")
        self._recreate_topics()

        self._say("Restarting Flink (parallelism/window unchanged in docker mode)…")
        for name in reversed(FLINK_CONTAINERS):  # jobmanager first, job last
            status, body = _docker_api("POST", f"/containers/{name}/start")
            if status not in (204, 304):
                raise RuntimeError(f"start {name}: HTTP {status} {body[:120]}")
            self._say(f"  {name}: started")

    def _recreate_topics(self) -> None:
        from confluent_kafka.admin import NewTopic

        from .kafka_admin import _get_admin  # shared, cached AdminClient

        admin = _get_admin()
        for fut in admin.delete_topics(TOPICS, operation_timeout=30).values():
            try:
                fut.result()
            except Exception:  # noqa: BLE001 - topic may not exist yet
                pass
        # Deletion is asynchronous; creating too early races "topic is marked
        # for deletion". Try immediately, sleep only between retries, so a
        # fast deletion costs no wait.
        last_err: Exception | None = None
        for attempt in range(6):
            if attempt:
                time.sleep(5)
            futures = admin.create_topics(
                [NewTopic(t, num_partitions=1, replication_factor=3)
                 for t in TOPICS],
                operation_timeout=30,
            )
            last_err = None
            for fut in futures.values():
                try:
                    fut.result()
                except Exception as exc:  # noqa: BLE001 - retry the batch
                    if "already exists" not in str(exc).lower():
                        last_err = exc
            if last_err is None:
                return
        raise RuntimeError(f"topic recreation failed: {last_err}")


producer = ProducerController()
pipeline = PipelineController()
