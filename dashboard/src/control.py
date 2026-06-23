"""
control.py
----------
Local-dev "manual mode": lets the dashboard start/stop the Kafka producer and
reset the pipeline (clear topics + restart Flink) straight from the UI.

This spawns REAL processes (the producer subprocess and `docker compose`), so it
is gated behind CONTROL_ENABLED and is meant for local development only - never
enable it in the deployed image, which has neither the producer nor Docker.

Safety model: endpoints never run arbitrary commands. They run fixed command
templates with numeric, range-validated parameters only.
"""

import os
import re
import subprocess
import threading
from pathlib import Path

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
KAFKA_CONTAINER = os.getenv("KAFKA_CONTAINER", "kafka-1")
KAFKA_INTERNAL_BS = os.getenv("KAFKA_INTERNAL_BS", "kafka-1:9094,kafka-2:9094,kafka-3:9094")
TOPICS = [
    "reddit-comments",
    "reddit-comments-cleaned",
    "reddit-comments-malformed",
    "sentiment-results",
]


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


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

    def start(self, speed, limit, skip=None) -> dict:
        with self._lock:
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
            self._cursor = skip_val + limit  # advance for the next run
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

    def reset_offset(self) -> None:
        """Rewind the auto-advance cursor so the next run starts from the top."""
        self._cursor = 0

    def status(self) -> dict:
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
        compose = str(FLINK_COMPOSE)
        try:
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

            self._say("Done - Flink restarting, job submits in ~25s.")
            self._state = "done"
        except Exception as e:  # noqa: BLE001 - surface any failure to the UI
            self._say(f"ERROR: {e}")
            self._state = "error"


producer = ProducerController()
pipeline = PipelineController()
