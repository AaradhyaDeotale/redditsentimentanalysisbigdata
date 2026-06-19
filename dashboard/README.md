# Dashboard (P5) — Web GUI + REST API
# Important - DO NOT USE THIS BRANCH TO MERGE INTO MAIN; USE NEW BRANCH p5-dashboard-v2 INSTEAD
# THIS BRANCH accidentally has the instructions to delete kafka-producer folder whenever it is merged into main which was an unintended consequence of me trying to keep my branch dashboard folder only. SO, instead I have created a new branch p5-dashboard-v2 from which i will continue and finish my work and merge into main from there.
The serving and UI layer of the sentiment-analysis pipeline. It lets a user
enter **two keywords** and shows the sentiment for each, plus how that
sentiment **evolves over time**.

```
Reddit data (P1) → Kafka (P2) → Flink preprocessing (P3) → ML model (P4) → [ THIS: dashboard (P5) ] → cloud (P6)
```

It consumes the Kafka topic **`sentiment-results`** (produced by the ML model,
P4), keeps the recent results in memory, and serves them to a small web page.

---

## What's inside

```
dashboard/
├── docker/
│   └── Dockerfile          # container image for this service
├── src/
│   ├── main.py             # FastAPI app + REST endpoints
│   ├── consumer.py         # Kafka consumer for sentiment-results (+ mock mode)
│   ├── store.py            # in-memory sentiment store, keyed by keyword
│   └── static/
│       └── index.html      # the dashboard web page (Chart.js)
├── tests/
│   └── test_api.py         # pytest smoke tests
├── .env.example            # copy to .env and adjust
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Run locally (no Kafka needed)

As P4 is not yet developed, for now you can develop the whole dashboard using built-in **mock data**:

```bash
cd dashboard
python -m venv .venv
.venv\Scripts\activate          # Windows PowerShell
# source .venv/bin/activate     # macOS / Linux
pip install -r requirements.txt

# start with fake data
$env:USE_MOCK_DATA="true"        # Windows PowerShell
# USE_MOCK_DATA=true             # macOS / Linux (prefix the command below)
uvicorn src.main:app --reload
```

Then open **http://localhost:8000** — you should see two keyword boxes
("apple" / "android"), live percentages, and a chart that updates every few
seconds.

---

## Run against real Kafka

```bash
cp .env.example .env       # then edit values
# KAFKA_BROKER=localhost:9092   (local)  or  kafka:9092  (inside Docker Compose)
# KAFKA_TOPIC=sentiment-results
uvicorn src.main:app --host 0.0.0.0 --port 8000
```

(Do **not** set `USE_MOCK_DATA`, or leave it `false`.)

---

## Run with Docker

```bash
cd dashboard
docker build -f docker/Dockerfile -t sentiment-dashboard .
docker run -p 8000:8000 --env-file .env sentiment-dashboard
```

For P6's full-stack Compose, this service just needs `KAFKA_BROKER=kafka:9092`
and to be on the same Docker network as the brokers.

---

## REST API

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/` | the dashboard web page |
| GET | `/health` | liveness check + list of known keywords |
| GET | `/api/sentiment?keyword=apple` | latest sentiment for one keyword |
| GET | `/api/timeseries?keyword=apple` | full time series for one keyword |
| GET | `/api/compare?keyword1=apple&keyword2=android` | time series for both (used by the page) |

---

## Expected message schema (from P4)

This service assumes each message on `sentiment-results` is JSON shaped like:

```json
{
  "keyword": "apple",
  "window_end": 1554080400,
  "positive_ratio": 0.82,
  "comment_count": 143
}
```

> **If P4's field names differ, the only file to change is `src/consumer.py`
> (the `_parse` function).** Everything else works off that normalized record.

---

## Tests

```bash
cd dashboard
$env:USE_MOCK_DATA="true"; pytest      # Windows PowerShell
# USE_MOCK_DATA=true pytest             # macOS / Linux
```
