# Reddit Sentiment Analysis — Big Data Pipeline

A cloud-based, real-time sentiment analysis system built on a streaming Big Data
pipeline. It ingests Reddit comments, processes them as a live stream, classifies
their sentiment with a machine-learning model trained from scratch, and visualises
how sentiment toward chosen keywords evolves over time.

> University project — **Big Data Lab (Topic A: Sentiment Analysis)**,
> Institute for Data Engineering, Hamburg University of Technology (TUHH).
> Team project; this repository is my personal copy. **My contribution is the
> dashboard / serving layer (P5)** — see [below](#my-contribution--dashboard-p5).

---

## What it does

Given two keywords (e.g. *"apple"* and *"android"*), the system reports how
positively or negatively Reddit comments talk about each one, and shows that
sentiment changing over time on a live dashboard.

The data is a slice of the [Pushshift Reddit Dataset](https://files.pushshift.io/reddit/)
(April 2019 comments). Rather than treating it as a static database, the pipeline
*replays* the comments as a real-time stream — emulating an endless live data
source, which is the realistic Big Data scenario.

---

## Architecture

The system is split into six components, each owned by a team member:

```
 Reddit dataset (.zst)
        │
        ▼
 ┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
 │ Data replay  │──▶│    Kafka     │──▶│    Flink     │──▶│   ML model   │
 │ producer (P1)│   │  cluster (P2)│   │ preprocessing│   │     (P4)     │
 │              │   │              │   │     (P3)     │   │              │
 └──────────────┘   └──────────────┘   └──────────────┘   └──────┬───────┘
                                                                  │
                                          sentiment-results topic │
                                                                  ▼
                                                          ┌──────────────┐
                                                          │  Dashboard   │
                                                          │   API + UI   │
                                                          │     (P5)     │  ◀── my part
                                                          └──────────────┘

         Everything containerised & cloud-deployed (P6)
```

| Component | Owner | Responsibility |
|-----------|-------|----------------|
| **P1** | Data replay | Replays Reddit `.zst` comments into Kafka in timestamp order, at a configurable speed |
| **P2** | Kafka infra | Kafka broker cluster and topics (`reddit-comments`, `processed-data`, `sentiment-results`) |
| **P3** | Flink pipeline | Stream preprocessing: tokenisation, emoji/language handling, stop-word removal, stemming, keyword filtering, windowing |
| **P4** | ML model | Lexicon labelling, Word2Vec features, model training (LSTM / Logistic Regression), real-time inference + retraining |
| **P5** | **Dashboard** | **REST API + web GUI: keyword input, sentiment percentages, sentiment-over-time charts** |
| **P6** | Deployment | Docker Compose full stack, cloud deployment, DockerHub publishing |

---

## Tech stack

- **Streaming:** Apache Kafka, Apache Flink
- **Machine learning:** Word2Vec, LSTM / Logistic Regression (trained from scratch — no pre-built sentiment tools)
- **Dashboard (P5):** Python, FastAPI, Chart.js
- **Infrastructure:** Docker, Docker Compose, public cloud (AWS / GCP / Azure)

---

## My contribution — Dashboard (P5)

I built the **serving and UI layer**: the part the end user actually sees and
interacts with. It lets a user enter two keywords and compare how sentiment
toward them evolves over time.

It consists of:

- a **FastAPI** backend exposing a small REST API for keyword sentiment queries,
- a **Kafka consumer** that subscribes to the `sentiment-results` topic (produced
  by the ML model) and keeps recent results in memory,
- a **web dashboard** (plain HTML + Chart.js) with two keyword inputs, live
  positivity percentages, and a sentiment-over-time line chart that auto-refreshes,
- a **Dockerfile** so the service slots into the full cloud-deployed stack,
- a **mock-data mode** so the entire dashboard can be developed and demoed
  independently of the rest of the pipeline.

Full details, run instructions, and the API reference are in
**[`dashboard/README.md`](dashboard/README.md)**.

### Quick start (dashboard only, no Kafka needed)

```bash
cd dashboard
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # macOS / Linux
pip install -r requirements.txt

$env:USE_MOCK_DATA="true"        # Windows PowerShell
uvicorn src.main:app --reload
```

Then open <http://localhost:8000>.

---

## Project status

This repository is a work in progress. The dashboard (P5) runs end-to-end on mock
data; integration with the live ML output (P4) and full cloud deployment (P6) are
ongoing as a team.
