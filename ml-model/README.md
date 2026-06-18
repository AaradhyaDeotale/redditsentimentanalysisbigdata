# ML Model (P4) — Sentiment Classifier

The machine-learning layer of the sentiment-analysis pipeline. It learns to
classify a Reddit comment as **positive** or **negative**, scores comments in
real time, and aggregates sentiment **per keyword over time**.

```
Reddit data (P1) → Kafka (P2) → Flink preprocessing (P3) → [ THIS: ML model (P4) ] → dashboard (P5) → cloud (P6)
```

It **consumes** the Kafka topic **`reddit-comments-cleaned`** (cleaned and
tokenized by Flink, P3) and **produces** the topic **`sentiment-results`**
(consumed by the dashboard, P5).

### Input record (from P3)

```json
{
  "id": "...", "author": "...", "created_utc": 1554076800,
  "subreddit": "technology", "language": "en",
  "original_body": "Apple's update is amazing 🔥",
  "cleaned_body": "apple update amazing 🔥",
  "tokens": ["apple", "update", "amazing", "🔥"],
  "score": 10, "controversiality": 0,
  "matched_keywords": ["apple"],
  "sentiment_label": null, "sentiment_score": null,
  "sentiment_status": "pending_ml_integration"
}
```

### Output record (to P5) — schema agreed with the dashboard

```json
{ "keyword": "apple", "window_start": 1554076800, "window_end": 1554080400,
  "positive_ratio": 0.82, "comment_count": 143 }
```

> The model is trained **by us**. A lexicon (VADER) is used **only** to generate
> training labels; the final classifier is our own model.

---

## What's inside

```
ml-model/
├── config/
│   ├── __init__.py
│   └── settings.py          # env-driven config (Kafka topics, model + training params)
├── src/
│   └── ml_model/
│       ├── __init__.py
│       ├── data/            # Phase 1 — collect a training corpus
│       ├── labeling/        # Phase 2 — lexicon labelling (labels only)
│       ├── features/        # Phase 3 — TF-IDF + self-trained Word2Vec
│       └── model/           # Phase 4 — train, evaluate, versioned model store
├── tests/
│   ├── conftest.py
│   └── test_settings.py     # smoke tests (green from day one)
├── .env.example             # copy to .env and adjust
├── .gitignore
└── requirements.txt
```

The sub-packages are scaffolded now and filled in over the next phases; the
real-time scorer and the windowed aggregator (Phase 5) plug into the Flink job
via the `SentimentScorer` interface already defined in `flink-streaming`.

---

## Quick start

```bash
cd ml-model

# 1. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate          # macOS / Linux
# .venv\Scripts\activate           # Windows (PowerShell)

# 2. Install dependencies
pip install -r requirements.txt

# 3. Copy the example environment file
cp .env.example .env               # then edit if needed

# 4. Run the tests (should pass on a fresh checkout)
pytest tests/ -v
```

## The P4 pipeline, end to end

1. Collect a corpus of cleaned comments from Kafka (Phase 1) -> data/cleaned_comments.jsonl
2. Label it with VADER (labels only):
       python src/ml_model/labeling/label_corpus.py \
           --input data/cleaned_comments.jsonl --output data/labeled_comments.jsonl
3. Train our own model and save a version:
       python src/ml_model/model/train.py \
           --input data/labeled_comments.jsonl --feature tfidf
4. The Flink scorer (flink-streaming) loads the latest model and scores the live
   stream; the windowed aggregator publishes per-keyword sentiment to the
   `sentiment-results` topic for the dashboard (P5).
5. Retrain periodically; the scorer hot-reloads the new version:
       python src/ml_model/retrain/retrain.py --input data/labeled_comments.jsonl

Run all tests:  pytest tests/ -v
