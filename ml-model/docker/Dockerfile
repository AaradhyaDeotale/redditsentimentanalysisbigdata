# Training / retraining image for the sentiment model (P4).
# The real-time SCORER runs inside the Flink image (see flink-streaming);
# this image runs the offline jobs: train.py and retrain.py.
FROM python:3.11-slim

LABEL description="Sentiment model training & retraining (P4)"

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config/ ./config/
COPY src/ ./src/

ENV PYTHONPATH=/app:/app/src
ENV MODEL_DIR=/models
VOLUME ["/models", "/data"]

ENTRYPOINT ["python", "src/ml_model/model/train.py"]
CMD ["--input", "/data/labeled_comments.jsonl", "--feature", "tfidf", "--model-dir", "/models"]