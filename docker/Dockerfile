FROM python:3.11-slim

LABEL description="Replays RC_2019-04.zst into Kafka in timestamp order"

# System deps for confluent-kafka (librdkafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source (build context must be project root)
COPY src/producer/producer.py src/producer/config.py ./

# The .zst dataset is mounted at runtime via docker-compose volume.
# Default path: /data/RC_2019-04.zst  (override with ZST_FILE env var)
ENV ZST_FILE=/data/RC_2019-04.zst
ENV KAFKA_BROKER=kafka:9092
ENV KAFKA_TOPIC=reddit-comments
ENV REPLAY_SPEED=1.0

ENTRYPOINT ["python", "producer.py"]
