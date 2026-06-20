# Reddit Kafka Producer

Reads `RC_2019-04.zst`, filters comments between `1554076800` and `1555472130`, and replays them into a Kafka topic in strict `created_utc` order. Comments sharing a timestamp are emitted simultaneously. Speed is configurable.

---

## Message format

Each Kafka message is a UTF-8 encoded JSON object:

```json
{
  "id":               "abc123",
  "author":           "some_user",
  "created_utc":      1554076812,
  "body":             "This is a comment 🔥",
  "score":            42,
  "subreddit":        "technology",
  "controversiality": 0
}
```

> `body` is passed through raw — emojis and punctuation are untouched.

---

## Quick start (local)

```bash
# 1. Create and activate a virtual environment
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Copy and edit config
cp .env.example .env
# Edit .env: set ZST_FILE, KAFKA_BROKER, KAFKA_TOPIC, REPLAY_SPEED

# 4. Run
python src/producer/producer.py
```

Or override with CLI flags:

```bash
python src/producer/producer.py --file /path/to/RC_2019-04.zst --broker localhost:9092,localhost:9095,localhost:9096 --topic reddit-comments --speed 10
```

---

## Validation (no real dataset needed)

### Step 1 — Unit tests

```bash
pytest tests/ -v
```

Runs 15+ tests covering: record parsing, date filtering, emoji preservation, deleted comments, bad JSON, field extraction.

### Step 2 — Start a local Kafka

```bash
docker compose -f docker/docker-compose.yml up -d

# Wait ~15 seconds then confirm the cluster is ready:
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Step 3 — Generate test data, send, verify

```bash
# Create a small .zst test file (8 records: 4 valid, 4 filtered)
python data/make_test_data.py

# Send to Kafka at 100x speed
python src/producer/producer.py --file data/test_data.zst --broker localhost:9092,localhost:9095,localhost:9096 --speed 100

# Read back from Kafka and run all checks
python src/producer/validate.py --broker localhost:9092,localhost:9095,localhost:9096
```

Expected output from `validate.py`:

```
=======================================================
VALIDATION RESULTS
=======================================================
  ✓  Message count = 4
  ✓  All messages are valid JSON
  ✓  Record a1: all required fields present
  ✓  Record a1: timestamp in valid range
  ✓  Record a1: body is not deleted/removed/empty
  ✓  Record a1: emoji 🔥 preserved in body
  ✓  Record a2: emoji 🎉 preserved in body
  ✓  Record a3: emoji 💯 preserved in body
  ✓  Filtered records not in output
=======================================================
  18 passed   0 failed   4 messages received
=======================================================
```

### Step 4 — Clean up

```bash
docker compose -f docker/docker-compose.yml down
```

---

## Docker

```bash
# Build (run from project root)
docker build -f docker/Dockerfile -t reddit-kafka-producer .

# Run (mount the .zst file into /data/)
docker run --rm \
  -e KAFKA_BROKER=kafka-1:9094,kafka-2:9094,kafka-3:9094 \
  -e KAFKA_TOPIC=reddit-comments \
  -e REPLAY_SPEED=10.0 \
  -v /path/to/RC_2019-04.zst:/data/RC_2019-04.zst:ro \
  reddit-kafka-producer
```

---

## Docker Compose integration

Add this service to a shared `docker-compose.yml`:

```yaml
  reddit-producer:
    build:
      context: ./kafka-producer
      dockerfile: docker/Dockerfile
    depends_on:
      - kafka-1
      - kafka-2
      - kafka-3
    environment:
      KAFKA_BROKER: kafka-1:9094,kafka-2:9094,kafka-3:9094
      KAFKA_TOPIC:  reddit-comments
      REPLAY_SPEED: "10.0"
      ZST_FILE:     /data/RC_2019-04.zst
    volumes:
      - /path/to/RC_2019-04.zst:/data/RC_2019-04.zst:ro
```

---

## Environment variables

| Variable       | Default           | Description                         |
|----------------|-------------------|-------------------------------------|
| `ZST_FILE`     | `RC_2019-04.zst`  | Path to the dataset file            |
| `KAFKA_BROKER` | `localhost:9092,localhost:9095,localhost:9096` | Kafka bootstrap servers (3-broker cluster) |
| `KAFKA_TOPIC`  | `reddit-comments` | Topic to publish to                 |
| `REPLAY_SPEED` | `1.0`             | Speed multiplier (10 = 10x faster)  |

---

## Date range

| Field               | Value        | Human date               |
|---------------------|--------------|--------------------------|
| `created_utc` start | `1554076800` | 2019-04-01 00:00:00 UTC  |
| `created_utc` end   | `1555472130` | 2019-04-17 06:55:30 UTC  |

---

## File structure

```
kafka-producer/
├── src/
│   └── producer/
│       ├── __init__.py
│       ├── producer.py        # Main replay producer
│       ├── config.py          # Environment config validation
│       └── validate.py        # Reads back from Kafka and verifies output
├── data/
│   ├── make_test_data.py      # Generates test_data.zst for local testing
│   └── test_data.zst          # Generated — not committed to Git
├── docker/
│   ├── Dockerfile
│   ├── docker-compose.yml     # Local 3-broker Kafka (KRaft, no Zookeeper)
│   └── brokers.env            # Bootstrap server strings (host + Docker)
├── tests/
│   ├── conftest.py
│   ├── test_producer.py       # Unit tests for parsing logic
│   └── test_config.py         # Unit tests for config validation
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```
