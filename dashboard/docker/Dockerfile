# Dashboard service (P5) - FastAPI backend + static web dashboard.
# Build context is the dashboard/ folder, so paths below are relative to it.
#
#   docker build -f docker/Dockerfile -t sentiment-dashboard .
#   docker run -p 8000:8000 --env-file .env sentiment-dashboard

FROM python:3.11-slim

WORKDIR /app

# Install dependencies first so Docker can cache this layer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY src/ ./src/

# Document the port the app listens on
EXPOSE 8000

# Start the API + dashboard. host 0.0.0.0 so it is reachable from outside the container.
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
