"""Sentiment analysis ML model (P4).

Consumes cleaned comments from Kafka (reddit-comments-cleaned), scores their
sentiment with a self-trained model, aggregates per keyword over time windows,
and publishes results to the sentiment-results topic for the dashboard (P5).
"""
