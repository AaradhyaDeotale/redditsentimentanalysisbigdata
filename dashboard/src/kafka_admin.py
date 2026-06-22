"""
kafka_admin.py
--------------
Read-only introspection of the Kafka cluster for the dashboard's Kafka tab,
via confluent-kafka's AdminClient. Every call degrades gracefully: if the
cluster is unreachable we return {"available": False, "error": ...} instead of
raising, so the tab can show a friendly "cluster unreachable" state.
"""

import os

_admin = None


def _get_admin():
    """Lazily build (and cache) the AdminClient. Lazy import keeps mock mode
    and tests free of any broker dependency."""
    global _admin
    if _admin is None:
        from confluent_kafka.admin import AdminClient

        _admin = AdminClient(
            {
                "bootstrap.servers": os.getenv(
                    "KAFKA_BROKER", "localhost:9092,localhost:9095,localhost:9096"
                )
            }
        )
    return _admin


def _state_name(state) -> str:
    """confluent's ConsumerGroupState stringifies as 'ConsumerGroupState.STABLE';
    prefer the bare enum name ('STABLE')."""
    return getattr(state, "name", str(state))


def overview() -> dict:
    """Broker list + controller, used for the cluster header."""
    try:
        md = _get_admin().list_topics(timeout=5)
        brokers = [
            {"id": b.id, "host": b.host, "port": b.port}
            for b in md.brokers.values()
        ]
        return {
            "available": True,
            "brokers": sorted(brokers, key=lambda x: x["id"]),
            "controller_id": md.controller_id,
            "topic_count": len(md.topics),
        }
    except Exception as e:  # noqa: BLE001 - any failure means "unreachable"
        return {"available": False, "error": str(e)}


def topics() -> dict:
    """Topic names + partition counts."""
    try:
        md = _get_admin().list_topics(timeout=5)
        items = [
            {
                "name": t.topic,
                "partitions": len(t.partitions),
                "internal": t.topic.startswith("_"),
            }
            for t in md.topics.values()
        ]
        return {"available": True, "topics": sorted(items, key=lambda x: x["name"])}
    except Exception as e:  # noqa: BLE001
        return {"available": False, "error": str(e)}


def groups() -> dict:
    """Consumer group ids + state."""
    try:
        future = _get_admin().list_consumer_groups(request_timeout=5)
        result = future.result(timeout=6)
        items = [
            {"id": g.group_id, "state": _state_name(getattr(g, "state", ""))}
            for g in result.valid
        ]
        return {"available": True, "groups": sorted(items, key=lambda x: x["id"])}
    except Exception as e:  # noqa: BLE001
        return {"available": False, "error": str(e)}
