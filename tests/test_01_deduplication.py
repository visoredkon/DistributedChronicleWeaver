from time import sleep
from typing import Any

from orjson import loads
from utils.testing import EventData, generate_test_events, get_request, post_request

SERVER_URL = "http://localhost:8080"


def test_deduplication_single_event() -> None:
    url: str = f"{SERVER_URL}/publish"

    event: EventData = {
        "event_id": "dedup-single-001",
        "topic": "dedup-topic",
        "source": "test-service",
        "payload": {
            "message": "Single event",
            "timestamp": "2025-01-01T00:00:00",
        },
        "timestamp": "2025-01-01T00:00:00",
    }

    status1, _ = post_request(url, {"events": [event]})
    assert status1 == 200

    status2, _ = post_request(url, {"events": [event]})
    assert status2 == 200

    sleep(2)

    events_url: str = f"{SERVER_URL}/events?topic=dedup-topic"
    events_status, events_response = get_request(events_url)
    assert events_status == 200

    events_data: dict[str, Any] = loads(events_response or "{}")
    dedup_events: list[dict[str, Any]] = [
        e for e in events_data["events"] if e["event_id"] == "dedup-single-001"
    ]
    assert len(dedup_events) == 1


def test_deduplication_batch() -> None:
    url: str = f"{SERVER_URL}/publish"

    events: list[EventData] = generate_test_events(
        count=100, duplicate_ratio=0.5, topic="dedup-batch-topic"
    )
    data: dict[str, list[EventData]] = {"events": events}
    status, _ = post_request(url, data)
    assert status == 200

    sleep(3)

    stats_url: str = f"{SERVER_URL}/stats"
    stats_status, stats_response = get_request(stats_url)
    assert stats_status == 200

    stats: dict[str, Any] = loads(stats_response or "{}")
    assert stats["received"] >= 100
    assert stats["duplicated_dropped"] >= 50


def test_deduplication_across_topics() -> None:
    url: str = f"{SERVER_URL}/publish"

    event1: EventData = {
        "event_id": "same-id-001",
        "topic": "topic-a",
        "source": "test-service",
        "payload": {
            "message": "Event in topic A",
            "timestamp": "2025-01-01T00:00:00",
        },
        "timestamp": "2025-01-01T00:00:00",
    }

    event2: EventData = {
        "event_id": "same-id-001",
        "topic": "topic-b",
        "source": "test-service",
        "payload": {
            "message": "Event in topic B",
            "timestamp": "2025-01-01T00:00:00",
        },
        "timestamp": "2025-01-01T00:00:00",
    }

    status1, _ = post_request(url, {"events": [event1]})
    assert status1 == 200

    status2, _ = post_request(url, {"events": [event2]})
    assert status2 == 200

    sleep(2)

    events_a_status, events_a_response = get_request(
        f"{SERVER_URL}/events?topic=topic-a"
    )
    assert events_a_status == 200

    events_b_status, events_b_response = get_request(
        f"{SERVER_URL}/events?topic=topic-b"
    )
    assert events_b_status == 200

    events_a: dict[str, Any] = loads(events_a_response or "{}")
    events_b: dict[str, Any] = loads(events_b_response or "{}")

    assert events_a["count"] >= 1
    assert events_b["count"] >= 1
