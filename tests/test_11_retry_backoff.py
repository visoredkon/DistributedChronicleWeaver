from time import sleep
from typing import Any

from orjson import loads
from utils.testing import EventData, get_request, post_request

SERVER_URL = "http://localhost:8080"


def test_retry_with_valid_request() -> None:
    url: str = f"{SERVER_URL}/publish"

    event: EventData = {
        "event_id": "retry-test-001",
        "topic": "retry-topic",
        "source": "test-service",
        "payload": {
            "message": "Retry test",
            "timestamp": "2025-01-01T00:00:00",
        },
        "timestamp": "2025-01-01T00:00:00",
    }

    for _ in range(3):
        status, _ = post_request(url, {"events": [event]})
        assert status == 200

    sleep(2)

    events_url: str = f"{SERVER_URL}/events?topic=retry-topic"
    events_status, events_response = get_request(events_url)
    assert events_status == 200

    events_data: dict[str, Any] = loads(events_response or "{}")
    retry_events: list[dict[str, Any]] = [
        e for e in events_data["events"] if e["event_id"] == "retry-test-001"
    ]
    assert len(retry_events) == 1


def test_retry_stats_accuracy() -> None:
    stats_url: str = f"{SERVER_URL}/stats"
    stats_status, stats_response = get_request(stats_url)
    assert stats_status == 200

    stats: dict[str, Any] = loads(stats_response or "{}")

    assert stats["received"] >= 3
    assert stats["duplicated_dropped"] >= 2
