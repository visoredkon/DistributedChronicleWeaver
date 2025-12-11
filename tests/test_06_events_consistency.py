from time import sleep
from typing import Any

from orjson import loads
from utils.testing import EventData, get_request, post_request

SERVER_URL = "http://localhost:8080"


def test_events_structure() -> None:
    events_url: str = f"{SERVER_URL}/events"
    events_status, events_response = get_request(events_url)
    assert events_status == 200

    events_data: dict[str, Any] = loads(events_response or "{}")

    assert "count" in events_data
    assert "events" in events_data
    assert isinstance(events_data["count"], int)
    assert isinstance(events_data["events"], list)


def test_events_filter_by_topic() -> None:
    url: str = f"{SERVER_URL}/publish"
    topic: str = "filter-topic"

    events: list[EventData] = [
        {
            "event_id": f"filter-event-{i}",
            "topic": topic,
            "source": "test-service",
            "payload": {
                "message": f"Filter test {i}",
                "timestamp": "2025-01-01T00:00:00",
            },
            "timestamp": "2025-01-01T00:00:00",
        }
        for i in range(5)
    ]

    status, _ = post_request(url, {"events": events})
    assert status == 200

    sleep(2)

    events_url: str = f"{SERVER_URL}/events?topic={topic}"
    events_status, events_response = get_request(events_url)
    assert events_status == 200

    events_data: dict[str, Any] = loads(events_response or "{}")
    assert events_data["count"] >= 5

    for event in events_data["events"]:
        assert event["topic"] == topic


def test_events_count_matches_unique_processed() -> None:
    stats_url: str = f"{SERVER_URL}/stats"
    stats_status, stats_response = get_request(stats_url)
    assert stats_status == 200

    stats: dict[str, Any] = loads(stats_response or "{}")

    events_url: str = f"{SERVER_URL}/events"
    events_status, events_response = get_request(events_url)
    assert events_status == 200

    events_data: dict[str, Any] = loads(events_response or "{}")

    assert events_data["count"] == stats["unique_processed"]


def test_events_unique_ids() -> None:
    events_url: str = f"{SERVER_URL}/events"
    events_status, events_response = get_request(events_url)
    assert events_status == 200

    events_data: dict[str, Any] = loads(events_response or "{}")

    event_keys: set[tuple[str, str]] = set()
    for event in events_data["events"]:
        key: tuple[str, str] = (event["event_id"], event["topic"])
        assert key not in event_keys
        event_keys.add(key)
