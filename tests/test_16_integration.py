from time import sleep
from typing import Any

from orjson import loads
from utils.testing import EventData, generate_test_events, get_request, post_request

SERVER_URL = "http://localhost:8080"


def test_integration_full_workflow() -> None:
    url: str = f"{SERVER_URL}/publish"
    topic: str = "integration-topic"

    events: list[EventData] = generate_test_events(
        count=100, duplicate_ratio=0.3, topic=topic
    )
    status, _ = post_request(url, {"events": events})
    assert status == 200

    sleep(5)

    stats_url: str = f"{SERVER_URL}/stats"
    stats_status, stats_response = get_request(stats_url)
    assert stats_status == 200

    stats: dict[str, Any] = loads(stats_response or "{}")
    assert stats["received"] >= 100
    assert stats["unique_processed"] >= 70
    assert stats["duplicated_dropped"] >= 30
    assert topic in stats["topics"]

    events_url: str = f"{SERVER_URL}/events?topic={topic}"
    events_status, events_response = get_request(events_url)
    assert events_status == 200

    events_data: dict[str, Any] = loads(events_response or "{}")
    assert events_data["count"] >= 70

    event_ids: set[str] = {e["event_id"] for e in events_data["events"]}
    assert len(event_ids) == events_data["count"]


def test_integration_multiple_topics() -> None:
    url: str = f"{SERVER_URL}/publish"

    topics: list[str] = ["multi-topic-a", "multi-topic-b", "multi-topic-c"]

    for topic in topics:
        events: list[EventData] = [
            {
                "event_id": f"multi-{topic}-{i}",
                "topic": topic,
                "source": "test-service",
                "payload": {
                    "message": f"Multi topic test {i}",
                    "timestamp": "2025-01-01T00:00:00",
                },
                "timestamp": "2025-01-01T00:00:00",
            }
            for i in range(10)
        ]
        status, _ = post_request(url, {"events": events})
        assert status == 200

    sleep(3)

    stats_url: str = f"{SERVER_URL}/stats"
    stats_status, stats_response = get_request(stats_url)
    assert stats_status == 200

    stats: dict[str, Any] = loads(stats_response or "{}")
    for topic in topics:
        assert topic in stats["topics"]


def test_integration_api_consistency() -> None:
    stats_url: str = f"{SERVER_URL}/stats"
    stats_status, stats_response = get_request(stats_url)
    assert stats_status == 200

    stats: dict[str, Any] = loads(stats_response or "{}")

    events_url: str = f"{SERVER_URL}/events"
    events_status, events_response = get_request(events_url)
    assert events_status == 200

    events_data: dict[str, Any] = loads(events_response or "{}")

    assert events_data["count"] == stats["unique_processed"]
    assert stats["received"] == stats["unique_processed"] + stats["duplicated_dropped"]
