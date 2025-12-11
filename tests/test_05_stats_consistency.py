from time import sleep
from typing import Any

from orjson import loads
from utils.testing import EventData, get_request, post_request

SERVER_URL = "http://localhost:8080"


def test_stats_structure() -> None:
    stats_url: str = f"{SERVER_URL}/stats"
    stats_status, stats_response = get_request(stats_url)
    assert stats_status == 200

    stats: dict[str, Any] = loads(stats_response or "{}")

    assert "received" in stats
    assert "unique_processed" in stats
    assert "duplicated_dropped" in stats
    assert "topics" in stats
    assert "uptime" in stats

    assert isinstance(stats["received"], int)
    assert isinstance(stats["unique_processed"], int)
    assert isinstance(stats["duplicated_dropped"], int)
    assert isinstance(stats["topics"], list)
    assert isinstance(stats["uptime"], int)


def test_stats_increments_on_publish() -> None:
    stats_url: str = f"{SERVER_URL}/stats"

    initial_status, initial_response = get_request(stats_url)
    assert initial_status == 200
    initial_stats: dict[str, Any] = loads(initial_response or "{}")
    initial_received: int = initial_stats["received"]

    events: list[EventData] = [
        {
            "event_id": f"stats-test-{i}",
            "topic": "stats-topic",
            "source": "test-service",
            "payload": {
                "message": f"Stats test {i}",
                "timestamp": "2025-01-01T00:00:00",
            },
            "timestamp": "2025-01-01T00:00:00",
        }
        for i in range(10)
    ]

    url: str = f"{SERVER_URL}/publish"
    status, _ = post_request(url, {"events": events})
    assert status == 200

    sleep(2)

    updated_status, updated_response = get_request(stats_url)
    assert updated_status == 200
    updated_stats: dict[str, Any] = loads(updated_response or "{}")

    assert updated_stats["received"] >= initial_received + 10


def test_stats_topics_updated() -> None:
    url: str = f"{SERVER_URL}/publish"
    unique_topic: str = "unique-stats-topic-001"

    event: EventData = {
        "event_id": "stats-topic-event",
        "topic": unique_topic,
        "source": "test-service",
        "payload": {
            "message": "Topic test",
            "timestamp": "2025-01-01T00:00:00",
        },
        "timestamp": "2025-01-01T00:00:00",
    }

    status, _ = post_request(url, {"events": [event]})
    assert status == 200

    sleep(2)

    stats_url: str = f"{SERVER_URL}/stats"
    stats_status, stats_response = get_request(stats_url)
    assert stats_status == 200

    stats: dict[str, Any] = loads(stats_response or "{}")
    assert unique_topic in stats["topics"]
