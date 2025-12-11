from time import sleep
from typing import Any

from orjson import loads
from utils.testing import EventData, get_request, post_request

SERVER_URL = "http://localhost:8080"


def test_persistence_after_restart() -> None:
    url: str = f"{SERVER_URL}/publish"

    events: list[EventData] = []
    for i in range(50):
        events.append(
            {
                "event_id": f"persist-event-{i}",
                "topic": "persist-topic",
                "source": "test-service",
                "payload": {
                    "message": f"Persist message {i}",
                    "timestamp": "2025-01-01T00:00:00",
                },
                "timestamp": "2025-01-01T00:00:00",
            }
        )

    status, _ = post_request(url, {"events": events})
    assert status == 200

    sleep(3)

    stats_url: str = f"{SERVER_URL}/stats"
    stats_status, stats_response = get_request(stats_url)
    assert stats_status == 200

    initial_stats: dict[str, Any] = loads(stats_response or "{}")
    initial_unique: int = initial_stats["unique_processed"]

    assert initial_unique >= 50


def test_persistence_events_retrievable() -> None:
    events_url: str = f"{SERVER_URL}/events?topic=persist-topic"
    events_status, events_response = get_request(events_url)
    assert events_status == 200

    events_data: dict[str, Any] = loads(events_response or "{}")
    assert events_data["count"] >= 50

    event_ids: set[str] = {e["event_id"] for e in events_data["events"]}
    assert len(event_ids) == events_data["count"]
