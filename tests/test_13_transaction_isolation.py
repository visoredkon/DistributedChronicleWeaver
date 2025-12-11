from time import sleep
from typing import Any

from orjson import loads
from utils.testing import EventData, get_request, post_request

SERVER_URL = "http://localhost:8080"


def test_transaction_isolation_unique_constraint() -> None:
    url: str = f"{SERVER_URL}/publish"

    event: EventData = {
        "event_id": "isolation-test-001",
        "topic": "isolation-topic",
        "source": "test-service",
        "payload": {
            "message": "Isolation test",
            "timestamp": "2025-01-01T00:00:00",
        },
        "timestamp": "2025-01-01T00:00:00",
    }

    for _ in range(10):
        status, _ = post_request(url, {"events": [event]})
        assert status == 200

    sleep(2)

    events_url: str = f"{SERVER_URL}/events?topic=isolation-topic"
    events_status, events_response = get_request(events_url)
    assert events_status == 200

    events_data: dict[str, Any] = loads(events_response or "{}")
    isolation_events: list[dict[str, Any]] = [
        e for e in events_data["events"] if e["event_id"] == "isolation-test-001"
    ]
    assert len(isolation_events) == 1


def test_transaction_atomic_stats_update() -> None:
    url: str = f"{SERVER_URL}/publish"

    events: list[EventData] = [
        {
            "event_id": f"atomic-stats-{i}",
            "topic": "atomic-topic",
            "source": "test-service",
            "payload": {
                "message": f"Atomic test {i}",
                "timestamp": "2025-01-01T00:00:00",
            },
            "timestamp": "2025-01-01T00:00:00",
        }
        for i in range(50)
    ]

    status, _ = post_request(url, {"events": events})
    assert status == 200

    sleep(3)

    stats_url: str = f"{SERVER_URL}/stats"
    stats_status, stats_response = get_request(stats_url)
    assert stats_status == 200

    stats: dict[str, Any] = loads(stats_response or "{}")
    assert stats["received"] == stats["unique_processed"] + stats["duplicated_dropped"]
