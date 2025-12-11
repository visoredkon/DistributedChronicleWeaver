from time import sleep
from typing import Any

from orjson import loads
from utils.testing import EventData, get_request, post_request

SERVER_URL = "http://localhost:8080"


def test_graceful_restart_data_preserved() -> None:
    url: str = f"{SERVER_URL}/publish"

    events: list[EventData] = [
        {
            "event_id": f"restart-event-{i}",
            "topic": "restart-topic",
            "source": "test-service",
            "payload": {
                "message": f"Restart test {i}",
                "timestamp": "2025-01-01T00:00:00",
            },
            "timestamp": "2025-01-01T00:00:00",
        }
        for i in range(25)
    ]

    status, _ = post_request(url, {"events": events})
    assert status == 200

    sleep(3)

    events_url: str = f"{SERVER_URL}/events?topic=restart-topic"
    events_status, events_response = get_request(events_url)
    assert events_status == 200

    events_data: dict[str, Any] = loads(events_response or "{}")
    assert events_data["count"] >= 25


def test_graceful_restart_dedup_works() -> None:
    url: str = f"{SERVER_URL}/publish"

    event: EventData = {
        "event_id": "restart-event-0",
        "topic": "restart-topic",
        "source": "test-service",
        "payload": {
            "message": "Duplicate after restart test",
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
    assert stats["duplicated_dropped"] >= 1
