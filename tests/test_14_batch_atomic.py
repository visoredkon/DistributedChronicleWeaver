from time import sleep
from typing import Any

from orjson import loads
from utils.testing import EventData, get_request, post_request

SERVER_URL = "http://localhost:8080"


def test_batch_atomic_all_valid() -> None:
    url: str = f"{SERVER_URL}/publish"

    events: list[EventData] = [
        {
            "event_id": f"batch-atomic-{i}",
            "topic": "batch-atomic-topic",
            "source": "test-service",
            "payload": {
                "message": f"Batch atomic {i}",
                "timestamp": "2025-01-01T00:00:00",
            },
            "timestamp": "2025-01-01T00:00:00",
        }
        for i in range(20)
    ]

    status, _ = post_request(url, {"events": events})
    assert status == 200

    sleep(2)

    events_url: str = f"{SERVER_URL}/events?topic=batch-atomic-topic"
    events_status, events_response = get_request(events_url)
    assert events_status == 200

    events_data: dict[str, Any] = loads(events_response or "{}")
    assert events_data["count"] >= 20


def test_batch_atomic_partial_duplicates() -> None:
    url: str = f"{SERVER_URL}/publish"

    events: list[EventData] = []
    for i in range(10):
        events.append(
            {
                "event_id": f"batch-partial-{i}",
                "topic": "batch-partial-topic",
                "source": "test-service",
                "payload": {
                    "message": f"Batch partial {i}",
                    "timestamp": "2025-01-01T00:00:00",
                },
                "timestamp": "2025-01-01T00:00:00",
            }
        )
    for i in range(5):
        events.append(
            {
                "event_id": f"batch-partial-{i}",
                "topic": "batch-partial-topic",
                "source": "test-service",
                "payload": {
                    "message": f"Batch partial duplicate {i}",
                    "timestamp": "2025-01-01T00:00:00",
                },
                "timestamp": "2025-01-01T00:00:00",
            }
        )

    status, _ = post_request(url, {"events": events})
    assert status == 200

    sleep(2)

    events_url: str = f"{SERVER_URL}/events?topic=batch-partial-topic"
    events_status, events_response = get_request(events_url)
    assert events_status == 200

    events_data: dict[str, Any] = loads(events_response or "{}")
    assert events_data["count"] == 10
