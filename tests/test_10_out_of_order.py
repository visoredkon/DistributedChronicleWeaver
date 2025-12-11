from time import sleep
from typing import Any

from orjson import loads
from utils.testing import EventData, get_request, post_request

SERVER_URL = "http://localhost:8080"


def test_out_of_order_processing() -> None:
    url: str = f"{SERVER_URL}/publish"

    events: list[EventData] = [
        {
            "event_id": f"order-event-{i}",
            "topic": "order-topic",
            "source": "test-service",
            "payload": {
                "message": f"Order test {i}",
                "timestamp": f"2025-01-01T00:00:{10 - i:02d}",
            },
            "timestamp": f"2025-01-01T00:00:{10 - i:02d}",
        }
        for i in range(10)
    ]

    status, _ = post_request(url, {"events": events})
    assert status == 200

    sleep(2)

    events_url: str = f"{SERVER_URL}/events?topic=order-topic"
    events_status, events_response = get_request(events_url)
    assert events_status == 200

    events_data: dict[str, Any] = loads(events_response or "{}")
    assert events_data["count"] >= 10


def test_out_of_order_all_events_stored() -> None:
    events_url: str = f"{SERVER_URL}/events?topic=order-topic"
    events_status, events_response = get_request(events_url)
    assert events_status == 200

    events_data: dict[str, Any] = loads(events_response or "{}")

    event_ids: set[str] = {e["event_id"] for e in events_data["events"]}

    expected_ids: set[str] = {f"order-event-{i}" for i in range(10)}
    assert expected_ids.issubset(event_ids)
