from time import sleep

from utils.testing import get_events, post_request


def test_out_of_order_processing(server_url: str) -> None:
    url = f"{server_url}/publish"

    events = [
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

    status, events_data = get_events(server_url, topic="order-topic")
    assert status == 200
    assert events_data["count"] == 10


def test_out_of_order_all_events_stored(server_url: str) -> None:
    status, events_data = get_events(server_url, topic="order-topic")
    assert status == 200

    event_ids = {e["event_id"] for e in events_data["events"]}
    expected_ids = {f"order-event-{i}" for i in range(10)}
    assert expected_ids == event_ids
