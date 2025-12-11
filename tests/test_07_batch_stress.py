from time import sleep, time
from typing import Any

from orjson import loads
from utils.testing import EventData, generate_test_events, get_request, post_request

SERVER_URL = "http://localhost:8080"


def test_batch_stress_20000_events() -> None:
    url: str = f"{SERVER_URL}/publish"
    event_count: int = 20000
    duplicate_ratio: float = 0.3
    batch_size: int = 1000

    events: list[EventData] = generate_test_events(
        count=event_count, duplicate_ratio=duplicate_ratio, topic="stress-topic"
    )

    start_time: float = time()
    total_sent: int = 0

    for i in range(0, len(events), batch_size):
        batch: list[EventData] = events[i : i + batch_size]
        status, _ = post_request(url, {"events": batch})
        assert status == 200
        total_sent += len(batch)

    publish_time: float = time() - start_time

    assert total_sent == event_count
    assert publish_time < 60

    sleep(10)

    stats_url: str = f"{SERVER_URL}/stats"
    stats_status, stats_response = get_request(stats_url)
    assert stats_status == 200

    stats: dict[str, Any] = loads(stats_response or "{}")
    assert stats["received"] >= event_count


def test_batch_stress_throughput() -> None:
    url: str = f"{SERVER_URL}/publish"
    batch_size: int = 500

    events: list[EventData] = [
        {
            "event_id": f"throughput-event-{i}",
            "topic": "throughput-topic",
            "source": "test-service",
            "payload": {
                "message": f"Throughput test {i}",
                "timestamp": "2025-01-01T00:00:00",
            },
            "timestamp": "2025-01-01T00:00:00",
        }
        for i in range(batch_size)
    ]

    start_time: float = time()
    status, _ = post_request(url, {"events": events})
    elapsed_time: float = time() - start_time

    assert status == 200

    throughput: float = batch_size / elapsed_time if elapsed_time > 0 else 0
    assert throughput > 50


def test_batch_stress_large_payload() -> None:
    url: str = f"{SERVER_URL}/publish"

    large_message: str = "x" * 1000

    events: list[EventData] = [
        {
            "event_id": f"large-payload-{i}",
            "topic": "large-topic",
            "source": "test-service",
            "payload": {
                "message": large_message,
                "timestamp": "2025-01-01T00:00:00",
            },
            "timestamp": "2025-01-01T00:00:00",
        }
        for i in range(100)
    ]

    status, _ = post_request(url, {"events": events})
    assert status == 200

    sleep(3)

    events_url: str = f"{SERVER_URL}/events?topic=large-topic"
    events_status, events_response = get_request(events_url)
    assert events_status == 200

    events_data: dict[str, Any] = loads(events_response or "{}")
    assert events_data["count"] >= 100
