from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
from typing import Any

from orjson import loads
from utils.testing import EventData, get_request, post_request

SERVER_URL = "http://localhost:8080"


def send_events(events: list[EventData]) -> tuple[int | None, str | None]:
    url: str = f"{SERVER_URL}/publish"
    return post_request(url, {"events": events})


def test_concurrency_parallel_requests() -> None:
    base_events: list[list[EventData]] = []
    for batch_id in range(10):
        batch: list[EventData] = []
        for i in range(10):
            batch.append(
                {
                    "event_id": f"concurrent-{batch_id}-{i}",
                    "topic": "concurrent-topic",
                    "source": "test-service",
                    "payload": {
                        "message": f"Concurrent message {batch_id}-{i}",
                        "timestamp": "2025-01-01T00:00:00",
                    },
                    "timestamp": "2025-01-01T00:00:00",
                }
            )
        base_events.append(batch)

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(send_events, batch) for batch in base_events]
        results = [f.result() for f in as_completed(futures)]

    for status, _ in results:
        assert status == 200

    sleep(3)

    events_url: str = f"{SERVER_URL}/events?topic=concurrent-topic"
    events_status, events_response = get_request(events_url)
    assert events_status == 200

    events_data: dict[str, Any] = loads(events_response or "{}")
    assert events_data["count"] == 100


def test_concurrency_duplicate_parallel() -> None:
    same_event: EventData = {
        "event_id": "same-concurrent-001",
        "topic": "concurrent-dup-topic",
        "source": "test-service",
        "payload": {
            "message": "Same event parallel",
            "timestamp": "2025-01-01T00:00:00",
        },
        "timestamp": "2025-01-01T00:00:00",
    }

    batches: list[list[EventData]] = [[same_event.copy()] for _ in range(20)]

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(send_events, batch) for batch in batches]
        results = [f.result() for f in as_completed(futures)]

    for status, _ in results:
        assert status == 200

    sleep(3)

    events_url: str = f"{SERVER_URL}/events?topic=concurrent-dup-topic"
    events_status, events_response = get_request(events_url)
    assert events_status == 200

    events_data: dict[str, Any] = loads(events_response or "{}")
    same_events: list[dict[str, Any]] = [
        e for e in events_data["events"] if e["event_id"] == "same-concurrent-001"
    ]
    assert len(same_events) == 1


def test_concurrency_stats_consistency() -> None:
    stats_url: str = f"{SERVER_URL}/stats"
    stats_status, stats_response = get_request(stats_url)
    assert stats_status == 200

    stats: dict[str, Any] = loads(stats_response or "{}")
    received: int = stats["received"]
    unique: int = stats["unique_processed"]
    dropped: int = stats["duplicated_dropped"]

    assert received == unique + dropped
