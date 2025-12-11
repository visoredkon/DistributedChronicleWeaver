from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
from typing import Any

from orjson import loads
from utils.testing import EventData, get_request, post_request

SERVER_URL = "http://localhost:8080"


def send_duplicate_event(event_id: str) -> tuple[int | None, str | None]:
    url: str = f"{SERVER_URL}/publish"
    event: EventData = {
        "event_id": event_id,
        "topic": "race-topic",
        "source": "test-service",
        "payload": {
            "message": "Race condition test",
            "timestamp": "2025-01-01T00:00:00",
        },
        "timestamp": "2025-01-01T00:00:00",
    }
    return post_request(url, {"events": [event]})


def test_race_condition_same_event() -> None:
    event_id: str = "race-event-001"

    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = [executor.submit(send_duplicate_event, event_id) for _ in range(50)]
        results = [f.result() for f in as_completed(futures)]

    for status, _ in results:
        assert status == 200

    sleep(3)

    events_url: str = f"{SERVER_URL}/events?topic=race-topic"
    events_status, events_response = get_request(events_url)
    assert events_status == 200

    events_data: dict[str, Any] = loads(events_response or "{}")
    race_events: list[dict[str, Any]] = [
        e for e in events_data["events"] if e["event_id"] == event_id
    ]

    assert len(race_events) == 1


def test_race_condition_stats_integrity() -> None:
    stats_url: str = f"{SERVER_URL}/stats"
    stats_status, stats_response = get_request(stats_url)
    assert stats_status == 200

    stats: dict[str, Any] = loads(stats_response or "{}")

    assert stats["received"] >= stats["unique_processed"]
    assert stats["received"] == stats["unique_processed"] + stats["duplicated_dropped"]


def test_race_condition_no_data_corruption() -> None:
    events_url: str = f"{SERVER_URL}/events"
    events_status, events_response = get_request(events_url)
    assert events_status == 200

    events_data: dict[str, Any] = loads(events_response or "{}")

    for event in events_data["events"]:
        assert "event_id" in event
        assert "topic" in event
        assert "source" in event
        assert "payload" in event
        assert "timestamp" in event
        assert isinstance(event["event_id"], str)
        assert isinstance(event["topic"], str)
