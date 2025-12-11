from time import sleep
from typing import Any

from orjson import loads
from utils.testing import EventData, get_request, post_request

SERVER_URL = "http://localhost:8080"


def test_edge_case_empty_topic() -> None:
    url: str = f"{SERVER_URL}/publish"

    event: EventData = {
        "event_id": "empty-topic-001",
        "topic": "",
        "source": "test-service",
        "payload": {
            "message": "Empty topic test",
            "timestamp": "2025-01-01T00:00:00",
        },
        "timestamp": "2025-01-01T00:00:00",
    }

    status, _ = post_request(url, {"events": [event]})
    assert status == 200


def test_edge_case_unicode_content() -> None:
    url: str = f"{SERVER_URL}/publish"

    event: EventData = {
        "event_id": "unicode-test-001",
        "topic": "unicode-topic-日本語",
        "source": "test-service-тест",
        "payload": {
            "message": "Unicode test 中文 العربية 한국어",
            "timestamp": "2025-01-01T00:00:00",
        },
        "timestamp": "2025-01-01T00:00:00",
    }

    status, _ = post_request(url, {"events": [event]})
    assert status == 200

    sleep(2)

    events_url: str = f"{SERVER_URL}/events"
    events_status, events_response = get_request(events_url)
    assert events_status == 200

    events_data: dict[str, Any] = loads(events_response or "{}")
    unicode_events: list[dict[str, Any]] = [
        e for e in events_data["events"] if e["event_id"] == "unicode-test-001"
    ]
    assert len(unicode_events) == 1


def test_edge_case_special_characters() -> None:
    url: str = f"{SERVER_URL}/publish"

    event: EventData = {
        "event_id": "special-char-001",
        "topic": "special-topic-!@#$%",
        "source": "test-service",
        "payload": {
            "message": "Special chars: <>&\"'`~",
            "timestamp": "2025-01-01T00:00:00",
        },
        "timestamp": "2025-01-01T00:00:00",
    }

    status, _ = post_request(url, {"events": [event]})
    assert status == 200


def test_edge_case_very_long_event_id() -> None:
    url: str = f"{SERVER_URL}/publish"

    long_id: str = "x" * 500

    event: EventData = {
        "event_id": long_id,
        "topic": "long-id-topic",
        "source": "test-service",
        "payload": {
            "message": "Long ID test",
            "timestamp": "2025-01-01T00:00:00",
        },
        "timestamp": "2025-01-01T00:00:00",
    }

    status, _ = post_request(url, {"events": [event]})
    assert status == 200
