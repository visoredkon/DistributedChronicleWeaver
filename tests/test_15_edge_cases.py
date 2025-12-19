from utils.testing import get_events, post_request


def test_edge_case_empty_topic(server_url: str) -> None:
    url = f"{server_url}/publish"
    event = {
        "event_id": "empty-topic-001",
        "topic": "",
        "source": "test-service",
        "payload": {"message": "Empty topic test", "timestamp": "2025-01-01T00:00:00"},
        "timestamp": "2025-01-01T00:00:00",
    }
    status, _ = post_request(url, {"events": [event]})
    assert status == 200


def test_edge_case_unicode_content(server_url: str) -> None:
    url = f"{server_url}/publish"
    event = {
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

    from time import sleep

    sleep(2)

    status, events_data = get_events(server_url)
    assert status == 200

    unicode_events = [
        e for e in events_data["events"] if e["event_id"] == "unicode-test-001"
    ]
    assert len(unicode_events) == 1


def test_edge_case_special_characters(server_url: str) -> None:
    url = f"{server_url}/publish"
    event = {
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


def test_edge_case_very_long_event_id(server_url: str) -> None:
    url = f"{server_url}/publish"
    long_id = "x" * 500
    event = {
        "event_id": long_id,
        "topic": "long-id-topic",
        "source": "test-service",
        "payload": {"message": "Long ID test", "timestamp": "2025-01-01T00:00:00"},
        "timestamp": "2025-01-01T00:00:00",
    }
    status, _ = post_request(url, {"events": [event]})
    assert status == 200
