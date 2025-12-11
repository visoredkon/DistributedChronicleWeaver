from typing import Any

from utils.testing import post_request

SERVER_URL = "http://localhost:8080"


def test_schema_valid_event() -> None:
    url: str = f"{SERVER_URL}/publish"

    valid_event: dict[str, Any] = {
        "event_id": "schema-valid-001",
        "topic": "schema-topic",
        "source": "test-source",
        "payload": {
            "message": "Valid message",
            "timestamp": "2025-01-01T00:00:00",
        },
        "timestamp": "2025-01-01T00:00:00",
    }

    status, _ = post_request(url, {"events": [valid_event]})
    assert status == 200


def test_schema_missing_event_id() -> None:
    url: str = f"{SERVER_URL}/publish"

    invalid_event: dict[str, Any] = {
        "topic": "schema-topic",
        "source": "test-source",
        "payload": {
            "message": "Missing event_id",
            "timestamp": "2025-01-01T00:00:00",
        },
        "timestamp": "2025-01-01T00:00:00",
    }

    status, _ = post_request(url, {"events": [invalid_event]})
    assert status == 422


def test_schema_missing_topic() -> None:
    url: str = f"{SERVER_URL}/publish"

    invalid_event: dict[str, Any] = {
        "event_id": "schema-invalid-001",
        "source": "test-source",
        "payload": {
            "message": "Missing topic",
            "timestamp": "2025-01-01T00:00:00",
        },
        "timestamp": "2025-01-01T00:00:00",
    }

    status, _ = post_request(url, {"events": [invalid_event]})
    assert status == 422


def test_schema_missing_payload() -> None:
    url: str = f"{SERVER_URL}/publish"

    invalid_event: dict[str, Any] = {
        "event_id": "schema-invalid-002",
        "topic": "schema-topic",
        "source": "test-source",
        "timestamp": "2025-01-01T00:00:00",
    }

    status, _ = post_request(url, {"events": [invalid_event]})
    assert status == 422


def test_schema_invalid_timestamp() -> None:
    url: str = f"{SERVER_URL}/publish"

    invalid_event: dict[str, Any] = {
        "event_id": "schema-invalid-003",
        "topic": "schema-topic",
        "source": "test-source",
        "payload": {
            "message": "Invalid timestamp",
            "timestamp": "2025-01-01T00:00:00",
        },
        "timestamp": "not-a-timestamp",
    }

    status, _ = post_request(url, {"events": [invalid_event]})
    assert status == 422


def test_schema_invalid_payload_timestamp() -> None:
    url: str = f"{SERVER_URL}/publish"

    invalid_event: dict[str, Any] = {
        "event_id": "schema-invalid-004",
        "topic": "schema-topic",
        "source": "test-source",
        "payload": {
            "message": "Invalid payload timestamp",
            "timestamp": "invalid",
        },
        "timestamp": "2025-01-01T00:00:00",
    }

    status, _ = post_request(url, {"events": [invalid_event]})
    assert status == 422


def test_schema_empty_events() -> None:
    url: str = f"{SERVER_URL}/publish"

    status, _ = post_request(url, {"events": []})
    assert status == 200


def test_schema_wrong_type_topic() -> None:
    url: str = f"{SERVER_URL}/publish"

    invalid_event: dict[str, Any] = {
        "event_id": "schema-invalid-005",
        "topic": 123,
        "source": "test-source",
        "payload": {
            "message": "Wrong type topic",
            "timestamp": "2025-01-01T00:00:00",
        },
        "timestamp": "2025-01-01T00:00:00",
    }

    status, _ = post_request(url, {"events": [invalid_event]})
    assert status == 422
