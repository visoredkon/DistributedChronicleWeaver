from time import sleep
from typing import Any

from orjson import loads
from utils.testing import EventData, get_request, post_request

SERVER_URL = "http://localhost:8080"


def test_audit_log_processed_event() -> None:
    url: str = f"{SERVER_URL}/publish"

    event: EventData = {
        "event_id": "audit-processed-001",
        "topic": "audit-topic",
        "source": "test-service",
        "payload": {
            "message": "Audit processed test",
            "timestamp": "2025-01-01T00:00:00",
        },
        "timestamp": "2025-01-01T00:00:00",
    }

    status, _ = post_request(url, {"events": [event]})
    assert status == 200

    sleep(3)

    audit_url: str = (
        f"{SERVER_URL}/audit?event_id=audit-processed-001&topic=audit-topic"
    )
    audit_status, audit_response = get_request(audit_url)
    assert audit_status == 200

    audit_data: dict[str, Any] = loads(audit_response or "{}")
    assert audit_data["count"] >= 3

    actions: set[str] = {log["action"] for log in audit_data["audit_logs"]}
    assert "RECEIVED" in actions
    assert "QUEUED" in actions
    assert "PROCESSED" in actions or "DROPPED" in actions


def test_audit_log_dropped_event() -> None:
    url: str = f"{SERVER_URL}/publish"

    event: EventData = {
        "event_id": "audit-dropped-001",
        "topic": "audit-drop-topic",
        "source": "test-service",
        "payload": {
            "message": "Audit dropped test",
            "timestamp": "2025-01-01T00:00:00",
        },
        "timestamp": "2025-01-01T00:00:00",
    }

    status1, _ = post_request(url, {"events": [event]})
    assert status1 == 200

    sleep(2)

    status2, _ = post_request(url, {"events": [event]})
    assert status2 == 200

    sleep(3)

    audit_url: str = f"{SERVER_URL}/audit?action=DROPPED&topic=audit-drop-topic"
    audit_status, audit_response = get_request(audit_url)
    assert audit_status == 200

    audit_data: dict[str, Any] = loads(audit_response or "{}")
    dropped_logs: list[dict[str, Any]] = [
        log
        for log in audit_data["audit_logs"]
        if log["event_id"] == "audit-dropped-001"
    ]
    assert len(dropped_logs) >= 1


def test_audit_summary_structure() -> None:
    summary_url: str = f"{SERVER_URL}/audit/summary"
    status, response = get_request(summary_url)
    assert status == 200

    summary: dict[str, Any] = loads(response or "{}")

    assert "total_received" in summary
    assert "total_queued" in summary
    assert "total_processed" in summary
    assert "total_dropped" in summary
    assert "total_failed" in summary
    assert "by_topic" in summary
    assert "by_worker" in summary


def test_audit_filter_by_action() -> None:
    audit_url: str = f"{SERVER_URL}/audit?action=PROCESSED&limit=10"
    status, response = get_request(audit_url)
    assert status == 200

    audit_data: dict[str, Any] = loads(response or "{}")

    for log in audit_data["audit_logs"]:
        assert log["action"] == "PROCESSED"


def test_audit_filter_by_topic() -> None:
    url: str = f"{SERVER_URL}/publish"
    unique_topic: str = "audit-filter-topic-unique"

    event: EventData = {
        "event_id": "audit-filter-001",
        "topic": unique_topic,
        "source": "test-service",
        "payload": {
            "message": "Filter test",
            "timestamp": "2025-01-01T00:00:00",
        },
        "timestamp": "2025-01-01T00:00:00",
    }

    status, _ = post_request(url, {"events": [event]})
    assert status == 200

    sleep(2)

    audit_url: str = f"{SERVER_URL}/audit?topic={unique_topic}"
    audit_status, audit_response = get_request(audit_url)
    assert audit_status == 200

    audit_data: dict[str, Any] = loads(audit_response or "{}")
    assert audit_data["count"] >= 1

    for log in audit_data["audit_logs"]:
        assert log["topic"] == unique_topic
