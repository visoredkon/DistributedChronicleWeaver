from orjson import loads
from utils.testing import create_event, get_request, publish_events


def test_audit_log_processed_event(server_url: str) -> None:
    event = create_event(
        event_id="audit-processed-001",
        topic="audit-topic",
        message="Audit processed test",
    )

    publish_events(server_url, [event], wait_seconds=3)

    audit_url = f"{server_url}/audit?event_id=audit-processed-001&topic=audit-topic"
    status, response = get_request(audit_url)
    assert status == 200

    audit_data = loads(response or "{}")
    assert audit_data["count"] == 3

    actions = {log["action"] for log in audit_data["audit_logs"]}
    assert "RECEIVED" in actions
    assert "QUEUED" in actions
    assert "PROCESSED" in actions or "DROPPED" in actions


def test_audit_log_dropped_event(server_url: str) -> None:
    event = create_event(
        event_id="audit-dropped-001",
        topic="audit-drop-topic",
        message="Audit dropped test",
    )

    publish_events(server_url, [event], wait_seconds=2)
    publish_events(server_url, [event], wait_seconds=3)

    audit_url = f"{server_url}/audit?action=DROPPED&topic=audit-drop-topic"
    status, response = get_request(audit_url)
    assert status == 200

    audit_data = loads(response or "{}")
    dropped_logs = [
        log
        for log in audit_data["audit_logs"]
        if log["event_id"] == "audit-dropped-001"
    ]
    assert len(dropped_logs) == 1


def test_audit_summary_structure(server_url: str) -> None:
    status, response = get_request(f"{server_url}/audit/summary")
    assert status == 200

    summary = loads(response or "{}")
    assert "total_received" in summary
    assert "total_queued" in summary
    assert "total_processed" in summary
    assert "total_dropped" in summary
    assert "total_failed" in summary
    assert "by_topic" in summary
    assert "by_worker" in summary


def test_audit_filter_by_action(server_url: str) -> None:
    status, response = get_request(f"{server_url}/audit?action=PROCESSED&limit=10")
    assert status == 200

    audit_data = loads(response or "{}")
    for log in audit_data["audit_logs"]:
        assert log["action"] == "PROCESSED"


def test_audit_filter_by_topic(server_url: str) -> None:
    unique_topic = "audit-filter-topic-unique"
    event = create_event(
        event_id="audit-filter-001", topic=unique_topic, message="Filter test"
    )

    publish_events(server_url, [event], wait_seconds=2)

    audit_url = f"{server_url}/audit?topic={unique_topic}"
    status, response = get_request(audit_url)
    assert status == 200

    audit_data = loads(response or "{}")
    assert audit_data["count"] == 3

    for log in audit_data["audit_logs"]:
        assert log["topic"] == unique_topic
