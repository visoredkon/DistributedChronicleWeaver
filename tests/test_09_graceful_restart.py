from utils.testing import (
    create_event,
    create_events,
    get_events,
    publish_events,
    restart_aggregator_container,
)


def test_graceful_restart_data_preserved(restart_environment: str) -> None:
    server_url = restart_environment
    events = create_events(
        count=25, topic="graceful-restart-topic", prefix="graceful-restart"
    )

    publish_events(server_url, events, wait_seconds=5)

    status, pre_data = get_events(server_url, topic="graceful-restart-topic")
    assert status == 200
    assert pre_data["count"] == 25

    restarted = restart_aggregator_container()
    assert restarted, "Failed to restart aggregator container"

    status, post_data = get_events(server_url, topic="graceful-restart-topic")
    assert status == 200
    assert post_data["count"] == 25

    expected_ids = {f"graceful-restart-{i}" for i in range(25)}
    actual_ids = {e["event_id"] for e in post_data["events"]}
    assert expected_ids == actual_ids


def test_graceful_restart_dedup_works_after_restart(restart_environment: str) -> None:
    server_url = restart_environment
    event = create_event(
        event_id="graceful-restart-0",
        topic="graceful-restart-topic",
        message="Duplicate after graceful restart",
    )

    publish_events(server_url, [event], wait_seconds=3)

    status, events_data = get_events(server_url, topic="graceful-restart-topic")
    assert status == 200

    matching_events = [
        e for e in events_data["events"] if e["event_id"] == "graceful-restart-0"
    ]
    assert len(matching_events) == 1
