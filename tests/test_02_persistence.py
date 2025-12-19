from utils.testing import (
    create_events,
    get_events,
    get_stats,
    publish_events,
    restart_aggregator_container,
)


def test_persistence_after_restart(restart_environment: str) -> None:
    server_url = restart_environment
    events = create_events(
        count=50, topic="persist-restart-topic", prefix="persist-restart"
    )

    publish_events(server_url, events, wait_seconds=5)

    status, pre_stats = get_stats(server_url)
    assert status == 200
    assert pre_stats["unique_processed"] == 50

    restarted = restart_aggregator_container()
    assert restarted, "Failed to restart aggregator container"

    status, post_stats = get_stats(server_url)
    assert status == 200
    assert post_stats["unique_processed"] == 50

    status, events_data = get_events(server_url, topic="persist-restart-topic")
    assert status == 200
    assert events_data["count"] == 50

    event_ids = {e["event_id"] for e in events_data["events"]}
    expected_ids = {f"persist-restart-{i}" for i in range(50)}
    assert expected_ids == event_ids
