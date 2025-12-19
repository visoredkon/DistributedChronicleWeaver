from utils.testing import (
    create_event,
    create_events,
    get_events,
    get_stats,
    publish_events,
)


def test_transaction_isolation_unique_constraint(server_url: str) -> None:
    event = create_event(
        event_id="isolation-test-001", topic="isolation-topic", message="Isolation test"
    )

    for _ in range(10):
        publish_events(server_url, [event], wait_seconds=0)

    from time import sleep

    sleep(2)

    status, events_data = get_events(server_url, topic="isolation-topic")
    assert status == 200

    isolation_events = [
        e for e in events_data["events"] if e["event_id"] == "isolation-test-001"
    ]
    assert len(isolation_events) == 1


def test_transaction_atomic_stats_update(server_url: str) -> None:
    events = create_events(count=50, topic="atomic-topic", prefix="atomic-stats")

    publish_events(server_url, events, wait_seconds=3)

    status, stats = get_stats(server_url)
    assert status == 200
    assert stats["received"] == stats["unique_processed"] + stats["duplicated_dropped"]
