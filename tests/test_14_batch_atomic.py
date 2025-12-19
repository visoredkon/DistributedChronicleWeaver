from utils.testing import create_event, create_events, get_events, publish_events


def test_batch_atomic_all_valid(server_url: str) -> None:
    events = create_events(count=20, topic="batch-atomic-topic", prefix="batch-atomic")

    publish_events(server_url, events, wait_seconds=2)

    status, events_data = get_events(server_url, topic="batch-atomic-topic")
    assert status == 200
    assert events_data["count"] == 20


def test_batch_atomic_partial_duplicates(server_url: str) -> None:
    unique_events = create_events(
        count=10, topic="batch-partial-topic", prefix="batch-partial"
    )
    duplicate_events = [
        create_event(
            event_id=f"batch-partial-{i}",
            topic="batch-partial-topic",
            message=f"Batch partial duplicate {i}",
        )
        for i in range(5)
    ]

    all_events = unique_events + duplicate_events

    publish_events(server_url, all_events, wait_seconds=2)

    status, events_data = get_events(server_url, topic="batch-partial-topic")
    assert status == 200
    assert events_data["count"] == 10
