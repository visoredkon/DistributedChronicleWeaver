from utils.testing import (
    create_event,
    generate_test_events,
    get_events,
    get_stats,
    publish_events,
)


def test_deduplication_single_event(server_url: str) -> None:
    event = create_event(
        event_id="dedup-single-001", topic="dedup-topic", message="Single event"
    )

    publish_events(server_url, [event], wait_seconds=1)
    publish_events(server_url, [event], wait_seconds=2)

    status, data = get_events(server_url, topic="dedup-topic")
    assert status == 200

    dedup_events = [e for e in data["events"] if e["event_id"] == "dedup-single-001"]
    assert len(dedup_events) == 1


def test_deduplication_batch(server_url: str) -> None:
    events = generate_test_events(
        count=100, duplicate_ratio=0.5, topic="dedup-batch-topic"
    )

    publish_events(server_url, events, wait_seconds=3)

    status, stats = get_stats(server_url)
    assert status == 200
    assert stats["received"] == 102
    assert stats["duplicated_dropped"] == 51


def test_deduplication_across_topics(server_url: str) -> None:
    event1 = create_event(
        event_id="cross-topic-001", topic="topic-a", message="Event in topic A"
    )
    event2 = create_event(
        event_id="cross-topic-001", topic="topic-b", message="Event in topic B"
    )

    publish_events(server_url, [event1], wait_seconds=1)
    publish_events(server_url, [event2], wait_seconds=2)

    status_a, events_a = get_events(server_url, topic="topic-a")
    status_b, events_b = get_events(server_url, topic="topic-b")

    assert status_a == 200
    assert status_b == 200
    assert events_a["count"] == 1
    assert events_b["count"] == 1
