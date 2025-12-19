from utils.testing import (
    create_events,
    generate_test_events,
    get_events,
    get_stats,
    publish_events,
)


def test_integration_full_workflow(server_url: str) -> None:
    topic = "integration-topic"
    events = generate_test_events(count=100, duplicate_ratio=0.3, topic=topic)

    publish_events(server_url, events, wait_seconds=5)

    expected_unique = 70
    expected_dupes = 30

    status, stats = get_stats(server_url)
    assert status == 200
    assert stats["received"] == 100
    assert stats["unique_processed"] == expected_unique
    assert stats["duplicated_dropped"] == expected_dupes
    assert topic in stats["topics"]

    status, events_data = get_events(server_url, topic=topic)
    assert status == 200
    assert events_data["count"] == expected_unique

    event_ids = {e["event_id"] for e in events_data["events"]}
    assert len(event_ids) == events_data["count"]


def test_integration_multiple_topics(server_url: str) -> None:
    topics = ["multi-topic-a", "multi-topic-b", "multi-topic-c"]

    for topic in topics:
        events = create_events(count=10, topic=topic, prefix=f"multi-{topic}")
        publish_events(server_url, events, wait_seconds=1)

    from time import sleep

    sleep(2)

    status, stats = get_stats(server_url)
    assert status == 200
    for topic in topics:
        assert topic in stats["topics"]


def test_integration_api_consistency(server_url: str) -> None:
    status, stats = get_stats(server_url)
    assert status == 200

    status, events_data = get_events(server_url)
    assert status == 200

    assert events_data["count"] == stats["unique_processed"]
    assert stats["received"] == stats["unique_processed"] + stats["duplicated_dropped"]
