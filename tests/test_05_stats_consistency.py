from utils.testing import create_event, create_events, get_stats, publish_events


def test_stats_structure(server_url: str) -> None:
    status, stats = get_stats(server_url)
    assert status == 200

    assert "received" in stats
    assert "unique_processed" in stats
    assert "duplicated_dropped" in stats
    assert "topics" in stats
    assert "uptime" in stats

    assert isinstance(stats["received"], int)
    assert isinstance(stats["unique_processed"], int)
    assert isinstance(stats["duplicated_dropped"], int)
    assert isinstance(stats["topics"], list)
    assert isinstance(stats["uptime"], int)

    assert stats["received"] == 0
    assert stats["unique_processed"] == 0
    assert stats["duplicated_dropped"] == 0


def test_stats_increments_on_publish(server_url: str) -> None:
    status, initial_stats = get_stats(server_url)
    assert status == 200
    initial_received = initial_stats["received"]

    events = create_events(count=10, topic="stats-topic", prefix="stats-test")
    publish_events(server_url, events, wait_seconds=2)

    status, updated_stats = get_stats(server_url)
    assert status == 200
    assert updated_stats["received"] == initial_received + 10


def test_stats_topics_updated(server_url: str) -> None:
    unique_topic = "unique-stats-topic-001"
    event = create_event(
        event_id="stats-topic-event", topic=unique_topic, message="Topic test"
    )

    publish_events(server_url, [event], wait_seconds=2)

    status, stats = get_stats(server_url)
    assert status == 200
    assert unique_topic in stats["topics"]
