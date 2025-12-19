from utils.testing import create_events, get_events, get_stats, publish_events


def test_events_structure(server_url: str) -> None:
    status, events_data = get_events(server_url)
    assert status == 200

    assert "count" in events_data
    assert "events" in events_data
    assert isinstance(events_data["count"], int)
    assert isinstance(events_data["events"], list)

    assert events_data["count"] == 0
    assert events_data["events"] == []


def test_events_filter_by_topic(server_url: str) -> None:
    topic = "filter-topic"
    events = create_events(count=5, topic=topic, prefix="filter-event")

    publish_events(server_url, events, wait_seconds=2)

    status, events_data = get_events(server_url, topic=topic)
    assert status == 200
    assert events_data["count"] == 5

    for event in events_data["events"]:
        assert event["topic"] == topic


def test_events_count_matches_unique_processed(server_url: str) -> None:
    status, stats = get_stats(server_url)
    assert status == 200

    status, events_data = get_events(server_url)
    assert status == 200

    assert events_data["count"] == stats["unique_processed"]


def test_events_unique_ids(server_url: str) -> None:
    status, events_data = get_events(server_url)
    assert status == 200

    event_keys: set[tuple[str, str]] = set()
    for event in events_data["events"]:
        key = (event["event_id"], event["topic"])
        assert key not in event_keys
        event_keys.add(key)
