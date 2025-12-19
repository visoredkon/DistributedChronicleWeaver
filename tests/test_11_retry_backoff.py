from time import sleep

from utils.testing import create_event, get_events, get_stats, publish_events


def test_retry_with_valid_request(server_url: str) -> None:
    event = create_event(
        event_id="retry-test-001", topic="retry-topic", message="Retry test"
    )

    for _ in range(3):
        publish_events(server_url, [event], wait_seconds=0)

    sleep(2)

    status, events_data = get_events(server_url, topic="retry-topic")
    assert status == 200

    retry_events = [
        e for e in events_data["events"] if e["event_id"] == "retry-test-001"
    ]
    assert len(retry_events) == 1


def test_retry_stats_accuracy(server_url: str) -> None:
    status, stats = get_stats(server_url)
    assert status == 200

    assert stats["received"] == 3
    assert stats["unique_processed"] == 1
    assert stats["duplicated_dropped"] == 2
