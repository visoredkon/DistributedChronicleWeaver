from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep

from utils.testing import create_event, get_events, get_stats, post_request


def test_race_condition_same_event(server_url: str) -> None:
    event_id = "race-event-001"
    event = create_event(
        event_id=event_id, topic="race-topic", message="Race condition test"
    )

    def send_event() -> int | None:
        url = f"{server_url}/publish"
        status, _ = post_request(url, {"events": [event]})
        return status

    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = [executor.submit(send_event) for _ in range(50)]
        results = [f.result() for f in as_completed(futures)]

    for status in results:
        assert status == 200

    sleep(3)

    status, events_data = get_events(server_url, topic="race-topic")
    assert status == 200

    race_events = [e for e in events_data["events"] if e["event_id"] == event_id]
    assert len(race_events) == 1


def test_race_condition_stats_integrity(server_url: str) -> None:
    status, stats = get_stats(server_url)
    assert status == 200

    assert stats["received"] >= stats["unique_processed"]
    assert stats["received"] == stats["unique_processed"] + stats["duplicated_dropped"]


def test_race_condition_no_data_corruption(server_url: str) -> None:
    status, events_data = get_events(server_url)
    assert status == 200

    for event in events_data["events"]:
        assert "event_id" in event
        assert "topic" in event
        assert "source" in event
        assert "payload" in event
        assert "timestamp" in event
        assert isinstance(event["event_id"], str)
        assert isinstance(event["topic"], str)
