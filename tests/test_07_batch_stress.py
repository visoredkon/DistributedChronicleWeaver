from time import sleep, time

from utils.testing import (
    create_events,
    generate_test_events,
    get_events,
    get_stats,
    post_request,
    publish_events,
)


def test_batch_stress_20000_events(server_url: str) -> None:
    event_count = 20000
    duplicate_ratio = 0.3
    batch_size = 1000

    events = generate_test_events(
        count=event_count, duplicate_ratio=duplicate_ratio, topic="stress-topic"
    )

    expected_unique = int(event_count * (1 - duplicate_ratio))
    expected_duplicates = event_count - expected_unique

    start_time = time()
    total_sent = 0

    url = f"{server_url}/publish"
    for i in range(0, len(events), batch_size):
        batch = events[i : i + batch_size]
        status, _ = post_request(url, {"events": batch})
        assert status == 200
        total_sent += len(batch)

    publish_time = time() - start_time

    assert total_sent == event_count
    assert publish_time < 60

    sleep(10)

    status, stats = get_stats(server_url)
    assert status == 200
    assert stats["received"] == event_count
    assert stats["unique_processed"] == expected_unique
    assert stats["duplicated_dropped"] == expected_duplicates


def test_batch_stress_throughput(server_url: str) -> None:
    events = create_events(
        count=500, topic="throughput-topic", prefix="throughput-event"
    )

    url = f"{server_url}/publish"
    start_time = time()
    status, _ = post_request(url, {"events": events})
    elapsed_time = time() - start_time

    assert status == 200

    throughput = 500 / elapsed_time if elapsed_time > 0 else 0
    assert throughput > 50


def test_batch_stress_large_payload(server_url: str) -> None:
    large_message = "x" * 1000
    events = [
        {
            "event_id": f"large-payload-{i}",
            "topic": "large-topic",
            "source": "test-service",
            "payload": {"message": large_message, "timestamp": "2025-01-01T00:00:00"},
            "timestamp": "2025-01-01T00:00:00",
        }
        for i in range(100)
    ]

    publish_events(server_url, events, wait_seconds=3)

    status, events_data = get_events(server_url, topic="large-topic")
    assert status == 200
    assert events_data["count"] == 100
