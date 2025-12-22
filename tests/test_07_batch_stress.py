from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep, time
from uuid import uuid4

from utils.testing import (
    create_events,
    generate_test_events,
    get_events,
    post_request,
    publish_events,
)


def wait_for_processing(
    server_url: str, topic: str, expected_count: int, timeout: int = 60
) -> int:
    start = time()
    actual_count = 0
    while time() - start < timeout:
        status, events_data = get_events(server_url, topic=topic)
        if status == 200:
            actual_count = events_data["count"]
            if actual_count >= expected_count:
                return actual_count
        sleep(2)
    return actual_count


def test_batch_stress_20000_events(server_url: str) -> None:
    event_count = 20000
    duplicate_ratio = 0.3
    batch_size = 1000
    unique_topic = f"stress-topic-{uuid4().hex[:8]}"

    events = generate_test_events(
        count=event_count, duplicate_ratio=duplicate_ratio, topic=unique_topic
    )

    expected_unique = int(event_count * (1 - duplicate_ratio))

    url = f"{server_url}/publish"

    def send_batch(batch: list[dict]) -> tuple[int | None, int]:
        status, _ = post_request(url, {"events": batch})
        return status, len(batch)

    batches = [events[i : i + batch_size] for i in range(0, len(events), batch_size)]

    start_time = time()
    total_sent = 0

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(send_batch, batch) for batch in batches]
        for future in as_completed(futures):
            status, count = future.result()
            assert status == 200
            total_sent += count

    publish_time = time() - start_time

    assert total_sent == event_count
    assert publish_time < 60

    actual_count = wait_for_processing(
        server_url, unique_topic, expected_unique, timeout=60
    )
    assert actual_count == expected_unique


def test_batch_stress_throughput(server_url: str) -> None:
    unique_topic = f"throughput-topic-{uuid4().hex[:8]}"
    events = create_events(count=500, topic=unique_topic, prefix="throughput-event")

    url = f"{server_url}/publish"
    start_time = time()
    status, _ = post_request(url, {"events": events})
    elapsed_time = time() - start_time

    assert status == 200

    throughput = 500 / elapsed_time if elapsed_time > 0 else 0
    assert throughput > 50


def test_batch_stress_large_payload(server_url: str) -> None:
    unique_topic = f"large-topic-{uuid4().hex[:8]}"
    large_message = "x" * 1000
    events = [
        {
            "event_id": f"large-payload-{i}",
            "topic": unique_topic,
            "source": "test-service",
            "payload": {"message": large_message, "timestamp": "2025-01-01T00:00:00"},
            "timestamp": "2025-01-01T00:00:00",
        }
        for i in range(100)
    ]

    publish_events(server_url, events, wait_seconds=1)

    actual_count = wait_for_processing(server_url, unique_topic, 100, timeout=30)
    assert actual_count == 100
