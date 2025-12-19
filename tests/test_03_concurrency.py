from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep

from utils.testing import (
    EventData,
    create_event,
    create_events,
    get_events,
    get_stats,
    post_request,
)


def test_concurrency_parallel_requests(server_url: str) -> None:
    batches: list[list[EventData]] = []
    for batch_id in range(10):
        batch = create_events(
            count=10, topic="concurrent-topic", prefix=f"concurrent-{batch_id}"
        )
        batches.append(batch)

    def send_batch(batch: list[EventData]) -> int | None:
        url = f"{server_url}/publish"
        status, _ = post_request(url, {"events": batch})
        return status

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(send_batch, batch) for batch in batches]
        results = [f.result() for f in as_completed(futures)]

    for status in results:
        assert status == 200

    sleep(3)

    status, events_data = get_events(server_url, topic="concurrent-topic")
    assert status == 200
    assert events_data["count"] == 100


def test_concurrency_duplicate_parallel(server_url: str) -> None:
    same_event = create_event(
        event_id="same-concurrent-001",
        topic="concurrent-dup-topic",
        message="Same event parallel",
    )

    def send_event() -> int | None:
        url = f"{server_url}/publish"
        status, _ = post_request(url, {"events": [same_event]})
        return status

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(send_event) for _ in range(20)]
        results = [f.result() for f in as_completed(futures)]

    for status in results:
        assert status == 200

    sleep(3)

    status, events_data = get_events(server_url, topic="concurrent-dup-topic")
    assert status == 200

    same_events = [
        e for e in events_data["events"] if e["event_id"] == "same-concurrent-001"
    ]
    assert len(same_events) == 1


def test_concurrency_stats_consistency(server_url: str) -> None:
    status, stats = get_stats(server_url)
    assert status == 200

    received = stats["received"]
    unique = stats["unique_processed"]
    dropped = stats["duplicated_dropped"]

    assert received == unique + dropped
