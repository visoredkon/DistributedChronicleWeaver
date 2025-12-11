from asyncio import run, sleep
from datetime import datetime
from http.client import HTTPResponse
from os import getenv
from time import time
from typing import TypeAlias
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from loguru import logger
from orjson import dumps

EventData: TypeAlias = dict[str, str | dict[str, str]]
RequestData: TypeAlias = dict[str, list[EventData]]


def make_request_with_retry(
    url: str, data: RequestData, max_retries: int = 5
) -> tuple[int | None, str | None]:
    for attempt in range(max_retries):
        try:
            request: Request = Request(
                url, data=dumps(data), headers={"Content-Type": "application/json"}
            )

            response: HTTPResponse = urlopen(url=request, timeout=30)
            with response:
                status_code: int = response.getcode()
                response_text: str = response.read().decode(encoding="utf-8")

                return status_code, response_text
        except HTTPError as e:
            return e.code, e.read().decode("utf-8")
        except Exception as e:
            backoff_time: float = min(2 ** (attempt + 1), 30)
            logger.warning(
                f"Request failed (attempt {attempt + 1}/{max_retries}): {e}, "
                f"retrying in {backoff_time}s"
            )

            if attempt < max_retries - 1:
                from time import sleep as sync_sleep

                sync_sleep(backoff_time)

    logger.error(f"All {max_retries} attempts failed")
    return None, None


def generate_test_events(
    count: int = 20000, duplicate_ratio: float = 0.3
) -> list[EventData]:
    events: list[EventData] = []
    unique_count: int = int(count * (1 - duplicate_ratio))
    duplicate_count: int = count - unique_count

    for i in range(unique_count):
        events.append(
            {
                "event_id": f"publisher-event-{i}",
                "topic": f"topic-{i % 5}",
                "source": "publisher-service",
                "payload": {
                    "message": f"Message from publisher {i}",
                    "timestamp": datetime.now().isoformat(),
                },
                "timestamp": datetime.now().isoformat(),
            }
        )

    for i in range(duplicate_count):
        if events:
            original: EventData = events[i % len(events)]
            events.append(
                {
                    "event_id": original["event_id"],
                    "topic": original["topic"],
                    "source": original["source"],
                    "payload": {
                        "message": f"Duplicate message {i}",
                        "timestamp": datetime.now().isoformat(),
                    },
                    "timestamp": datetime.now().isoformat(),
                }
            )

    return events


async def main() -> None:
    aggregator_url: str = getenv("AGGREGATOR_URL", default="http://localhost:8080")
    event_count: int = int(getenv("EVENT_COUNT", default="20000"))
    duplicate_ratio: float = float(getenv("DUPLICATE_RATIO", default="0.3"))
    batch_size: int = int(getenv("BATCH_SIZE", default="1000"))

    url: str = f"{aggregator_url}/publish"

    logger.info(
        f"Publisher starting stress test: {event_count} events, "
        f"{duplicate_ratio * 100:.0f}% duplicates, batch size {batch_size}"
    )

    events: list[EventData] = generate_test_events(
        count=event_count, duplicate_ratio=duplicate_ratio
    )

    start_time: float = time()
    total_sent: int = 0
    failed_batches: int = 0

    for i in range(0, len(events), batch_size):
        batch: list[EventData] = events[i : i + batch_size]
        data: RequestData = {"events": batch}

        status, response = make_request_with_retry(url, data)

        if status == 200:
            total_sent += len(batch)
            logger.info(f"Batch {i // batch_size + 1}: Sent {len(batch)} events")
        else:
            failed_batches += 1
            logger.error(f"Batch {i // batch_size + 1} failed: {response}")

        await sleep(0.1)

    elapsed_time: float = time() - start_time
    throughput: float = total_sent / elapsed_time if elapsed_time > 0 else 0

    logger.info(
        f"Stress test completed: "
        f"sent {total_sent}/{len(events)} events in {elapsed_time:.2f}s "
        f"({throughput:.2f} events/s), {failed_batches} failed batches"
    )


if __name__ == "__main__":
    run(main=main())
