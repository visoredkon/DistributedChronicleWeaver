from datetime import datetime
from http.client import HTTPResponse
from os import environ
from time import sleep
from typing import Any, TypeAlias
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from orjson import dumps

EventData: TypeAlias = dict[str, str | dict[str, str]]


def generate_test_events(
    count: int = 10, duplicate_ratio: float = 0.1, topic: str = "test-topic"
) -> list[EventData]:
    events: list[EventData] = []
    unique_count: int = int(count * (1 - duplicate_ratio))
    duplicate_count: int = count - unique_count

    for i in range(unique_count):
        events.append(
            {
                "event_id": f"event-{i}",
                "topic": topic,
                "source": "test-service",
                "payload": {
                    "message": f"Message {i}",
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
                        "message": f"Duplicate {i}",
                        "timestamp": datetime.now().isoformat(),
                    },
                    "timestamp": datetime.now().isoformat(),
                }
            )

    return events


def get_request(url: str, timeout: int = 30) -> tuple[int | None, str | None]:
    try:
        request: Request = Request(url)

        response: HTTPResponse = urlopen(url=request, timeout=timeout)
        with response:
            status_code: int = response.getcode()
            response_text: str = response.read().decode(encoding="utf-8")

            return status_code, response_text
    except HTTPError as e:
        return e.code, e.read().decode("utf-8")
    except Exception:
        return None, None


def post_request(
    url: str, data: dict[str, Any], timeout: int = 30
) -> tuple[int | None, str | None]:
    try:
        request: Request = Request(
            url, data=dumps(data), headers={"Content-Type": "application/json"}
        )

        response: HTTPResponse = urlopen(url=request, timeout=timeout)
        with response:
            status_code: int = response.getcode()
            response_text: str = response.read().decode("utf-8")

            return status_code, response_text
    except HTTPError as e:
        return e.code, e.read().decode("utf-8")
    except Exception:
        return None, None


def wait_for_server(url: str, max_retries: int = 30) -> bool:
    for _ in range(max_retries):
        status, _ = get_request(f"{url}/health")
        if status == 200:
            return True
        sleep(1)
    return False
