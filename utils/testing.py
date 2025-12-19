from os import environ
from pathlib import Path
from subprocess import PIPE, run
from time import sleep
from typing import Any, TypeAlias

from orjson import dumps, loads

EventData: TypeAlias = dict[str, str | dict[str, str]]

DEFAULT_TIMESTAMP: str = "2025-01-01T00:00:00"
DEFAULT_SERVER_URL: str = "http://localhost:8080"
DEFAULT_COMPOSE_DIR: str = "docker"


def _get_compose_path(compose_dir: str = DEFAULT_COMPOSE_DIR) -> Path:
    return Path(__file__).parent.parent / compose_dir


def _run_compose_command(
    args: list[str], compose_dir: str = DEFAULT_COMPOSE_DIR, timeout: int = 60
) -> bool:
    result = run(
        ["docker", "compose", *args],
        cwd=_get_compose_path(compose_dir),
        stdout=PIPE,
        stderr=PIPE,
        timeout=timeout,
    )
    return result.returncode == 0


def create_event(
    event_id: str,
    topic: str,
    message: str = "Test message",
    source: str = "test-service",
    timestamp: str = DEFAULT_TIMESTAMP,
) -> EventData:
    return {
        "event_id": event_id,
        "topic": topic,
        "source": source,
        "payload": {"message": message, "timestamp": timestamp},
        "timestamp": timestamp,
    }


def create_events(
    count: int,
    topic: str,
    prefix: str = "event",
    source: str = "test-service",
) -> list[EventData]:
    return [
        create_event(
            event_id=f"{prefix}-{i}",
            topic=topic,
            message=f"Message {i}",
            source=source,
        )
        for i in range(count)
    ]


def generate_test_events(
    count: int = 10, duplicate_ratio: float = 0.1, topic: str = "test-topic"
) -> list[EventData]:
    unique_count = int(count * (1 - duplicate_ratio))
    duplicate_count = count - unique_count

    events: list[EventData] = [
        create_event(event_id=f"event-{i}", topic=topic, message=f"Message {i}")
        for i in range(unique_count)
    ]

    for i in range(duplicate_count):
        if events:
            original = events[i % len(events)]
            events.append(
                create_event(
                    event_id=str(original["event_id"]),
                    topic=str(original["topic"]),
                    source=str(original["source"]),
                    message=f"Duplicate {i}",
                )
            )

    return events


def get_request(url: str, timeout: int = 30) -> tuple[int | None, str | None]:
    from http.client import HTTPResponse
    from urllib.error import HTTPError
    from urllib.request import Request, urlopen

    try:
        request = Request(url)
        response: HTTPResponse = urlopen(url=request, timeout=timeout)
        with response:
            return response.getcode(), response.read().decode("utf-8")
    except HTTPError as e:
        return e.code, e.read().decode("utf-8")
    except Exception:
        return None, None


def post_request(
    url: str, data: dict[str, Any], timeout: int = 30
) -> tuple[int | None, str | None]:
    from http.client import HTTPResponse
    from urllib.error import HTTPError
    from urllib.request import Request, urlopen

    try:
        request = Request(
            url, data=dumps(data), headers={"Content-Type": "application/json"}
        )
        response: HTTPResponse = urlopen(url=request, timeout=timeout)
        with response:
            return response.getcode(), response.read().decode("utf-8")
    except HTTPError as e:
        return e.code, e.read().decode("utf-8")
    except Exception:
        return None, None


def publish_events(
    server_url: str, events: list[EventData], wait_seconds: int = 2
) -> tuple[int | None, dict[str, Any]]:
    status, response = post_request(f"{server_url}/publish", {"events": events})
    sleep(wait_seconds)
    return status, loads(response or "{}")


def get_stats(server_url: str) -> tuple[int | None, dict[str, Any]]:
    status, response = get_request(f"{server_url}/stats")
    return status, loads(response or "{}")


def get_events(
    server_url: str, topic: str | None = None
) -> tuple[int | None, dict[str, Any]]:
    url = f"{server_url}/events"
    if topic:
        url = f"{url}?topic={topic}"
    status, response = get_request(url)
    return status, loads(response or "{}")


def wait_for_server(url: str, max_retries: int = 30) -> bool:
    for _ in range(max_retries):
        status, _ = get_request(f"{url}/health")
        if status == 200:
            return True
        sleep(1)
    return False


def is_environment_running(server_url: str = DEFAULT_SERVER_URL) -> bool:
    status, _ = get_request(f"{server_url}/health")
    return status == 200


def restart_aggregator_container(
    compose_dir: str = DEFAULT_COMPOSE_DIR, max_wait: int = 60
) -> bool:
    if not _run_compose_command(["restart", "aggregator"], compose_dir, timeout=30):
        return False

    server_url = environ.get("SERVER_URL", DEFAULT_SERVER_URL)
    return wait_for_server(server_url, max_retries=max_wait)


def reset_environment(
    compose_dir: str = DEFAULT_COMPOSE_DIR, max_wait: int = 120
) -> bool:
    if not _run_compose_command(
        ["down", "-v", "--remove-orphans"], compose_dir, timeout=60
    ):
        return False

    if not _run_compose_command(
        ["up", "-d", "--build", "--wait"], compose_dir, timeout=300
    ):
        return False

    server_url = environ.get("SERVER_URL", DEFAULT_SERVER_URL)
    return wait_for_server(server_url, max_retries=max_wait)


def verify_clean_environment(server_url: str = DEFAULT_SERVER_URL) -> bool:
    stats_status, stats_response = get_request(f"{server_url}/stats")
    if stats_status != 200:
        return False

    stats = loads(stats_response or "{}")
    return (
        stats.get("received", -1) == 0
        and stats.get("unique_processed", -1) == 0
        and stats.get("duplicated_dropped", -1) == 0
    )


def teardown_environment(compose_dir: str = DEFAULT_COMPOSE_DIR) -> bool:
    return _run_compose_command(
        ["down", "--remove-orphans", "--volumes", "--rmi", "local"],
        compose_dir,
        timeout=120,
    )


def truncate_database(compose_dir: str = DEFAULT_COMPOSE_DIR) -> bool:
    sql = "TRUNCATE processed_events, audit_log RESTART IDENTITY CASCADE; UPDATE stats SET received = 0, duplicated_dropped = 0, updated_at = NOW() WHERE id = 1;"

    return _run_compose_command(
        [
            "exec",
            "-T",
            "postgres",
            "psql",
            "-U",
            "chronicle",
            "-d",
            "chronicle",
            "-c",
            sql,
        ],
        compose_dir,
        timeout=10,
    )


def flush_redis(compose_dir: str = DEFAULT_COMPOSE_DIR) -> bool:
    return _run_compose_command(
        ["exec", "-T", "redis", "redis-cli", "FLUSHDB"],
        compose_dir,
        timeout=5,
    )


def fast_reset_environment() -> bool:
    db_success = truncate_database()
    redis_success = flush_redis()
    sleep(0.5)
    return db_success and redis_success
