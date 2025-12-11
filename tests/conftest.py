import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

TEST_DATABASE_URL = "postgresql://chronicle:chronicle@localhost:5432/chronicle_test"
TEST_REDIS_URL = "redis://localhost:6379/1"


@pytest.fixture
def event_data() -> dict[str, str | dict[str, str]]:
    return {
        "event_id": "test-event-001",
        "topic": "test-topic",
        "source": "test-source",
        "payload": {
            "message": "Test message",
            "timestamp": "2025-01-01T00:00:00",
        },
        "timestamp": "2025-01-01T00:00:00",
    }


@pytest.fixture
def batch_events() -> list[dict[str, str | dict[str, str]]]:
    events: list[dict[str, str | dict[str, str]]] = []
    for i in range(100):
        events.append(
            {
                "event_id": f"batch-event-{i}",
                "topic": "batch-topic",
                "source": "batch-source",
                "payload": {
                    "message": f"Batch message {i}",
                    "timestamp": "2025-01-01T00:00:00",
                },
                "timestamp": "2025-01-01T00:00:00",
            }
        )
    return events


@pytest.fixture
def duplicate_events() -> list[dict[str, str | dict[str, str]]]:
    base_event: dict[str, str | dict[str, str]] = {
        "event_id": "duplicate-event-001",
        "topic": "duplicate-topic",
        "source": "duplicate-source",
        "payload": {
            "message": "Duplicate message",
            "timestamp": "2025-01-01T00:00:00",
        },
        "timestamp": "2025-01-01T00:00:00",
    }
    return [base_event.copy() for _ in range(10)]
