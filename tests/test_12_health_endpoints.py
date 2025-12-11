from typing import Any

from orjson import loads
from utils.testing import get_request

SERVER_URL = "http://localhost:8080"


def test_health_endpoint() -> None:
    health_url: str = f"{SERVER_URL}/health"
    status, response = get_request(health_url)
    assert status == 200

    data: dict[str, Any] = loads(response or "{}")
    assert data["status"] == "healthy"


def test_ready_endpoint() -> None:
    ready_url: str = f"{SERVER_URL}/ready"
    status, response = get_request(ready_url)
    assert status == 200

    data: dict[str, Any] = loads(response or "{}")
    assert data["status"] == "ready"


def test_root_endpoint() -> None:
    root_url: str = SERVER_URL
    status, response = get_request(root_url)
    assert status == 200

    data: dict[str, Any] = loads(response or "{}")
    assert "message" in data
    assert "ChronicleWeaver" in data["message"]


def test_docs_endpoint() -> None:
    docs_url: str = f"{SERVER_URL}/docs"
    status, _ = get_request(docs_url)
    assert status == 200


def test_openapi_endpoint() -> None:
    openapi_url: str = f"{SERVER_URL}/openapi.json"
    status, response = get_request(openapi_url)
    assert status == 200

    data: dict[str, Any] = loads(response or "{}")
    assert "openapi" in data
    assert "paths" in data
