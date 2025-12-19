from orjson import loads
from utils.testing import get_request


def test_health_endpoint(server_url: str) -> None:
    status, response = get_request(f"{server_url}/health")
    assert status == 200
    data = loads(response or "{}")
    assert data["status"] == "healthy"


def test_ready_endpoint(server_url: str) -> None:
    status, response = get_request(f"{server_url}/ready")
    assert status == 200
    data = loads(response or "{}")
    assert data["status"] == "ready"


def test_root_endpoint(server_url: str) -> None:
    status, response = get_request(server_url)
    assert status == 200
    data = loads(response or "{}")
    assert "message" in data
    assert "ChronicleWeaver" in data["message"]


def test_docs_endpoint(server_url: str) -> None:
    status, _ = get_request(f"{server_url}/docs")
    assert status == 200


def test_openapi_endpoint(server_url: str) -> None:
    status, response = get_request(f"{server_url}/openapi.json")
    assert status == 200
    data = loads(response or "{}")
    assert "openapi" in data
    assert "paths" in data
