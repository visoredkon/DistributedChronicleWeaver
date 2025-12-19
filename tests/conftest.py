import sys
from pathlib import Path
from typing import Generator

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from utils.testing import (
    fast_reset_environment,
    is_environment_running,
    reset_environment,
    verify_clean_environment,
)

SERVER_URL = "http://localhost:8080"


def _ensure_environment_up() -> None:
    if is_environment_running(SERVER_URL):
        return

    success = reset_environment()
    assert success, "Failed to initialize environment"


@pytest.fixture(scope="session", autouse=True)
def test_session_lifecycle() -> Generator[None, None, None]:
    _ensure_environment_up()
    yield


@pytest.fixture(scope="module")
def fresh_environment() -> Generator[str, None, None]:
    _ensure_environment_up()
    success = fast_reset_environment()
    assert success, "Failed to reset environment with truncate/flush"

    is_clean = verify_clean_environment(SERVER_URL)
    assert is_clean, "Environment is not clean after reset"

    yield SERVER_URL


@pytest.fixture(scope="module")
def restart_environment() -> Generator[str, None, None]:
    _ensure_environment_up()

    success = fast_reset_environment()
    assert success, "Failed to reset data with truncate/flush"

    from utils.testing import restart_aggregator_container

    restarted = restart_aggregator_container()
    assert restarted, "Failed to restart aggregator container"

    is_clean = verify_clean_environment(SERVER_URL)
    assert is_clean, "Environment is not clean after restart"

    yield SERVER_URL


@pytest.fixture
def server_url(fresh_environment: str) -> str:
    return fresh_environment
