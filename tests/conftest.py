import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures" / "gbfs"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / f"{name}.json").read_text())


@pytest.fixture
def status_doc() -> dict:
    return load_fixture("station_status")


@pytest.fixture
def info_doc() -> dict:
    return load_fixture("station_information")


class FakePublisher:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, dict]] = []

    def publish(self, topic: str, key: str, value: bytes) -> None:
        self.sent.append((topic, key, json.loads(value)))

    def flush(self, timeout: float = 30.0) -> int:
        return 0

    def on(self, topic: str) -> list[tuple[str, dict]]:
        return [(k, v) for t, k, v in self.sent if t == topic]


@pytest.fixture
def publisher() -> FakePublisher:
    return FakePublisher()
