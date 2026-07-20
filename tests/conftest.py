from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from truthlease import SQLiteStore, TruthLease


class ManualClock:
    def __init__(self) -> None:
        self.current = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)

    def now(self) -> datetime:
        return self.current

    def advance(self, **kwargs: float) -> None:
        self.current += timedelta(**kwargs)


class SequentialIds:
    def __init__(self) -> None:
        self.counter = 0

    def new(self, prefix: str) -> str:
        self.counter += 1
        return f"{prefix}_{self.counter:04d}"


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "truthlease.db"


@pytest.fixture
def runtime(db_path: Path, clock: ManualClock) -> TruthLease:
    return TruthLease(SQLiteStore(db_path), clock=clock, ids=SequentialIds())
