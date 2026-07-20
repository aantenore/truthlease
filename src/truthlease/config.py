"""Typed runtime configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Settings:
    database_path: Path = Path("truthlease.db")
    default_lease_seconds: int = 30
    max_graph_depth: int = 64
    max_impact_nodes: int = 10_000
    busy_timeout_ms: int = 5_000

    def __post_init__(self) -> None:
        if self.default_lease_seconds <= 0:
            raise ValueError("default_lease_seconds must be positive")
        if self.max_graph_depth <= 0:
            raise ValueError("max_graph_depth must be positive")
        if self.max_impact_nodes <= 0:
            raise ValueError("max_impact_nodes must be positive")
        if self.busy_timeout_ms <= 0:
            raise ValueError("busy_timeout_ms must be positive")

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            database_path=Path(os.getenv("TRUTHLEASE_DB", "truthlease.db")),
            default_lease_seconds=int(os.getenv("TRUTHLEASE_DEFAULT_LEASE_SECONDS", "30")),
            max_graph_depth=int(os.getenv("TRUTHLEASE_MAX_GRAPH_DEPTH", "64")),
            max_impact_nodes=int(os.getenv("TRUTHLEASE_MAX_IMPACT_NODES", "10000")),
            busy_timeout_ms=int(os.getenv("TRUTHLEASE_BUSY_TIMEOUT_MS", "5000")),
        )
