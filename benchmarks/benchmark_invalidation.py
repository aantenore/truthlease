"""Run the packaged TruthLease invalidation benchmark."""

from __future__ import annotations

import argparse
import json

from truthlease.benchmark import run_benchmark


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--nodes", type=int, default=1_000)
    parser.add_argument("--partitions", type=int, default=20)
    args = parser.parse_args()
    print(json.dumps(run_benchmark(nodes=args.nodes, partitions=args.partitions), indent=2))


if __name__ == "__main__":
    main()
