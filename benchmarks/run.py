"""Local baselines, not cross-language comparisons or CI pass/fail thresholds."""

from __future__ import annotations

import argparse
import gc
import json
import platform
import statistics
import time
from collections.abc import Callable

import anyio
from tix_asyncio import TaskManager, map_concurrent
from tix_config import ConfigManager, MappingSource
from tix_lifecycle import Component, LifecycleManager
from tix_transition import MachineDefinition, State, Transition, TransitionManager


def measure(operation: Callable[[], object], iterations: int, repeats: int) -> dict[str, float]:
    samples = []
    operation()
    for _ in range(repeats):
        gc.collect()
        started = time.perf_counter()
        for _ in range(iterations):
            operation()
        samples.append((time.perf_counter() - started) / iterations)
    median = statistics.median(samples)
    return {
        "median_seconds": median,
        "operations_per_second": 1 / median,
        "min_seconds": min(samples),
        "max_seconds": max(samples),
    }


async def identity(value: int) -> int:
    return value


async def task_batch(size: int) -> None:
    async with TaskManager(concurrency=32) as tasks:
        for value in range(size):
            await tasks.submit(identity, value)


async def map_batch(size: int) -> None:
    await map_concurrent(identity, range(size), concurrency=32)


async def resource(context: object) -> None:
    return None


async def lifecycle_batch() -> None:
    async with LifecycleManager([Component(str(i), resource) for i in range(100)]):
        pass


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--iterations", type=int, default=10000)
    args = parser.parse_args()
    if args.repeats < 1 or args.iterations < 1:
        parser.error("repeats and iterations must be positive")
    manager = ConfigManager([MappingSource({"db": {"port": 5432}, "values": list(range(1000))})])
    manager.reload()
    snapshot = manager.snapshot
    machine = TransitionManager(
        MachineDefinition((State("ready"),), (Transition("tick", "ready", None),), "ready")
    )
    results = {
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "processor": platform.processor(),
            "repeats": args.repeats,
            "iterations": args.iterations,
            "backend": "asyncio",
        },
        "snapshot_lookup": measure(lambda: snapshot.at("db.port"), args.iterations, args.repeats),
        "manager_snapshot_lookup": measure(
            lambda: manager.snapshot.at("db.port"), args.iterations, args.repeats
        ),
        "config_reload_1000_values": measure(
            manager.reload, max(1, args.iterations // 100), args.repeats
        ),
        "internal_transition": measure(
            lambda: machine.dispatch("tick"), args.iterations, args.repeats
        ),
        "task_batch_1000": measure(lambda: anyio.run(task_batch, 1000), 1, args.repeats),
        "map_batch_1000": measure(lambda: anyio.run(map_batch, 1000), 1, args.repeats),
        "lifecycle_100_components": measure(lambda: anyio.run(lifecycle_batch), 1, args.repeats),
    }
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
