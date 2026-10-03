"""Fixed-rate, non-overlapping periodic work with skipped missed ticks."""

from __future__ import annotations

import math
from collections.abc import Awaitable, Callable

import anyio

from .cancellation import CancellationToken


async def run_periodic(
    operation: Callable[[], Awaitable[object]],
    interval: float,
    *,
    stop: CancellationToken | None = None,
    immediate: bool = True,
) -> None:
    if not math.isfinite(interval) or interval <= 0:
        raise ValueError("interval must be positive and finite")
    stop = stop or CancellationToken()
    deadline = anyio.current_time() + (0 if immediate else interval)
    while not stop.cancelled:
        delay = max(0, deadline - anyio.current_time())
        with anyio.move_on_after(delay):
            await stop.wait()
        if stop.cancelled:
            return
        await operation()
        deadline += interval
        now = anyio.current_time()
        if deadline <= now:
            deadline += (math.floor((now - deadline) / interval) + 1) * interval
