"""Finite retries with injectable timing and explicit exception selection."""

from __future__ import annotations

import math
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar

import anyio

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    attempts: int = 3
    initial_delay: float = 0.1
    multiplier: float = 2.0
    max_delay: float = 30.0
    jitter: bool = True
    attempt_timeout: float | None = None
    total_timeout: float | None = None

    def __post_init__(self) -> None:
        if self.attempts < 1:
            raise ValueError("attempts must be positive")
        for value in (self.initial_delay, self.max_delay):
            if not math.isfinite(value) or value < 0:
                raise ValueError("delays must be finite and nonnegative")
        if not math.isfinite(self.multiplier) or self.multiplier < 1:
            raise ValueError("multiplier must be finite and at least one")
        for timeout in (self.attempt_timeout, self.total_timeout):
            if timeout is not None and (not math.isfinite(timeout) or timeout <= 0):
                raise ValueError("timeouts must be positive and finite")


@dataclass(frozen=True, slots=True)
class RetryEvent:
    attempt: int
    exception: Exception
    delay: float


async def retry(
    operation: Callable[[], Awaitable[T]],
    *,
    policy: RetryPolicy | None = None,
    retry_if: Callable[[Exception], bool] = lambda error: isinstance(
        error, (OSError, TimeoutError)
    ),
    on_retry: Callable[[RetryEvent], object] | None = None,
    sleep: Callable[[float], Awaitable[object]] = anyio.sleep,
    random_value: Callable[[], float] = random.random,
) -> T:
    policy = policy or RetryPolicy()
    delay = min(policy.initial_delay, policy.max_delay)
    with anyio.fail_after(policy.total_timeout):
        for attempt in range(1, policy.attempts + 1):
            try:
                with anyio.fail_after(policy.attempt_timeout):
                    return await operation()
            except anyio.get_cancelled_exc_class():
                raise
            except Exception as error:
                if attempt == policy.attempts or not retry_if(error):
                    raise
                actual_delay = delay * random_value() if policy.jitter else delay
                if not 0 <= actual_delay <= delay:
                    raise ValueError("random_value must return a value in [0, 1]") from error
                if on_retry is not None:
                    on_retry(RetryEvent(attempt, error, actual_delay))
                await sleep(actual_delay)
                delay = min(policy.max_delay, delay * policy.multiplier)
    raise AssertionError("retry loop exhausted without returning or raising")
