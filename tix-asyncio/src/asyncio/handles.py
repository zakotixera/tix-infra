"""Caller-owned, awaitable task results and startup readiness metadata."""

from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, cast

import anyio
from anyio.abc import TaskStatus

T = TypeVar("T")


class TaskCancelled(Exception):
    """The owned operation was cancelled (distinct from cancelling a waiter)."""


class TaskHandle(Generic[T]):
    def __init__(self, name: str | None = None) -> None:
        self.name = name
        self._done = anyio.Event()
        self._scope: anyio.CancelScope | None = None
        self._cancel_requested = False
        self._value: T | None = None
        self._error: BaseException | None = None

    @property
    def done(self) -> bool:
        return self._done.is_set()

    @property
    def cancelled(self) -> bool:
        return isinstance(self._error, TaskCancelled)

    def cancel(self) -> None:
        self._cancel_requested = True
        if self._scope is not None:
            self._scope.cancel()

    async def wait(self) -> None:
        """Wait for completion without raising the operation's exception."""
        await self._done.wait()

    def result_nowait(self) -> T:
        if not self.done:
            raise anyio.WouldBlock("task has not finished")
        if self._error is not None:
            raise self._error
        return cast(T, self._value)

    def exception_nowait(self) -> BaseException | None:
        if not self.done:
            raise anyio.WouldBlock("task has not finished")
        if self.cancelled:
            assert self._error is not None
            raise self._error
        return self._error

    async def result(self, *, timeout: float | None = None) -> T:  # noqa: ASYNC109
        with anyio.fail_after(timeout):
            await self.wait()
        return self.result_nowait()

    def __await__(self) -> Generator[Any, None, T]:
        return self.result().__await__()


@dataclass(frozen=True, slots=True)
class TaskStatistics:
    submitted: int
    active: int
    succeeded: int
    failed: int
    cancelled: int


class Readiness:
    """Track the native readiness handshake without replacing its validation."""

    def __init__(self, status: TaskStatus[Any]) -> None:
        self._status = status
        self.called = False

    def started(self, value: object = None) -> None:
        self._status.started(value)
        self.called = True
