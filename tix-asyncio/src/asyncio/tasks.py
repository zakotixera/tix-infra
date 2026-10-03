"""Task ownership with bounded admission and caller-owned result handles."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from contextvars import ContextVar
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Generic, Literal, TypeVar, cast

import anyio
from anyio.abc import TaskGroup

T = TypeVar("T")
U = TypeVar("U")
_OWNER: ContextVar[object | None] = ContextVar("tix_task_owner", default=None)


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

    def cancel(self) -> None:
        self._cancel_requested = True
        if self._scope is not None:
            self._scope.cancel()

    async def result(self, *, timeout: float | None = None) -> T:  # noqa: ASYNC109
        with anyio.fail_after(timeout):
            await self._done.wait()
        if self._error is not None:
            raise self._error
        return cast(T, self._value)


@dataclass(frozen=True, slots=True)
class TaskStatistics:
    submitted: int
    active: int
    succeeded: int
    failed: int
    cancelled: int


class TaskManager:
    """Single-use scope. Never submit recursively to the same bounded manager."""

    def __init__(
        self, *, concurrency: int = 32, error_policy: Literal["fail_fast", "collect"] = "fail_fast"
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be positive")
        if error_policy not in ("fail_fast", "collect"):
            raise ValueError("invalid error policy")
        self._capacity = anyio.Semaphore(concurrency)
        self._policy = error_policy
        self._group: TaskGroup | None = None
        self._state = "new"
        self._active: set[TaskHandle[Any]] = set()
        self._submitted = self._succeeded = self._failed = self._cancelled = 0

    @property
    def statistics(self) -> TaskStatistics:
        return TaskStatistics(
            self._submitted, len(self._active), self._succeeded, self._failed, self._cancelled
        )

    async def __aenter__(self) -> TaskManager:
        if self._state != "new":
            raise RuntimeError("task managers are single-use")
        self._group = anyio.create_task_group()
        await self._group.__aenter__()
        self._state = "open"
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        self._state = "closing"
        assert self._group is not None
        try:
            return await self._group.__aexit__(exc_type, exc, tb)
        finally:
            self._state = "closed"

    async def submit(
        self, operation: Callable[..., Awaitable[T]], *args: Any, name: str | None = None
    ) -> TaskHandle[T]:
        if self._state != "open":
            raise RuntimeError("task manager is not accepting work")
        if _OWNER.get() is self:
            raise RuntimeError("recursive submission to the same bounded manager can deadlock")
        await self._capacity.acquire()
        try:
            if self._state != "open":
                raise RuntimeError("task manager closed while waiting for capacity")
            handle: TaskHandle[T] = TaskHandle(name)
            self._active.add(handle)
            assert self._group is not None
            self._group.start_soon(self._run, handle, operation, args, name=name)
            self._submitted += 1
            return handle
        except BaseException:
            if "handle" in locals():
                self._active.discard(handle)
            self._capacity.release()
            raise

    async def _run(
        self, handle: TaskHandle[T], operation: Callable[..., Awaitable[T]], args: tuple[Any, ...]
    ) -> None:
        token = _OWNER.set(self)
        completed = False
        try:
            with anyio.CancelScope() as scope:
                handle._scope = scope
                if handle._cancel_requested:
                    scope.cancel()
                await anyio.lowlevel.checkpoint()
                handle._value = await operation(*args)
                completed = True
            if completed:
                self._succeeded += 1
            else:
                handle._error = TaskCancelled("task was cancelled")
                self._cancelled += 1
        except anyio.get_cancelled_exc_class():
            handle._error = TaskCancelled("task was cancelled")
            self._cancelled += 1
            raise
        except BaseException as error:
            handle._error = error
            self._failed += 1
            if self._policy == "fail_fast" or not isinstance(error, Exception):
                raise
        finally:
            handle._scope = None
            handle._done.set()
            self._active.discard(handle)
            self._capacity.release()
            _OWNER.reset(token)

    def cancel(self) -> None:
        """Cancel admitted work without cancelling the context-manager owner."""
        for handle in tuple(self._active):
            handle.cancel()


async def map_concurrent(
    operation: Callable[[T], Awaitable[U]],
    items: Iterable[T],
    *,
    concurrency: int = 32,
    buffer_size: int = 0,
) -> list[U]:
    """Fixed worker count and bounded input buffering; ordered output takes O(n)."""
    if concurrency < 1 or buffer_size < 0:
        raise ValueError("concurrency must be positive and buffer_size nonnegative")
    send, receive = anyio.create_memory_object_stream[tuple[int, T]](buffer_size)
    results: list[U | None] = []

    async def worker() -> None:
        async with receive.clone() as stream:
            async for index, item in stream:
                results[index] = await operation(item)

    async with send, receive, anyio.create_task_group() as group:
        for _ in range(concurrency):
            group.start_soon(worker)
        async with send:
            for index, item in enumerate(items):
                results.append(None)
                await send.send((index, item))
    return cast(list[U], results)
