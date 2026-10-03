"""Task ownership with bounded admission and caller-owned result handles."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from contextvars import ContextVar
from types import TracebackType
from typing import Any, Literal, TypeVar, cast

import anyio
from anyio.abc import TaskGroup, TaskStatus

from .handles import Readiness, TaskCancelled, TaskHandle, TaskStatistics

T = TypeVar("T")
U = TypeVar("U")
_OWNER: ContextVar[object | None] = ContextVar("tix_task_owner", default=None)


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
        self._concurrency = concurrency
        self._policy = error_policy
        self._group: TaskGroup | None = None
        self._state = "new"
        self._active: set[TaskHandle[Any]] = set()
        self._submitted = self._succeeded = self._failed = self._cancelled = 0
        self._idle = anyio.Event()
        self._idle.set()

    @property
    def concurrency(self) -> int:
        return self._concurrency

    @property
    def cancel_scope(self) -> anyio.CancelScope:
        """Native group scope: cancelling it also cancels the owning task."""
        if self._group is None or self._state == "closed":
            raise RuntimeError("task manager is not active")
        return self._group.cancel_scope

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
        self,
        operation: Callable[..., Awaitable[T]],
        *args: Any,
        name: str | None = None,
        **kwargs: Any,
    ) -> TaskHandle[T]:
        self._check_admission()
        await self._capacity.acquire()
        return self._spawn(operation, args, kwargs, name)

    def start_soon(
        self,
        operation: Callable[..., Awaitable[T]],
        *args: Any,
        name: str | None = None,
        **kwargs: Any,
    ) -> TaskHandle[T]:
        """Admit immediately or raise WouldBlock, without creating queued tasks."""
        self._check_admission()
        self._capacity.acquire_nowait()
        return self._spawn(operation, args, kwargs, name)

    async def start(
        self,
        operation: Callable[..., Awaitable[Any]],
        *args: Any,
        name: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """Wait for task_status.started(value), returning value like AnyIO.start."""
        self._check_admission()
        if "task_status" in kwargs:
            raise TypeError("start supplies task_status; do not pass it explicitly")
        await self._capacity.acquire()
        handle: TaskHandle[Any] = self._admit(name)
        assert self._group is not None
        try:
            return await self._group.start(
                self._run_started, handle, operation, args, kwargs, name=name
            )
        except BaseException:
            # Native start owns cancelling/waiting for the child on handshake failure.
            # A scheduling error before the child runs still needs admission cleanup.
            if not handle.done:
                self._release(handle)
                self._submitted -= 1
            raise

    def _check_admission(self) -> None:
        if self._state != "open":
            raise RuntimeError("task manager is not accepting work")
        if _OWNER.get() is self:
            raise RuntimeError("recursive submission to the same bounded manager can deadlock")

    def _admit(self, name: str | None) -> TaskHandle[Any]:
        if self._state != "open":
            self._capacity.release()
            raise RuntimeError("task manager closed while waiting for capacity")
        handle: TaskHandle[Any] = TaskHandle(name)
        if not self._active:
            self._idle = anyio.Event()
        self._active.add(handle)
        self._submitted += 1
        return handle

    def _release(self, handle: TaskHandle[Any]) -> None:
        if handle in self._active:
            self._active.remove(handle)
            self._capacity.release()
            if not self._active:
                self._idle.set()

    def _spawn(
        self,
        operation: Callable[..., Awaitable[T]],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        name: str | None,
    ) -> TaskHandle[T]:
        handle: TaskHandle[T] = self._admit(name)
        try:
            assert self._group is not None
            self._group.start_soon(self._run, handle, operation, args, kwargs, name=name)
            return handle
        except BaseException:
            self._release(handle)
            self._submitted -= 1
            raise

    async def _run_started(
        self,
        handle: TaskHandle[Any],
        operation: Callable[..., Awaitable[Any]],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        task_status: TaskStatus[Any],
    ) -> None:
        readiness = Readiness(task_status)
        await self._run(handle, operation, args, {**kwargs, "task_status": readiness}, readiness)

    async def _run(
        self,
        handle: TaskHandle[T],
        operation: Callable[..., Awaitable[T]],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        readiness: Readiness | None = None,
    ) -> None:
        token = _OWNER.set(self)
        completed = False
        try:
            with anyio.CancelScope() as scope:
                handle._scope = scope
                if handle._cancel_requested:
                    scope.cancel()
                await anyio.lowlevel.checkpoint()
                handle._value = await operation(*args, **kwargs)
                if readiness is not None and not readiness.called:
                    raise RuntimeError("child exited without calling task_status.started()")
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
            if (
                self._policy == "fail_fast"
                or not isinstance(error, Exception)
                or (readiness is not None and not readiness.called)
            ):
                raise
        finally:
            handle._scope = None
            handle._done.set()
            self._release(handle)
            _OWNER.reset(token)

    async def wait_idle(self) -> None:
        """Wait until admitted tasks finish; future submissions remain permitted."""
        if _OWNER.get() is self:
            raise RuntimeError("a managed task cannot wait for its own manager to become idle")
        while True:
            await self._idle.wait()
            if not self._active:
                return

    def close(self) -> None:
        """Stop admission. Existing work drains when the context exits."""
        if self._state == "open":
            self._state = "closing"

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
