"""Bounded concurrent result streams with explicit consumer lifetime."""

from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable
from contextlib import asynccontextmanager
from typing import TypeVar

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from ._mapping import report_independent_cancellation

T = TypeVar("T")
U = TypeVar("U")


@asynccontextmanager
async def map_unordered(
    operation: Callable[[T], Awaitable[U]],
    items: Iterable[T] | AsyncIterable[T],
    *,
    concurrency: int = 32,
    buffer_size: int = 0,
) -> AsyncIterator[AsyncIterator[U]]:
    """Yield completion-order results; exit cancels pending producer/worker work.

    Enter and exit in the same task, as with an AnyIO task group. Buffer size bounds
    both input and output streams. The caller owns closing an input async iterator.
    """
    if concurrency < 1 or buffer_size < 0:
        raise ValueError("concurrency must be positive and buffer_size nonnegative")
    input_send, input_receive = anyio.create_memory_object_stream[T](buffer_size)
    output_send, output_receive = anyio.create_memory_object_stream[U](buffer_size)

    async def produce() -> None:
        with report_independent_cancellation():
            async with input_send:
                if isinstance(items, AsyncIterable):
                    async for item in items:
                        await input_send.send(item)
                else:
                    for item in items:
                        await input_send.send(item)

    async def worker(source: MemoryObjectReceiveStream[T], sink: MemoryObjectSendStream[U]) -> None:
        with report_independent_cancellation():
            async with source, sink:
                async for item in source:
                    await sink.send(await operation(item))

    async with input_send, input_receive, output_send, output_receive:
        consumer_error: BaseException | None = None
        async with anyio.create_task_group() as group:
            for _ in range(concurrency):
                group.start_soon(worker, input_receive.clone(), output_send.clone())
            await input_receive.aclose()
            await output_send.aclose()
            group.start_soon(produce)
            try:
                yield output_receive
            except BaseException as error:
                consumer_error = error
                raise
            finally:
                group.cancel_scope.cancel()
        # Older AnyIO versions can suppress native caller cancellation after our
        # explicit group cancellation. Consumer failure must still escape after join.
        if consumer_error is not None:
            raise consumer_error
