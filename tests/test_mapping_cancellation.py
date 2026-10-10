import asyncio
from contextlib import aclosing

import anyio
import pytest
from tix_asyncio import TaskCancelled, map_concurrent, map_unordered


def flatten(error):
    if isinstance(error, BaseExceptionGroup):
        return [leaf for child in error.exceptions for leaf in flatten(child)]
    return [error]


@pytest.mark.parametrize("ordered", [False, True])
@pytest.mark.parametrize("count", [1, 5])
@pytest.mark.parametrize("cancellation", ["future", "task"])
def test_independent_asyncio_cancellation_fails_mapping_and_joins_siblings(
    ordered, count, cancellation
):
    async def run():
        active = set()

        async def operation(value):
            active.add(value)
            try:
                if value == 0:
                    if cancellation == "future":
                        future = asyncio.get_running_loop().create_future()
                        future.cancel()
                        await future
                    else:
                        asyncio.current_task().cancel()
                        await asyncio.sleep(0)
                await anyio.sleep_forever()
            finally:
                active.remove(value)

        with anyio.fail_after(1):
            with pytest.raises(ExceptionGroup) as caught:
                if ordered:
                    await map_concurrent(operation, range(count), concurrency=2)
                else:
                    async with map_unordered(operation, range(count), concurrency=2) as results:
                        _ = [value async for value in results]
        assert any(isinstance(error, TaskCancelled) for error in flatten(caught.value))
        assert not active

    asyncio.run(run())


def test_cancelled_async_input_is_not_normal_stream_exhaustion():
    async def run():
        closed = []

        async def source():
            try:
                yield 1
                future = asyncio.get_running_loop().create_future()
                future.cancel()
                await future
            finally:
                closed.append(True)

        async def operation(value):
            return value

        with pytest.raises(ExceptionGroup) as caught:
            async with aclosing(source()) as items:
                async with map_unordered(operation, items) as results:
                    _ = [value async for value in results]
        assert any(isinstance(error, TaskCancelled) for error in flatten(caught.value))
        assert closed == [True]

    asyncio.run(run())


@pytest.mark.parametrize("ordered", [False, True])
def test_cancelled_sync_input_fails_both_mapping_apis(ordered):
    async def run():
        def source():
            yield 1
            raise asyncio.CancelledError("input cancelled")

        async def operation(value):
            return value

        with pytest.raises(ExceptionGroup) as caught:
            if ordered:
                await map_concurrent(operation, source())
            else:
                async with map_unordered(operation, source()) as results:
                    _ = [value async for value in results]
        assert any(isinstance(error, TaskCancelled) for error in flatten(caught.value))

    asyncio.run(run())


@pytest.mark.parametrize("ordered", [False, True])
@pytest.mark.parametrize("method", ["timeout", "wait_for", "cancel"])
@pytest.mark.parametrize("buffer_size", [0, 20])
def test_mapping_preserves_native_asyncio_caller_cancellation(ordered, method, buffer_size):
    async def run():
        started = asyncio.Event()
        cleaned = []
        deadline = None

        async def operation(value):
            try:
                started.set()
                if deadline is not None:
                    deadline.reschedule(asyncio.get_running_loop().time())
                await asyncio.Event().wait()
            finally:
                cleaned.append(value)

        async def mapping():
            if ordered:
                await map_concurrent(operation, range(20), concurrency=1, buffer_size=buffer_size)
            else:
                async with map_unordered(
                    operation, range(20), concurrency=1, buffer_size=buffer_size
                ) as results:
                    _ = [value async for value in results]

        if method == "timeout":
            # Cancel the caller itself only after a worker has started.
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(None) as deadline:
                    await mapping()
            assert asyncio.current_task().cancelling() == 0
        else:
            task = asyncio.create_task(mapping())
            try:
                await asyncio.wait_for(started.wait(), timeout=1)
                await anyio.wait_all_tasks_blocked()
                if method == "wait_for":
                    with pytest.raises(TimeoutError):
                        await asyncio.wait_for(task, timeout=0)
                else:
                    task.cancel("caller cancelled")
                    with pytest.raises(asyncio.CancelledError):
                        await task
                assert task.cancelled()
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        assert cleaned == [0]

    asyncio.run(run())


@pytest.mark.anyio
@pytest.mark.parametrize("ordered", [False, True])
async def test_mapping_preserves_ambient_cancellation_and_joins_workers(ordered):
    started, cleaned = [], []

    async def operation(value):
        started.append(value)
        try:
            await anyio.sleep_forever()
        finally:
            cleaned.append(value)

    with anyio.move_on_after(0.02) as scope:
        if ordered:
            await map_concurrent(operation, range(20), concurrency=2)
        else:
            async with map_unordered(operation, range(20), concurrency=2) as results:
                _ = [value async for value in results]
        pytest.fail("cancelled mapping returned successfully")
    assert scope.cancel_called
    assert started and sorted(started) == sorted(cleaned)


@pytest.mark.anyio
async def test_none_remains_a_valid_ordered_result():
    async def operation(value):
        return None

    assert await map_concurrent(operation, range(3)) == [None, None, None]


@pytest.mark.anyio
@pytest.mark.parametrize("ordered", [False, True])
@pytest.mark.parametrize("mixed", [False, True])
async def test_mapping_never_swallows_nested_cancellation_groups(ordered, mixed):
    with anyio.CancelScope() as scope:
        scope.cancel()
        try:
            await anyio.lowlevel.checkpoint()
        except anyio.get_cancelled_exc_class() as error:
            cancellation = error
    original = BaseExceptionGroup("nested", [cancellation])
    if mixed:
        original = BaseExceptionGroup("mixed", [original, ValueError("operation failed")])

    async def operation(value):
        raise original

    with pytest.raises(BaseExceptionGroup) as caught:
        if ordered:
            await map_concurrent(operation, [1])
        else:
            async with map_unordered(operation, [1]) as results:
                _ = [value async for value in results]
    errors = flatten(caught.value)
    if mixed:
        assert any(isinstance(error, ValueError) for error in errors)
        assert not any(isinstance(error, TaskCancelled) for error in errors)
    else:
        assert len(errors) == 1 and isinstance(errors[0], TaskCancelled)
        assert errors[0].__cause__ is original
