import asyncio
import importlib

import anyio
import pytest
import tix_asyncio as tix


def test_native_exports_preserve_identity_and_versions():
    assert tix.anyio is anyio and tix.asyncio is asyncio
    for name in (
        "run",
        "sleep",
        "Lock",
        "Event",
        "Semaphore",
        "CancelScope",
        "CapacityLimiter",
        "create_task_group",
        "create_memory_object_stream",
        "to_thread",
        "from_thread",
        "to_process",
        "connect_tcp",
        "open_file",
        "run_process",
        "lowlevel",
    ):
        assert getattr(tix, name) is getattr(anyio, name)
    for name in (
        "gather",
        "create_task",
        "wait_for",
        "wait",
        "as_completed",
        "Runner",
        "Queue",
        "Future",
        "Task",
        "timeout",
        "shield",
        "CancelledError",
    ):
        assert getattr(tix, name) is getattr(asyncio, name)
    assert importlib.import_module("asyncio") is asyncio


def test_native_asyncio_tasks_queues_timeouts_and_taskgroups():
    async def run():
        queue = tix.Queue(maxsize=1)

        async def producer():
            await queue.put(42)
            return "done"

        task = tix.create_task(producer(), name="native")
        async with tix.timeout(2):
            assert await queue.get() == 42
            assert await tix.gather(task) == ["done"]
        async with tix.asyncio.TaskGroup() as group:
            child = group.create_task(asyncio.sleep(0, result=7))
        assert child.result() == 7
        assert await tix.wait_for(asyncio.sleep(0, result=3), 2) == 3

    with tix.Runner() as runner:
        runner.run(run())


@pytest.mark.anyio
async def test_native_anyio_streams_threads_and_synchronization():
    send, receive = tix.create_memory_object_stream[int](1)
    lock, event = tix.Lock(), tix.Event()

    async def producer():
        async with lock, send:
            await send.send(await tix.to_thread.run_sync(lambda: 42))
            event.set()

    async with tix.create_task_group() as group:
        group.start_soon(producer)
        async with receive:
            assert await receive.receive() == 42
        await event.wait()


@pytest.mark.anyio
async def test_nowait_admission_handles_keywords_and_capacity_reuse():
    release = anyio.Event()

    async def operation(value, *, factor):
        await release.wait()
        return value * factor

    async with tix.TaskManager(concurrency=1) as manager:
        handle = manager.start_soon(operation, 3, factor=4, name="named")
        assert handle.name == "named" and manager.concurrency == 1
        with pytest.raises(anyio.WouldBlock):
            manager.start_soon(operation, 9, factor=9)
        with pytest.raises(anyio.WouldBlock):
            handle.result_nowait()
        with pytest.raises(anyio.WouldBlock):
            handle.exception_nowait()
        release.set()
        assert await handle == 12
        assert handle.done and handle.result_nowait() == 12
        assert handle.exception_nowait() is None
        second = await manager.submit(operation, 2, factor=5)
        await manager.wait_idle()
        assert second.result_nowait() == 10
        manager.close()
        with pytest.raises(RuntimeError, match="accepting"):
            manager.start_soon(operation, 1, factor=1)
    assert manager.statistics.submitted == 2 and manager.statistics.active == 0


@pytest.mark.anyio
async def test_wait_does_not_raise_but_result_and_exception_reflect_failure():
    async def broken():
        raise ValueError("operation")

    async with tix.TaskManager(error_policy="collect") as manager:
        failed = manager.start_soon(broken)
        await failed.wait()
        assert isinstance(failed.exception_nowait(), ValueError)
        with pytest.raises(ValueError):
            await failed
        cancelled = manager.start_soon(anyio.sleep_forever)
        await anyio.wait_all_tasks_blocked()
        cancelled.cancel()
        await cancelled.wait()
        assert cancelled.cancelled
        with pytest.raises(tix.TaskCancelled):
            cancelled.exception_nowait()


@pytest.mark.anyio
async def test_readiness_returns_value_and_drain_owns_worker():
    release, closed = anyio.Event(), []

    async def service(value, *, multiplier, task_status):
        task_status.started(value * multiplier)
        try:
            await release.wait()
        finally:
            closed.append(True)

    async with tix.TaskManager(concurrency=1) as manager:
        assert await manager.start(service, 3, multiplier=7) == 21
        assert manager.statistics.active == 1
        release.set()
        await manager.wait_idle()
        assert closed == [True] and manager.statistics.succeeded == 1


@pytest.mark.anyio
async def test_readiness_errors_before_started_go_to_caller_and_reuse_capacity():
    async def fail(*, task_status):
        raise ValueError("before readiness")

    async def missing(*, task_status):
        return 42

    async def good(*, task_status):
        task_status.started("ok")

    async with tix.TaskManager(concurrency=1, error_policy="collect") as manager:
        with pytest.raises(ValueError, match="readiness"):
            await manager.start(fail)
        with pytest.raises(RuntimeError, match="started"):
            await manager.start(missing)
        assert await manager.start(good) == "ok"
        await manager.wait_idle()
        assert manager.statistics.failed == 2 and manager.statistics.active == 0


@pytest.mark.anyio
async def test_cancelled_readiness_cleans_up_and_releases_permit():
    cleaned = []

    async def pending(*, task_status):
        try:
            await anyio.sleep_forever()
        finally:
            cleaned.append(True)

    async with tix.TaskManager(concurrency=1) as manager:
        with anyio.move_on_after(0.01) as scope:
            await manager.start(pending)
        assert scope.cancel_called and cleaned == [True]
        assert manager.statistics.active == 0
        handle = await manager.submit(anyio.sleep, 0)
        await handle


@pytest.mark.anyio
async def test_failure_after_readiness_follows_collect_policy_and_duplicate_started_fails():
    async def fail(*, task_status):
        task_status.started("ready")
        await anyio.lowlevel.checkpoint()
        raise ValueError("after readiness")

    async def duplicate(*, task_status):
        task_status.started(1)
        task_status.started(2)

    async with tix.TaskManager(error_policy="collect") as manager:
        assert await manager.start(fail) == "ready"
        assert await manager.start(duplicate) == 1
        await manager.wait_idle()
        assert manager.statistics.failed == 2


@pytest.mark.anyio
async def test_native_group_cancel_scope_and_wait_idle_recursion():
    manager = tix.TaskManager(error_policy="collect")
    with pytest.raises(RuntimeError):
        _ = manager.cancel_scope
    async with manager:
        handle = await manager.submit(manager.wait_idle)
        with pytest.raises(RuntimeError, match="own manager"):
            await handle
        manager.cancel_scope.cancel()
        await anyio.lowlevel.checkpoint()
    with pytest.raises(RuntimeError):
        _ = manager.cancel_scope


@pytest.mark.anyio
async def test_streaming_completion_order_and_async_input():
    release = anyio.Event()

    async def source():
        for value in range(3):
            yield value

    async def work(value):
        if value == 0:
            await release.wait()
        return value

    async with tix.map_unordered(work, source(), concurrency=3) as results:
        first = await anext(results)
        assert first in (1, 2)
        release.set()
        rest = [value async for value in results]
    assert sorted([first, *rest]) == [0, 1, 2]


@pytest.mark.anyio
async def test_streaming_early_exit_bounds_consumption_and_cleans_workers():
    consumed, active, closed = [], [], []

    def source():
        for value in range(1000000):
            consumed.append(value)
            yield value

    async def work(value):
        active.append(value)
        try:
            if value:
                await anyio.sleep_forever()
            return value
        finally:
            closed.append(value)

    async with tix.map_unordered(work, source(), concurrency=3, buffer_size=1) as results:
        assert await anext(results) == 0
        await anyio.wait_all_tasks_blocked()
        assert len(consumed) <= 6
    assert sorted(active) == sorted(closed)


@pytest.mark.anyio
async def test_streaming_failures_and_empty_input():
    async def fail(value):
        raise ValueError("stream failure")

    with pytest.raises(ExceptionGroup):
        async with tix.map_unordered(fail, range(100)) as results:
            async for _ in results:
                pass
    async with tix.map_unordered(fail, []) as results:
        assert [value async for value in results] == []
    with pytest.raises(ValueError):
        async with tix.map_unordered(fail, [], concurrency=0):
            pass


@pytest.mark.anyio
async def test_close_rejects_a_submitter_already_waiting_for_capacity():
    release, rejected = anyio.Event(), []

    async with tix.TaskManager(concurrency=1) as manager:
        await manager.submit(release.wait)

        async def submit():
            try:
                await manager.submit(anyio.sleep, 0)
            except RuntimeError as error:
                rejected.append(str(error))

        async with anyio.create_task_group() as group:
            group.start_soon(submit)
            await anyio.wait_all_tasks_blocked()
            manager.close()
            release.set()
        assert rejected == ["task manager closed while waiting for capacity"]
    assert manager.statistics.submitted == 1 and manager.statistics.active == 0


@pytest.mark.anyio
async def test_readiness_fail_fast_cancels_siblings_after_started():
    started = anyio.Event()
    cleaned = []

    async def sibling():
        try:
            started.set()
            await anyio.sleep_forever()
        finally:
            cleaned.append(True)

    async def service(*, task_status):
        await started.wait()
        task_status.started()
        await anyio.lowlevel.checkpoint()
        raise ValueError("after readiness")

    with pytest.raises(ExceptionGroup):
        async with tix.TaskManager() as manager:
            await manager.submit(sibling)
            await manager.start(service)
    assert cleaned == [True] and manager.statistics.active == 0
