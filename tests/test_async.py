import contextvars
import math

import anyio
import pytest
from tix_asyncio import (
    CancellationToken,
    FlightCancelled,
    RetryPolicy,
    SingleFlight,
    TaskCancelled,
    TaskManager,
    map_concurrent,
    retry,
    run_periodic,
)

pytestmark = pytest.mark.anyio


async def test_task_admission_backpressure_results_and_context():
    release, submitted = anyio.Event(), anyio.Event()
    context = contextvars.ContextVar("request_id", default="unset")
    context.set("request")
    handles = []

    async def work():
        await release.wait()
        return context.get()

    async with TaskManager(concurrency=1) as manager:
        first = await manager.submit(work)

        async def submit_second():
            handles.append(await manager.submit(work))
            submitted.set()

        async with anyio.create_task_group() as group:
            group.start_soon(submit_second)
            await anyio.wait_all_tasks_blocked()
            assert not submitted.is_set()
            assert manager.statistics.active == 1
            release.set()
        assert await first.result() == "request"
        assert await handles[0].result() == "request"
    assert manager.statistics.succeeded == 2
    assert manager.statistics.active == 0
    with pytest.raises(RuntimeError):
        await manager.submit(work)
    with pytest.raises(RuntimeError):
        async with manager:
            pass


async def test_collect_failures_and_explicit_cancel():
    async def fail():
        raise ValueError("broken")

    async with TaskManager(error_policy="collect") as manager:
        broken = await manager.submit(fail)
        sleeping = await manager.submit(anyio.sleep_forever)
        sleeping.cancel()
        with pytest.raises(ValueError, match="broken"):
            await broken.result()
        with pytest.raises(TaskCancelled):
            await sleeping.result()
    assert manager.statistics.failed == manager.statistics.cancelled == 1


async def test_fail_fast_cancels_siblings_and_releases_handles():
    started, cleaned = anyio.Event(), anyio.Event()

    async def sibling():
        try:
            started.set()
            await anyio.sleep_forever()
        finally:
            cleaned.set()

    async def fail():
        await started.wait()
        raise ValueError("broken")

    with pytest.raises(ExceptionGroup):
        async with TaskManager() as manager:
            handle = await manager.submit(sibling)
            await manager.submit(fail)
    assert cleaned.is_set() and handle.done
    assert manager.statistics.active == 0


async def test_task_result_timeout_does_not_cancel_work():
    release = anyio.Event()
    async with TaskManager() as manager:
        handle = await manager.submit(release.wait)
        with pytest.raises(TimeoutError):
            await handle.result(timeout=0.01)
        assert not handle.done
        release.set()
        assert await handle.result() is None


async def test_recursive_submission_rejected_and_bulk_cancel():
    async with TaskManager(concurrency=1, error_policy="collect") as manager:

        async def recursive():
            await manager.submit(anyio.sleep_forever)

        handle = await manager.submit(recursive)
        with pytest.raises(RuntimeError, match="recursive"):
            await handle.result()
        handle = await manager.submit(anyio.sleep_forever)
        manager.cancel()
        with pytest.raises(TaskCancelled):
            await handle.result()


async def test_map_uses_fixed_workers_preserves_order_and_accepts_generator():
    active = peak = 0

    async def work(value):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await anyio.lowlevel.checkpoint()
        active -= 1
        return value * 2

    result = await map_concurrent(work, (i for i in range(200)), concurrency=4, buffer_size=2)
    assert result == [i * 2 for i in range(200)]
    assert peak <= 4 and active == 0
    assert await map_concurrent(work, []) == []


async def test_map_failure_cancels_inflight_and_closes_generator_consumer():
    finalized = []

    async def work(value):
        try:
            if value == 2:
                raise RuntimeError("map failure")
            await anyio.sleep_forever()
        finally:
            finalized.append(value)

    with pytest.raises(ExceptionGroup):
        await map_concurrent(work, range(1000000), concurrency=3)
    assert set(finalized) == {0, 1, 2}


async def test_retry_backoff_injection_and_nonretryable_errors():
    attempts, delays, events = 0, [], []

    async def work():
        nonlocal attempts
        attempts += 1
        if attempts < 4:
            raise OSError("transient")
        return 42

    async def sleep(delay):
        delays.append(delay)

    result = await retry(
        work,
        policy=RetryPolicy(attempts=4, initial_delay=1, max_delay=3),
        random_value=lambda: 0.5,
        sleep=sleep,
        on_retry=events.append,
    )
    assert result == 42 and delays == [0.5, 1, 1.5]
    assert [event.attempt for event in events] == [1, 2, 3]

    async def fail():
        raise ValueError("not transient")

    with pytest.raises(ValueError):
        await retry(fail, sleep=sleep)
    assert len(delays) == 3


async def test_retry_timeouts_and_cancellation_not_retried():
    attempts = 0

    async def wait():
        nonlocal attempts
        attempts += 1
        await anyio.sleep_forever()

    with pytest.raises(TimeoutError):
        await retry(wait, policy=RetryPolicy(attempts=2, attempt_timeout=0.01, initial_delay=0))
    assert attempts == 2
    attempts = 0
    with anyio.move_on_after(0.01) as scope:
        await retry(wait, retry_if=lambda error: True)
    assert scope.cancel_called and attempts == 1
    with pytest.raises(TimeoutError):
        await retry(wait, policy=RetryPolicy(total_timeout=0.01))


async def test_tokens_propagate_only_downward_and_wait_is_reusable():
    parent = CancellationToken()
    first, second = parent.child(), parent.child()
    grandchild = first.child()
    first.cancel()
    assert first.cancelled and grandchild.cancelled
    assert not parent.cancelled and not second.cancelled
    parent.cancel()
    parent.cancel()
    assert parent.child().cancelled and second.cancelled
    await second.wait()


async def test_singleflight_coalesces_and_does_not_cache():
    flight = SingleFlight()
    release = anyio.Event()
    results = []
    calls = 0

    async def operation():
        nonlocal calls
        calls += 1
        await release.wait()
        return calls

    async def call():
        results.append(await flight.do("key", operation))

    async with anyio.create_task_group() as group:
        for _ in range(20):
            group.start_soon(call)
        await anyio.wait_all_tasks_blocked()
        assert flight.active == 1 and calls == 1
        release.set()
    assert results == [1] * 20 and flight.active == 0
    assert await flight.do("key", operation) == 2


async def test_singleflight_leader_cancellation_unblocks_followers():
    flight, started = SingleFlight(), anyio.Event()
    errors = []
    leader_scope = None

    async def operation():
        started.set()
        await anyio.sleep_forever()

    async def leader():
        nonlocal leader_scope
        with anyio.CancelScope() as leader_scope:
            await flight.do("key", operation)

    async def follower():
        try:
            await flight.do("key", operation)
        except FlightCancelled as error:
            errors.append(error)

    async with anyio.create_task_group() as group:
        group.start_soon(leader)
        await started.wait()
        group.start_soon(follower)
        await anyio.wait_all_tasks_blocked()
        leader_scope.cancel()
    assert len(errors) == 1 and flight.active == 0


async def test_singleflight_follower_cancellation_does_not_cancel_leader():
    flight, started, release = SingleFlight(), anyio.Event(), anyio.Event()
    results = []

    async def operation():
        started.set()
        await release.wait()
        return 7

    async def leader():
        results.append(await flight.do("key", operation))

    async with anyio.create_task_group() as group:
        group.start_soon(leader)
        await started.wait()
        with anyio.move_on_after(0.01) as scope:
            await flight.do("key", operation)
        assert scope.cancel_called and flight.active == 1
        release.set()
    assert results == [7]


async def test_singleflight_exception_and_recursive_key():
    flight = SingleFlight()

    async def recursive():
        return await flight.do("key", recursive)

    with pytest.raises(RuntimeError, match="recursive"):
        await flight.do("key", recursive)
    assert flight.active == 0


async def test_periodic_stops_cooperatively_and_can_stop_before_first_tick():
    stop = CancellationToken()
    calls = 0

    async def tick():
        nonlocal calls
        calls += 1
        stop.cancel()

    await run_periodic(tick, 60, stop=stop)
    assert calls == 1
    await run_periodic(tick, 60, stop=stop)
    assert calls == 1
    with anyio.move_on_after(0.01) as scope:
        await run_periodic(tick, 60, immediate=False)
    assert scope.cancel_called and calls == 1


async def test_invalid_options():
    with pytest.raises(ValueError):
        TaskManager(concurrency=0)
    with pytest.raises(ValueError):
        TaskManager(error_policy="unknown")
    with pytest.raises(ValueError):
        await map_concurrent(anyio.sleep, [], concurrency=0)
    for interval in (0, -1, math.nan, math.inf):
        with pytest.raises(ValueError):
            await run_periodic(anyio.sleep_forever, interval)
    for kwargs in (
        {"attempts": 0},
        {"initial_delay": -1},
        {"multiplier": 0},
        {"max_delay": math.inf},
        {"total_timeout": 0},
    ):
        with pytest.raises(ValueError):
            RetryPolicy(**kwargs)
