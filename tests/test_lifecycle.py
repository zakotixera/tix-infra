import signal
import subprocess
import sys
from contextlib import asynccontextmanager, contextmanager
from functools import partial

import anyio
import pytest
from tix_lifecycle import (
    Component,
    ComponentError,
    Health,
    LifecycleManager,
    LifecycleState,
    OwnedResource,
    compile_graph,
    lifespan,
    resource_component,
    serve,
)

pytestmark = pytest.mark.anyio


def flatten(error):
    if isinstance(error, BaseExceptionGroup):
        return [leaf for child in error.exceptions for leaf in flatten(child)]
    return [error]


async def nothing(context):
    return None


async def test_graph_validation_happens_before_hooks():
    for components in [
        [Component("a", nothing), Component("a", nothing)],
        [Component("a", nothing, requires=("missing",))],
        [Component("a", nothing, requires=("b",)), Component("b", nothing, requires=("a",))],
    ]:
        with pytest.raises(ValueError):
            LifecycleManager(components)
    assert compile_graph([]) == ()
    with pytest.raises(ValueError):
        Component("a", nothing, requires=("x", "x"))
    with pytest.raises(ValueError):
        Component("a", nothing, stop_timeout=0)


async def test_dependency_startup_and_reverse_shutdown():
    seen = []
    resource = object()

    async def database(context):
        seen.append("start:db")
        return resource

    async def api(context):
        assert context.get("db") is resource
        with pytest.raises(KeyError):
            context.get("not-declared")
        seen.append("start:api")
        return "api"

    async def stop(context, value):
        seen.append(f"stop:{context.component}")
        assert context.shutdown_requested
        if context.component == "api":
            assert context.get("db") is resource

    manager = LifecycleManager(
        [
            Component("api", api, stop, requires=("db",)),
            Component("db", database, stop),
        ]
    )
    assert manager.layers == (("db",), ("api",))
    async with manager:
        assert manager.state == LifecycleState.RUNNING
        assert manager.get("db") is resource
        await manager.start()
        assert all(report.healthy for report in (await manager.health()).values())
    assert manager.state == LifecycleState.STOPPED
    assert seen == ["start:db", "start:api", "stop:api", "stop:db"]
    await manager.stop()
    with pytest.raises(RuntimeError):
        await manager.start()
    with pytest.raises(RuntimeError):
        manager.get("db")


async def test_startup_parallelism_is_bounded():
    active = peak = 0

    async def start(context):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await anyio.lowlevel.checkpoint()
        active -= 1

    async with LifecycleManager([Component(str(i), start) for i in range(100)], concurrency=3):
        assert peak == 3
    assert active == 0


async def test_startup_failure_rolls_back_only_acquired_resources():
    seen = []
    good_started = anyio.Event()

    async def root(context):
        seen.append("start:root")
        return "root"

    async def good(context):
        seen.append("start:good")
        good_started.set()
        return "good"

    async def bad(context):
        await good_started.wait()
        raise ValueError("cannot start")

    async def stop(context, value):
        seen.append(f"stop:{context.component}")

    manager = LifecycleManager(
        [
            Component("root", root, stop),
            Component("good", good, stop, requires=("root",)),
            Component("bad", bad, stop, requires=("root",)),
            Component("never", nothing, requires=("bad",)),
        ]
    )
    with pytest.raises(ExceptionGroup) as errors:
        await manager.start()
    assert any(
        isinstance(error, ComponentError) and error.component == "bad"
        for error in flatten(errors.value)
    )
    assert seen == ["start:root", "start:good", "stop:good", "stop:root"]
    assert manager.state == LifecycleState.FAILED
    await manager.stop()


async def test_startup_cancellation_shields_rollback():
    cleaned = anyio.Event()

    async def wait(context):
        await anyio.sleep_forever()

    async def stop(context, value):
        await anyio.lowlevel.checkpoint()
        cleaned.set()

    manager = LifecycleManager(
        [
            Component("root", nothing, stop),
            Component("wait", wait, requires=("root",)),
        ]
    )
    with anyio.move_on_after(0.02) as scope:
        await manager.start()
    assert scope.cancel_called and cleaned.is_set()
    assert manager.state == LifecycleState.FAILED


async def test_stop_attempts_all_hooks_and_times_out_cooperatively():
    seen = []

    async def slow(context, value):
        try:
            await anyio.sleep_forever()
        finally:
            seen.append("slow")

    async def broken(context, value):
        seen.append("broken")
        raise ValueError("stop failed")

    async def good(context, value):
        seen.append("root")

    manager = LifecycleManager(
        [
            Component("root", nothing, good),
            Component("slow", nothing, slow, requires=("root",), stop_timeout=0.01),
            Component("broken", nothing, broken, requires=("root",)),
        ]
    )
    await manager.start()
    with pytest.raises(ExceptionGroup) as errors:
        await manager.stop()
    leaves = flatten(errors.value)
    assert len(leaves) == 2
    assert any(isinstance(error.__cause__, TimeoutError) for error in leaves)
    assert set(seen[:2]) == {"slow", "broken"} and seen[-1] == "root"
    assert manager.state == LifecycleState.FAILED


async def test_context_exit_cleans_up_under_cancellation_and_preserves_errors():
    cleaned = []

    async def stop(context, value):
        await anyio.lowlevel.checkpoint()
        cleaned.append(context.component)

    manager = LifecycleManager([Component("service", nothing, stop)])
    with anyio.move_on_after(0.01):
        async with manager:
            await anyio.sleep_forever()
    assert cleaned == ["service"] and manager.state == LifecycleState.STOPPED

    async def broken(context, value):
        raise ValueError("cleanup")

    with pytest.raises(ExceptionGroup) as errors:
        async with LifecycleManager([Component("service", nothing, broken)]):
            raise RuntimeError("application")
    assert {type(error) for error in flatten(errors.value)} == {RuntimeError, ComponentError}


async def test_startup_and_rollback_errors_are_both_preserved():
    async def broken_start(context):
        raise ValueError("startup")

    async def broken_stop(context, value):
        raise RuntimeError("cleanup")

    manager = LifecycleManager(
        [
            Component("root", nothing, broken_stop),
            Component("bad", broken_start, requires=("root",)),
        ]
    )
    with pytest.raises(ExceptionGroup) as errors:
        await manager.start()
    assert {error.phase for error in flatten(errors.value)} == {"start", "stop"}


async def test_resource_context_is_closed_and_unwrapped():
    closed = []

    @asynccontextmanager
    async def resource(context):
        try:
            yield {"value": 42}
        finally:
            await anyio.lowlevel.checkpoint()
            closed.append(True)

    manager = LifecycleManager([resource_component("resource", resource)])
    async with manager:
        assert manager.get("resource") == {"value": 42}
    assert closed == [True]


async def test_owned_resource_closes_even_when_stop_hook_fails():
    closed = []

    async def close():
        closed.append(True)

    async def start(context):
        return OwnedResource(42, close)

    async def stop(context, value):
        assert value == 42
        raise ValueError("stop hook")

    manager = LifecycleManager([Component("owned", start, stop)])
    await manager.start()
    with pytest.raises(ExceptionGroup):
        await manager.stop()
    assert closed == [True]


async def test_health_timeout_failure_and_stopped_state():
    async def healthy(context, resource):
        return Health(True, "ready")

    async def slow(context, resource):
        await anyio.sleep_forever()

    manager = LifecycleManager(
        [
            Component("healthy", nothing, health=healthy),
            Component("slow", nothing, health=slow, health_timeout=0.01),
        ]
    )
    assert not (await manager.health())["healthy"].healthy
    async with manager:
        reports = await manager.health()
        assert reports["healthy"] == Health(True, "ready")
        assert reports["slow"] == Health(False, "TimeoutError")


async def test_reentrant_hooks_rejected_and_observer_failure_rolls_back():
    async def recurse(context):
        await manager.stop()

    manager = LifecycleManager([Component("recursive", recurse)])
    with pytest.raises(ExceptionGroup) as errors:
        await manager.start()
    assert "own hooks" in str(flatten(errors.value)[0].__cause__)

    cleaned, events = [], []

    async def stop(context, value):
        cleaned.append(True)

    def observer(event):
        events.append(event.phase)
        if event.phase == "start":
            raise ValueError("observer")

    manager = LifecycleManager([Component("service", nothing, stop)], observers=[observer])
    with pytest.raises(ExceptionGroup):
        await manager.start()
    assert cleaned == [True] and events == ["start", "stop"]


async def test_concurrent_start_stop_serializes_hooks():
    starts, stops = [], []

    async def start(context):
        await anyio.lowlevel.checkpoint()
        starts.append(True)

    async def stop(context, value):
        await anyio.lowlevel.checkpoint()
        stops.append(True)

    manager = LifecycleManager([Component("service", start, stop)])
    async with anyio.create_task_group() as group:
        for _ in range(10):
            group.start_soon(manager.start)
    async with anyio.create_task_group() as group:
        for _ in range(10):
            group.start_soon(manager.stop)
    assert starts == stops == [True]


async def test_lifespan_and_cooperative_serve():
    manager = LifecycleManager([Component("service", nothing)])
    async with lifespan(manager) as state:
        assert state == {"lifecycle": manager} and manager.state == LifecycleState.RUNNING
    assert manager.state == LifecycleState.STOPPED

    async def start(context):
        context.request_shutdown()
        await context.wait_shutdown()

    manager = LifecycleManager([Component("service", start)])
    await serve(manager)
    assert manager.state == LifecycleState.STOPPED


async def test_empty_manager_and_stop_before_start():
    async with LifecycleManager([]) as manager:
        assert await manager.health() == {}
    manager = LifecycleManager([])
    await manager.stop()
    assert manager.state == LifecycleState.STOPPED
    with pytest.raises(RuntimeError):
        async with manager:
            pass


async def test_stop_notifies_startup_before_waiting_for_lock():
    entered = anyio.Event()

    async def start(context):
        entered.set()
        await context.wait_shutdown()

    manager = LifecycleManager([Component("waiting", start)])
    with anyio.fail_after(2):
        async with anyio.create_task_group() as group:
            group.start_soon(manager.start)
            await entered.wait()
            await manager.stop()
    assert manager.state == LifecycleState.STOPPED


async def test_start_and_observer_failures_both_reported():
    async def start(context):
        raise ValueError("start")

    def observer(event):
        raise RuntimeError("observer")

    with pytest.raises(ExceptionGroup) as errors:
        await LifecycleManager([Component("bad", start)], observers=[observer]).start()
    assert {type(error) for error in flatten(errors.value)} == {ComponentError, RuntimeError}


async def test_signal_adapter_requests_shutdown_and_closes_receiver(monkeypatch):
    send, receive = anyio.create_memory_object_stream(1)
    started, closed = anyio.Event(), []

    @contextmanager
    def signals(*numbers):
        assert numbers == (signal.SIGINT, signal.SIGTERM)
        try:
            yield receive
        finally:
            closed.append(True)

    monkeypatch.setattr(anyio, "open_signal_receiver", signals)

    async def start(context):
        started.set()

    manager = LifecycleManager([Component("service", start)])
    async with send, receive, anyio.create_task_group() as group:
        group.start_soon(partial(serve, manager, install_signals=True))
        await started.wait()
        await send.send(signal.SIGTERM)
    assert manager.state == LifecycleState.STOPPED and closed == [True]


@pytest.mark.skipif(
    sys.platform == "win32", reason="POSIX signal delivery; Windows uses stop event"
)
def test_real_sigterm_releases_resources():
    code = """
import anyio
from functools import partial
from tix_lifecycle import Component, LifecycleManager, serve

async def start(context):
    print("READY", flush=True)

async def stop(context, resource):
    print("CLEANED", flush=True)

manager = LifecycleManager([Component("service", start, stop)])
anyio.run(partial(serve, manager, install_signals=True))
"""
    process = subprocess.Popen(
        [sys.executable, "-u", "-c", code],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout.readline().strip() == "READY"
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=10)
        assert process.returncode == 0, stderr
        assert "CLEANED" in stdout
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)
