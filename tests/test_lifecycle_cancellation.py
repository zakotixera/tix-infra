import asyncio

import anyio
import pytest
from tix_lifecycle import Component, Health, LifecycleManager, LifecycleState, OwnedResource

pytestmark = pytest.mark.anyio


async def cancel_independently(backend):
    if backend == "asyncio":
        future = asyncio.get_running_loop().create_future()
        future.cancel()
        await future
    else:
        # Capture a native Trio cancellation, then raise it outside its owning scope.
        with anyio.CancelScope() as scope:
            scope.cancel()
            try:
                await anyio.lowlevel.checkpoint()
            except anyio.get_cancelled_exc_class() as error:
                cancellation = error
        raise cancellation


def flatten(error):
    if isinstance(error, BaseExceptionGroup):
        return [leaf for child in error.exceptions for leaf in flatten(child)]
    return [error]


@pytest.mark.parametrize("concurrency", [1, 2])
async def test_independent_start_cancellation_rolls_back(anyio_backend, concurrency):
    released = []

    async def root(context):
        return "root"

    async def stop(context, value):
        await anyio.lowlevel.checkpoint()
        released.append(value)

    async def cancelled(context):
        await cancel_independently(anyio_backend)

    async def pending(context):
        await anyio.sleep_forever()

    manager = LifecycleManager(
        [
            Component("root", root, stop),
            Component("bad", cancelled, requires=("root",)),
            Component("pending", pending, requires=("root",)),
        ],
        concurrency=concurrency,
    )
    with anyio.fail_after(1):
        with pytest.raises(anyio.get_cancelled_exc_class()):
            await manager.start()
    assert manager.state == LifecycleState.FAILED
    assert released == ["root"]
    await manager.stop()
    assert released == ["root"]


@pytest.mark.parametrize("phase", ["stop", "close", "observer"])
async def test_cancelled_cleanup_completes_all_phases_and_dependencies(anyio_backend, phase):
    released, events = [], []
    cancellation = None
    try:
        await cancel_independently(anyio_backend)
    except anyio.get_cancelled_exc_class() as error:
        cancellation = error

    async def start(context):
        async def close():
            await anyio.lowlevel.checkpoint()
            released.append("close:" + context.component)
            if context.component == "child" and phase == "close":
                raise cancellation

        return OwnedResource(context.component, close)

    async def stop(context, value):
        released.append("stop:" + value)
        if value == "child":
            assert context.get("root") == "root"
            if phase == "stop":
                raise cancellation

    def observe(event):
        if event.phase == "stop":
            events.append(event)
            if event.component == "child" and phase == "observer":
                raise cancellation

    manager = LifecycleManager(
        [Component("root", start, stop), Component("child", start, stop, requires=("root",))],
        observers=[observe],
    )
    await manager.start()
    with pytest.raises(BaseExceptionGroup) as caught:
        await manager.stop()
    assert flatten(caught.value) == [cancellation]
    assert released == ["stop:child", "close:child", "stop:root", "close:root"]
    assert [event.component for event in events] == ["child", "root"]
    assert manager.state == LifecycleState.FAILED
    await manager.stop()
    assert len(released) == 4


async def test_cancelled_probe_never_returns_partial_health(anyio_backend):
    async def start(context):
        pass

    async def health(context, value):
        await cancel_independently(anyio_backend)
        return Health(True)

    async with LifecycleManager([Component("service", start, health=health)]) as manager:
        with pytest.raises(anyio.get_cancelled_exc_class()):
            await manager.health()
        assert manager.state == LifecycleState.RUNNING


async def test_startup_observer_cancellation_rolls_back_acquired_resource(anyio_backend):
    released = []
    try:
        await cancel_independently(anyio_backend)
    except anyio.get_cancelled_exc_class() as error:
        cancellation = error

    async def start(context):
        return "resource"

    async def stop(context, value):
        released.append(value)

    def observe(event):
        if event.phase == "start":
            raise cancellation

    manager = LifecycleManager([Component("service", start, stop)], observers=[observe])
    with pytest.raises(BaseExceptionGroup) as caught:
        await manager.start()
    assert flatten(caught.value) == [cancellation]
    assert manager.state == LifecycleState.FAILED
    assert released == ["resource"]


async def test_start_failure_and_cancelled_rollback_are_both_preserved(anyio_backend):
    released = []

    async def root(context):
        return "root"

    async def bad(context):
        raise ValueError("startup failed")

    async def stop(context, value):
        released.append(value)
        await cancel_independently(anyio_backend)

    manager = LifecycleManager(
        [Component("root", root, stop), Component("bad", bad, requires=("root",))]
    )
    with pytest.raises(BaseExceptionGroup) as caught:
        await manager.start()
    errors = flatten(caught.value)
    assert len(errors) == 2
    assert isinstance(errors[0].__cause__, ValueError)
    assert isinstance(errors[1], anyio.get_cancelled_exc_class())
    assert released == ["root"] and manager.state == LifecycleState.FAILED
