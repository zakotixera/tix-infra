import asyncio

import pytest
from tix_transition import (
    AsyncTransitionManager,
    Checkpoint,
    InvalidEvent,
    MachineDefinition,
    State,
    Transition,
    TransitionManager,
)


def definition(**kwargs):
    return MachineDefinition(
        (State("idle"), State("running"), State("done", final=True)),
        (Transition("start", "idle", "running"), Transition("finish", "running", "done")),
        "idle",
        **kwargs,
    )


def test_definitions_reuse_history_checkpoint_and_terminal_states():
    spec = definition(name="job", version="v2")
    first, second = TransitionManager(spec, history_limit=1), TransitionManager(spec)
    assert first.available_events() == ("start",)
    record = first.dispatch("start", job_id=42)
    assert record.accepted and record.source == "idle" and record.target == "running"
    assert second.state == "idle"
    checkpoint = first.checkpoint()
    first.dispatch("finish")
    assert first.is_final and first.available_events() == ()
    assert len(first.history) == 1 and first.history[0].sequence == 2
    first.restore(checkpoint)
    assert first.state == "running"
    assert second.history == ()
    with pytest.raises(ValueError, match="version"):
        first.restore(Checkpoint("job", "old", "idle"))
    with pytest.raises(ValueError, match="state"):
        first.restore(Checkpoint("job", "v2", "unknown"))
    assert "stateDiagram-v2" in spec.mermaid()
    with pytest.raises(InvalidEvent):
        first.dispatch("start")


def test_callback_phases_guards_and_context():
    seen = []
    context = {"approved": False}

    def callback(phase):
        def record(event):
            assert event.context is context
            assert event.source == "a" and event.payload["id"] == 5
            seen.append(phase)

        return record

    spec = MachineDefinition(
        (State("a", on_exit=(callback("exit"),)), State("b", on_enter=(callback("enter"),))),
        (
            Transition(
                "go",
                "a",
                "b",
                guards=(lambda event: event.context["approved"],),
                before=(callback("before"),),
                after=(callback("after"),),
            ),
        ),
        "a",
    )
    manager = TransitionManager(spec, context=context)
    assert not manager.dispatch("go", id=5).accepted
    assert seen == [] and manager.state == "a"
    context["approved"] = True
    assert manager.dispatch("go", id=5).accepted
    assert seen == ["before", "exit", "enter", "after"]


def test_ordered_alternatives_internal_events_and_wildcard():
    seen = []
    spec = MachineDefinition(
        (
            State("a", on_enter=(lambda event: seen.append("enter"),)),
            State("b"),
            State("done", final=True),
        ),
        (
            Transition("tick", "a", None, after=(lambda event: seen.append("tick"),)),
            Transition("go", "a", "b", guards=(lambda event: False,)),
            Transition("go", "a", "done"),
            Transition("reset", "*", "a"),
        ),
        "a",
    )
    manager = TransitionManager(spec)
    assert manager.dispatch("tick").accepted and seen == ["tick"]
    assert manager.dispatch("go").target == "done"
    assert manager.available_events() == ()


def test_failed_after_callback_reports_committed_state_and_observer_failure():
    def fail(event):
        raise ValueError("callback")

    spec = MachineDefinition(
        (State("a"), State("b")), (Transition("go", "a", "b", after=(fail,)),), "a"
    )
    observed = []
    manager = TransitionManager(spec, history_limit=2, observers=[observed.append])
    with pytest.raises(ValueError):
        manager.dispatch("go")
    assert manager.state == "b"
    assert manager.history[0].error_type == "ValueError"
    assert observed[0].target == "b" and not observed[0].accepted
    manager = TransitionManager(spec, observers=[fail])
    with pytest.raises(ExceptionGroup) as errors:
        manager.dispatch("go")
    assert len(errors.value.exceptions) == 2


def test_reentrant_and_async_callbacks_rejected_in_sync_manager():
    manager = None

    def reentrant(event):
        manager.dispatch("go")

    spec = MachineDefinition(
        (State("a"),), (Transition("go", "a", None, before=(reentrant,)),), "a"
    )
    manager = TransitionManager(spec)
    with pytest.raises(RuntimeError, match="reentrant"):
        manager.dispatch("go")

    async def asynchronous(event):
        return True

    spec = MachineDefinition(
        (State("a"),), (Transition("go", "a", None, guards=(asynchronous,)),), "a"
    )
    with pytest.raises(TypeError, match="AsyncTransitionManager"):
        TransitionManager(spec).dispatch("go")


@pytest.mark.parametrize(
    "states,transitions,initial",
    [
        ((State("a"), State("a")), (), "a"),
        ((State("a"),), (), "missing"),
        ((State("a"),), (Transition("go", "missing", "a"),), "a"),
        ((State("a"),), (Transition("go", "a", "missing"),), "a"),
        ((State("a", final=True),), (Transition("go", "a", "a"),), "a"),
        ((State("a"),), (Transition("", "a", "a"),), "a"),
    ],
)
def test_invalid_definitions(states, transitions, initial):
    with pytest.raises(ValueError):
        MachineDefinition(states, transitions, initial)


def test_async_callbacks_serialization_and_cancellation():
    async def run():
        seen = []

        async def before(event):
            await asyncio.sleep(0)
            seen.append("before")

        async def after(event):
            seen.append("after")

        spec = MachineDefinition(
            (State("a"),), (Transition("tick", "a", None, before=(before,), after=(after,)),), "a"
        )
        manager = AsyncTransitionManager(spec, history_limit=2)
        records = await asyncio.gather(*(manager.dispatch("tick") for _ in range(10)))
        assert seen == ["before", "after"] * 10
        assert [record.sequence for record in records] == list(range(1, 11))
        await manager.restore(manager.checkpoint())

        started = asyncio.Event()

        async def wait(event):
            started.set()
            await asyncio.Event().wait()

        spec = MachineDefinition(
            (State("a"), State("b")), (Transition("go", "a", "b", before=(wait,)),), "a"
        )
        manager = AsyncTransitionManager(spec, history_limit=1)
        task = asyncio.create_task(manager.dispatch("go"))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert manager.state == "a" and manager.history[0].error_type == "CancelledError"

    asyncio.run(run())


def test_async_callback_failure_does_not_leave_siblings_running():
    async def run():
        seen = []

        async def fail(event):
            await asyncio.sleep(0)
            raise ValueError("broken")

        async def must_not_run(event):
            seen.append("unexpected")

        spec = MachineDefinition(
            (State("a"),), (Transition("go", "a", None, before=(fail, must_not_run)),), "a"
        )
        manager = AsyncTransitionManager(spec)
        with pytest.raises(ValueError):
            await manager.dispatch("go")
        await asyncio.sleep(0)
        assert seen == []

    asyncio.run(run())


def test_async_reentrant_dispatch_fails_instead_of_hanging():
    async def run():
        async def recurse(event):
            await manager.dispatch("go")

        spec = MachineDefinition(
            (State("a"),), (Transition("go", "a", None, before=(recurse,)),), "a"
        )
        manager = AsyncTransitionManager(spec)
        with pytest.raises(RuntimeError, match="reentrant"):
            await manager.dispatch("go")

    asyncio.run(run())


def test_async_guards_short_circuit_in_definition_order():
    async def run():
        seen = []

        async def reject(event):
            seen.append("first")
            return False

        async def skipped(event):
            seen.append("second")
            return True

        spec = MachineDefinition(
            (State("a"),), (Transition("go", "a", None, guards=(reject, skipped)),), "a"
        )
        manager = AsyncTransitionManager(spec)
        assert not (await manager.dispatch("go")).accepted
        assert seen == ["first"]

    asyncio.run(run())
