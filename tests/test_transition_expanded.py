import asyncio
from enum import Enum
from types import SimpleNamespace

import pytest
import tix_transition as tix
import transitions
from transitions import extensions


def test_native_exports_preserve_upstream_classes():
    assert tix.native is transitions and tix.extensions is extensions
    assert tix.Machine is transitions.Machine
    assert tix.NativeState is transitions.State
    assert tix.NativeTransition is transitions.Transition
    for name in (
        "AsyncMachine",
        "HierarchicalMachine",
        "HierarchicalAsyncMachine",
        "LockedMachine",
        "LockedHierarchicalMachine",
        "GraphMachine",
        "MachineFactory",
    ):
        assert getattr(tix, name) is getattr(extensions, name)
    assert tix.MachineFactory.get_predefined(nested=True) is tix.HierarchicalMachine
    assert issubclass(tix.MachineFactory.get_predefined(graph=True), tix.GraphMachine)


def test_native_enum_multimodel_dynamic_changes_and_ordered_transitions():
    class Status(Enum):
        IDLE = 1
        ACTIVE = 2
        DONE = 3

    first, second = SimpleNamespace(), SimpleNamespace()
    machine = tix.Machine(
        model=[first, second], states=Status, initial=Status.IDLE, auto_transitions=False
    )
    machine.add_transition("start", Status.IDLE, Status.ACTIVE)
    machine.add_transition("finish", Status.ACTIVE, Status.DONE)
    assert first.may_start() and first.start()
    assert first.state is Status.ACTIVE and second.state is Status.IDLE
    assert first.is_ACTIVE()
    assert machine.get_transitions("start")[0].dest == "ACTIVE"
    machine.remove_transition("start")
    assert "start" not in machine.get_triggers(Status.IDLE)
    machine.remove_model(second)
    assert machine.models == [first]
    third = SimpleNamespace()
    machine.add_model(third, initial=Status.ACTIVE)
    assert machine.dispatch("finish")
    assert first.state is third.state is Status.DONE

    ordered = tix.Machine(states=["a", "b", "c"], initial="a", auto_transitions=False)
    ordered.add_ordered_transitions(loop=False)
    ordered.next_state()
    ordered.next_state()
    assert ordered.state == "c"
    ordered.add_states("d")
    ordered.add_transition("extend", "c", "d")
    assert ordered.extend() and ordered.state == "d"


def test_native_queue_callback_strings_and_eventdata():
    phases = []

    class Model:
        def entered(self, event):
            assert isinstance(event, tix.EventData)
            phases.append("entered")
            self.finish()
            phases.append("queued")

    model = Model()
    machine = tix.Machine(
        model=model,
        states=["a", {"name": "b", "on_enter": "entered"}, "c"],
        initial="a",
        queued=True,
        send_event=True,
        transitions=[
            {
                "trigger": "go",
                "source": "a",
                "dest": "b",
                "after": lambda event: phases.append("after"),
            },
            {"trigger": "finish", "source": "b", "dest": "c"},
        ],
    )
    assert model.go()
    assert model.state == "c" and phases == ["entered", "queued", "after"]
    model.to_a()
    assert machine.get_model_state(model).name == "a"


def test_native_hierarchical_parallel_states_and_locked_machine():
    machine = tix.HierarchicalMachine(
        states=[
            "idle",
            {
                "name": "work",
                "parallel": [
                    {"name": "left", "children": ["pending", "done"], "initial": "pending"},
                    {"name": "right", "children": ["pending", "done"], "initial": "pending"},
                ],
            },
        ],
        initial="idle",
        auto_transitions=False,
        transitions=[
            ["begin", "idle", "work"],
            ["left_done", "work_left_pending", "work_left_done"],
            ["right_done", "work_right_pending", "work_right_done"],
        ],
    )
    machine.begin()
    assert machine.state == ["work_left_pending", "work_right_pending"]
    machine.left_done()
    machine.right_done()
    assert machine.state == ["work_left_done", "work_right_done"]
    assert machine.is_work(allow_substates=True)
    locked = tix.LockedMachine(states=["a", "b"], initial="a")
    assert locked.to_b() and locked.state == "b"


def test_native_hierarchical_async_callbacks():
    async def run():
        calls = []

        async def entered():
            await asyncio.sleep(0)
            calls.append(True)

        machine = tix.HierarchicalAsyncMachine(
            states=[
                "idle",
                {
                    "name": "work",
                    "children": ["ready", "done"],
                    "initial": "ready",
                    "on_enter": entered,
                },
            ],
            initial="idle",
            transitions=[["begin", "idle", "work"], ["finish", "work_ready", "work_done"]],
        )
        assert await machine.begin()
        assert await machine.finish()
        assert machine.state == "work_done" and calls == [True]

    asyncio.run(run())


def make_definition(phases):
    def phase(name):
        def callback(event):
            phases.append(name)
            assert event.args == (42,)
            assert event.kwargs["allowed"] is True

        return callback

    return tix.MachineDefinition(
        states=(
            tix.State("a", on_exit=(phase("exit"),)),
            tix.State("b", on_enter=(phase("enter"),), final=True),
        ),
        transitions=(
            tix.Transition(
                "go",
                "a",
                "b",
                conditions=("allowed",),
                unless=("blocked",),
                prepare=(phase("prepare"),),
                before=(phase("before"),),
                after=(phase("after"),),
            ),
        ),
        initial="a",
        prepare_event=(phase("prepare_event"),),
        before_state_change=(phase("before_state_change"),),
        after_state_change=(phase("after_state_change"),),
        on_final=(phase("final"),),
        finalize_event=(phase("finalize"),),
    )


def test_managed_callback_phases_conditions_unless_names_args_and_completion():
    phases = []
    context = SimpleNamespace(allowed=lambda event: event.kwargs["allowed"], blocked=False)
    manager = tix.TransitionManager(make_definition(phases), context=context)
    assert manager.may_trigger("go", 42, allowed=True)
    assert phases == ["prepare_event", "prepare"]
    assert manager.history == () and manager.state == "a"
    phases.clear()
    assert manager.trigger("go", 42, allowed=True)
    assert phases == [
        "prepare_event",
        "prepare",
        "before_state_change",
        "before",
        "exit",
        "enter",
        "final",
        "after",
        "after_state_change",
        "finalize",
    ]
    assert manager.is_final and manager.is_state("b")
    assert manager.get_state("b").final
    assert manager.get_triggers("a") == ("go",)
    assert manager.get_transitions("go", "a", "b") == manager.definition.transitions
    assert not manager.may_trigger("missing")


def test_managed_unless_named_properties_and_reflexive_transition():
    phases, context = [], {"policy": {"blocked": True}}
    definition = tix.MachineDefinition(
        (
            tix.State(
                "a",
                on_enter=(lambda event: phases.append("enter"),),
                on_exit=(lambda event: phases.append("exit"),),
            ),
        ),
        (tix.Transition("again", "*", "=", unless=("policy.blocked",)),),
        "a",
    )
    manager = tix.TransitionManager(definition, context=context)
    assert not manager.may_trigger("again")
    assert not manager.trigger("again")
    context["policy"]["blocked"] = False
    assert manager.trigger("again") and phases == ["exit", "enter"]
    assert manager.get_transitions(dest="a") == definition.transitions
    assert "s0 --> s0: again" in definition.mermaid()


def test_managed_structural_edits_are_atomic_and_isolated():
    definition = tix.MachineDefinition(
        (tix.State("a"), tix.State("b")), (tix.Transition("reset", "*", "a"),), "a"
    )
    first, second = tix.TransitionManager(definition), tix.TransitionManager(definition)
    first.add_states(tix.State("c"))
    first.add_transition(tix.Transition("go", ("a", "b"), "c"))
    assert first.get_triggers("c") == ("reset",)
    assert first.trigger("go") and first.state == "c"
    with pytest.raises(ValueError):
        first.add_transition(tix.Transition("invalid", "c", "absent"))
    assert first.state == "c" and first.get_triggers() == ("reset",)
    assert tuple(second.states) == ("a", "b")
    first.remove_transition("go", source="a")
    assert first.get_transitions("go")[0].source == ("b",)
    assert "go" not in first.get_triggers("a") and "go" in first.get_triggers("b")
    first.remove_transition("go")
    assert first.get_transitions("go") == ()


def test_managed_reflexive_removal_and_duplicate_state_validation():
    manager = tix.TransitionManager(
        tix.MachineDefinition(
            (tix.State("a"), tix.State("b")), (tix.Transition("tick", "*", "="),), "a"
        )
    )
    manager.remove_transition("tick", dest="a")
    assert manager.get_triggers("a") == () and manager.get_triggers("b") == ("tick",)
    with pytest.raises(ValueError):
        manager.add_states(tix.State("a"))
    assert len(manager.states) == 2


def test_completion_hooks_see_failures_and_do_not_swallow_them():
    seen = []

    def fail(event):
        raise ValueError("operation")

    def report(event):
        assert isinstance(event.error, ValueError)
        assert event.state == "b" and event.result is False
        seen.append("error")

    def finalize(event):
        seen.append("final")
        raise RuntimeError("finalizer")

    definition = tix.MachineDefinition(
        (tix.State("a"), tix.State("b")),
        (tix.Transition("go", "a", "b", after=(fail,)),),
        "a",
        on_exception=(report,),
        finalize_event=(finalize,),
    )
    manager = tix.TransitionManager(definition, history_limit=2)
    with pytest.raises(ExceptionGroup) as errors:
        manager.trigger("go")
    assert [type(error) for error in errors.value.exceptions] == [ValueError, RuntimeError]
    assert seen == ["error", "final"] and manager.history[0].target == "b"


def test_completion_event_target_and_state_at_enter():
    observed = []
    definition = tix.MachineDefinition(
        (tix.State("a"), tix.State("b", on_enter=(observed.append,))),
        (tix.Transition("go", "a", "b"),),
        "a",
    )
    tix.TransitionManager(definition).trigger("go", 7)
    (event,) = observed
    assert event.source == "a" and event.target == event.state == "b" and event.args == (7,)


def test_managed_async_parity_and_structural_edits():
    async def run():
        phases = []
        manager = tix.AsyncTransitionManager(
            make_definition(phases), context={"allowed": True, "blocked": False}
        )
        assert await manager.may_trigger("go", 42, allowed=True)
        phases.clear()
        assert await manager.trigger("go", 42, allowed=True)
        assert phases == [
            "prepare_event",
            "prepare",
            "before_state_change",
            "before",
            "exit",
            "enter",
            "final",
            "after",
            "after_state_change",
            "finalize",
        ]
        assert not await manager.may_trigger("unknown")
        await manager.restore(tix.Checkpoint("machine", "1", "a"))
        await manager.add_states(tix.State("c"))
        await manager.add_transition(tix.Transition("extra", "a", "c"))
        await manager.remove_transition("go", dest="b")
        assert manager.get_triggers() == ("extra",)
        assert await manager.trigger("extra", 42, allowed=True)
        assert manager.state == "c"

    asyncio.run(run())


def test_managed_async_error_hooks_await_and_finalizers_report_errors():
    async def run():
        seen = []

        async def fail(event):
            raise ValueError("operation")

        async def report(event):
            await asyncio.sleep(0)
            seen.append(type(event.error).__name__)

        async def finalize(event):
            await asyncio.sleep(0)
            seen.append("final")
            raise RuntimeError("finalizer")

        manager = tix.AsyncTransitionManager(
            tix.MachineDefinition(
                (tix.State("a"),),
                (tix.Transition("go", "a", None, before=(fail,)),),
                "a",
                on_exception=(report,),
                finalize_event=(finalize,),
            ),
            history_limit=1,
        )
        with pytest.raises(ExceptionGroup) as errors:
            await manager.trigger("go")
        assert len(errors.value.exceptions) == 2
        assert seen == ["ValueError", "final"] and len(manager.history) == 1

    asyncio.run(run())


def test_reentrant_mutation_and_guard_checks_rejected():
    manager = None

    def edit(event):
        manager.add_states(tix.State("b"))

    manager = tix.TransitionManager(
        tix.MachineDefinition(
            (tix.State("a"),), (tix.Transition("go", "a", None, conditions=(edit,)),), "a"
        )
    )
    with pytest.raises(RuntimeError, match="reentrant"):
        manager.may_trigger("go")
    assert tuple(manager.states) == ("a",)


def test_cancellation_during_async_finalizer_still_records_committed_state():
    async def run():
        started = asyncio.Event()

        async def finalize(event):
            started.set()
            await asyncio.Event().wait()

        manager = tix.AsyncTransitionManager(
            tix.MachineDefinition(
                (tix.State("a"), tix.State("b")),
                (tix.Transition("go", "a", "b"),),
                "a",
                finalize_event=(finalize,),
            ),
            history_limit=1,
        )
        task = asyncio.create_task(manager.trigger("go"))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        (record,) = manager.history
        assert record.target == "b" and record.error_type == "CancelledError"
        assert record.accepted

    asyncio.run(run())


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("dispatch_fails", [False, True])
def test_interrupted_completion_preserves_all_failures_and_history(asynchronous, dispatch_fails):
    calls = []
    operation_error = ValueError("operation")
    hook_error = RuntimeError("hook")
    interrupted = asyncio.CancelledError("interrupted")
    observer_error = LookupError("observer")

    def operation(event):
        if dispatch_fails:
            raise operation_error

    def failed_hook(event):
        calls.append("failed")
        raise hook_error

    def interrupt(event):
        calls.append("interrupted")
        raise interrupted

    def observe(record):
        calls.append("observed")
        raise observer_error

    cls = tix.AsyncTransitionManager if asynchronous else tix.TransitionManager
    manager = cls(
        tix.MachineDefinition(
            (tix.State("a"), tix.State("b")),
            (tix.Transition("go", "a", "b", after=(operation,)),),
            "a",
            finalize_event=(failed_hook, interrupt, lambda event: calls.append("unreachable")),
        ),
        history_limit=1,
        observers=(observe,),
    )

    async def run():
        with pytest.raises(BaseExceptionGroup) as caught:
            if asynchronous:
                await manager.dispatch("go")
            else:
                manager.dispatch("go")
        expected = ([operation_error] if dispatch_fails else []) + [
            hook_error,
            interrupted,
            observer_error,
        ]
        assert list(caught.value.exceptions) == expected
        assert calls == ["failed", "interrupted", "observed"]
        (record,) = manager.history
        assert record.target == "b" and record.accepted is not dispatch_fails
        assert record.error_type == ("ValueError" if dispatch_fails else "RuntimeError")
        # The dispatch guard and lock must be released even after interruption.
        if asynchronous:
            await manager.restore(tix.Checkpoint("machine", "1", "a"))
        else:
            manager.restore(tix.Checkpoint("machine", "1", "a"))
        assert manager.state == "a"

    asyncio.run(run())


@pytest.mark.parametrize("asynchronous", [False, True])
def test_completion_only_cancellation_propagates_without_grouping(asynchronous):
    cancellation = asyncio.CancelledError("cancelled")

    def cancel(event):
        raise cancellation

    cls = tix.AsyncTransitionManager if asynchronous else tix.TransitionManager
    manager = cls(
        tix.MachineDefinition(
            (tix.State("a"),),
            (tix.Transition("go", "a", None, before=(cancel,)),),
            "a",
            finalize_event=(cancel,),
        ),
        history_limit=1,
    )

    async def run():
        with pytest.raises(asyncio.CancelledError) as caught:
            if asynchronous:
                await manager.dispatch("go")
            else:
                manager.dispatch("go")
        assert caught.value is cancellation
        assert manager.history[0].error_type == "CancelledError"

    asyncio.run(run())
