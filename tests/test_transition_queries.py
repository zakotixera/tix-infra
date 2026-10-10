import asyncio

import pytest
from tix_transition import (
    AsyncTransitionManager,
    MachineDefinition,
    State,
    Transition,
    TransitionManager,
)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("condition", ["guards", "conditions", "unless"])
def test_guard_query_and_dispatch_short_circuit_identically(asynchronous, condition):
    seen = []

    def reject(event):
        seen.append("reject")
        return condition == "unless"

    def unreachable(event):
        raise AssertionError("a rejected transition must not evaluate later guards")

    cls = AsyncTransitionManager if asynchronous else TransitionManager
    manager = cls(
        MachineDefinition(
            (State("a"), State("b")),
            (Transition("go", "a", "b", **{condition: (reject, unreachable)}),),
            "a",
        ),
        history_limit=2,
    )

    async def run():
        if asynchronous:
            assert not await manager.may_trigger("go")
        else:
            assert not manager.may_trigger("go")
        assert manager.state == "a" and manager.history == ()
        if asynchronous:
            assert not await manager.trigger("go")
        else:
            assert not manager.trigger("go")
        assert manager.state == "a" and len(manager.history) == 1
        assert seen == ["reject", "reject"]

    asyncio.run(run())


def test_async_query_awaits_guards_and_keeps_ordered_alternatives():
    async def run():
        seen = []

        async def prepare(event):
            await asyncio.sleep(0)
            seen.append("prepare:" + str(event.target))

        async def reject(event):
            await asyncio.sleep(0)
            seen.append("reject:" + event.target)
            return False

        async def accept(event):
            await asyncio.sleep(0)
            seen.append("accept:" + event.target)
            return True

        manager = AsyncTransitionManager(
            MachineDefinition(
                (State("a"), State("b"), State("c")),
                (
                    Transition("go", "a", "b", conditions=(reject,)),
                    Transition("go", "a", "c", conditions=(accept,)),
                ),
                "a",
                prepare_event=(prepare,),
            )
        )
        assert await manager.may_trigger("go")
        assert seen == ["prepare:b", "reject:b", "prepare:c", "accept:c"]
        assert manager.state == "a"
        assert await manager.trigger("go")
        assert manager.state == "c"

    asyncio.run(run())
