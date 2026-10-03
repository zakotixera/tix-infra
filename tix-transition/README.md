# tix-transition

Reusable state machine definitions backed by `transitions`. Python 3.11+. The sync
manager uses `Machine`; the async manager uses the upstream asyncio extension.

```python
from tix_transition import MachineDefinition, State, Transition, TransitionManager

definition = MachineDefinition(
    states=(State("draft"), State("approved"), State("sent", final=True)),
    transitions=(
        Transition(
            "approve",
            "draft",
            "approved",
            guards=(lambda event: event.payload.get("authorized", False),),
        ),
        Transition("send", "approved", "sent"),
    ),
    initial="draft",
    name="message",
    version="1",
)
manager = TransitionManager(definition, history_limit=32)
assert manager.dispatch("approve", authorized=True).accepted
manager.dispatch("send")
assert manager.is_final
```

## Definitions and callbacks

Definitions are frozen and reusable across independent manager instances. Callables
themselves can still capture shared mutable state; pass per-instance `context=` for
business state instead. A private backend model prevents dynamic trigger methods
from being injected into application objects.

`State` has `name`, ordered `on_enter` / `on_exit` callbacks and a `final` flag.
`Transition` has an event name, one source or a tuple of sources, a target and ordered
`guards`, `before` and `after` callbacks. `source="*"` expands to every non-final
state. `target=None` is an internal transition: before/after run without exit/entry.
A transition to the current state exits and re-enters it. Repeated event/source
definitions form ordered guarded alternatives; the first accepted transition wins.

Every callback receives an `Event` containing its name, source, application context
and payload. The payload mapping is copied and read-only; nested values and context
are borrowed application objects, not recursively frozen. Guards short-circuit in
definition order. Callback phases are guards -> before -> exit -> state update ->
enter -> after. Initial construction and checkpoint restoration do not run entry
callbacks. Final states reject outgoing transitions.

Definition errors are rejected before backend construction. Structural
`available_events()` does not run guards and therefore does not promise acceptance.
`InvalidEvent` means the event is unknown/unavailable from the current state; it is
rejected before dispatch/history. A valid event rejected by guards returns a record
with `accepted=False`.

## Async and concurrency

```python
from tix_transition import AsyncTransitionManager

manager = AsyncTransitionManager(definition)
record = await manager.dispatch("approve", authorized=True)
checkpoint = manager.checkpoint()
await manager.restore(checkpoint)
```

Async managers accept both sync and async callbacks, execute phases in order, and
propagate cancellation. The adapter overrides upstream parallel callback execution
and cancellation swallowing. It requires an asyncio loop; Trio is not supported by
the upstream backend. Sync managers reject accidentally supplied async callbacks.

Sync dispatch/restore use a reentrant thread lock; async dispatch/restore use an
AnyIO lock. Reentrant calls from a callback or an inherited callback context fail
explicitly to prevent deadlocks. Callbacks may dispatch to a different manager but
must avoid cross-manager wait cycles. Async managers belong to one event loop.
Individual state reads are observational; external readers may see a state update
before post-transition callbacks finish. Share a manager through dispatch and its
returned record when completion matters.

## Diagnostics and persistence boundaries

`TransitionRecord` contains sequence, event, source, resulting state, acceptance,
duration and error class name. History defaults to zero and is bounded by
`history_limit`; records do not retain payloads or exceptions. Synchronous observers
receive the same record after each valid dispatch attempt. All observers are tried;
their failures are aggregated with a dispatch failure if both occur.

Exceptions propagate. A failing before/exit callback leaves the original state; a
failing enter/after callback can leave the destination state committed. `accepted`
means dispatch completed successfully, not that external side effects are atomic.
Cancellation follows the same phase-dependent state semantics. There is no automatic
rollback of application effects.

`checkpoint()` returns a small `Checkpoint(definition, version, state)` value.
`restore()` validates all three fields and changes only state; application context,
side effects, sequence and history are not replayed. Serialize with
`dataclasses.asdict` and construct `Checkpoint(**data)` on load. Definition identity
and version are explicit application contracts: bump versions when compatibility
changes. This is not durable workflow execution or transactional event storage.

`definition.mermaid()` exports a flat state diagram without Graphviz. Typed
hierarchical/parallel statecharts and persistent event queues are future adapters;
the initial API deliberately describes flat finite-state machines.
