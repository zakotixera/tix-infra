# tix-transition

Reusable state machine definitions backed by `transitions`. Python 3.11+. The sync
manager uses `Machine`; the async manager uses the upstream asyncio extension.

The full native API is also available: `Machine`, `AsyncMachine`, `HierarchicalMachine`,
`HierarchicalAsyncMachine`, locked/graph variants, `MachineFactory`, `MachineError`,
`EventData`, `NativeState`, `NativeTransition` and `NativeEvent` are upstream classes.
Exported `native` and `extensions` modules provide the complete installed-version API.

```python
from tix_transition import HierarchicalMachine

machine = HierarchicalMachine(
    states=["idle", {"name": "work", "children": ["ready", "done"], "initial": "ready"}],
    initial="idle",
    queued=True,
    transitions=[["begin", "idle", "work"], ["finish", "work_ready", "work_done"]],
)
machine.begin()
machine.finish()
assert machine.state == "work_done"
```

Native classes retain enum states, nested/parallel states, automatic and queued
transitions, generated model methods, string callbacks, multi-model `dispatch`,
`add_model` / `remove_model`, dynamic/ordered transitions, inspection, subclassing,
graph generation and constructor options. Renderers retain upstream optional
dependencies. The typed manager policies below do not alter exported native classes.

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
A transition to the current state or `target="="` exits and re-enters it. Repeated event/source
definitions form ordered guarded alternatives; the first accepted transition wins.

Callbacks receive an `Event` containing its name, original source, context, positional
`args` and keyword `payload` (also `kwargs`). Transition callbacks additionally receive
the selected `target`, current `state` and backend result/error fields. The payload
mapping is copied and read-only; nested values/context remain borrowed application
objects. String callbacks resolve against context attributes or mapping keys, including
dotted paths (`conditions=("policy.allowed",)`). Callables receive the event;
non-callable attributes act as condition values. Managed resolution does not import
arbitrary dotted paths; native machines retain upstream callable resolution.

`conditions` extends `guards`; all must pass. `unless` conditions must be false.
Successful dispatch follows: `prepare_event` -> transition `prepare` -> guards,
conditions and unless -> `before_state_change` -> transition `before` -> exit ->
state update -> enter -> `on_final` (if terminal) -> transition `after` ->
`after_state_change` -> `finalize_event`. Machine hooks are tuples on
`MachineDefinition`. Initial construction/restore do not run entry callbacks.

`on_exception` and `finalize_event` receive completion events with resulting state,
acceptance and the original exception. Managed hooks observe failures without
suppressing them. Ordinary hook failures are aggregated with the dispatch failure,
and history/observers still receive a record. Async hooks honor ambient cancellation
without implicit shielding; cancelled hooks may prevent later hooks from running,
but cancellation is recorded and propagated. Earlier hook failures and observer
failures are preserved alongside cancellation in a `BaseExceptionGroup`; cancellation
alone remains a native `CancelledError`. Native machines retain upstream error
suppression/finalization behavior. Final states reject outgoing transitions.

Definition errors are rejected before backend construction. Structural
`available_events()` does not run guards and therefore does not promise acceptance.
`InvalidEvent` means the event is unknown/unavailable from the current state; it is
rejected before dispatch/history. A valid event rejected by guards returns a record
with `accepted=False`.

`trigger(name, *args, **payload)` returns a boolean like upstream; `dispatch` returns
the richer record. `may_trigger` runs upstream preparation/conditions without changing
state or history and returns false for unavailable events. These callbacks can have
application side effects, so it is not a pure availability query. Guard queries do
not run managed completion hooks.
Managed sync and async guard queries use the same ordered, short-circuit condition
evaluation as dispatch: later conditions are skipped after a rejection.

`states`, `get_state`, `is_state`, `get_triggers(*states)` and
`get_transitions(trigger="*", source="*", dest="*")` expose inspection. Returned
states/transitions are immutable definitions. `dest=None` filters internal transitions.
`add_states(*states)`, `add_transition(Transition(...))` and
`remove_transition(trigger, source="*", dest="*")` validate and rebuild atomically
while retaining current state, context and history. Partial source removal preserves
the other sources; edits affect only that manager. Rebuilds are control-plane
operations. Wildcard definitions include newly added non-final states when rebuilt.

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

Async `trigger`, `may_trigger`, structural edits and `restore` must be awaited.

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
Structural edits do not automatically bump the explicit version; version your schema
before exchanging checkpoints across changed definitions.

`definition.mermaid()` exports a flat state diagram without Graphviz. Managed
definitions describe flat FSMs. Native hierarchical/parallel statecharts and queues
are available through the exported upstream classes; they do not automatically carry
managed history and serialization guarantees.
