# Upstream API preservation

The managed APIs provide explicit ownership and policies. Native API exports retain
upstream object identity, signatures, subclassing and behavior. Applications can
choose either layer without importing a different implementation of the same primitive.

| Package | Native API | Managed additions |
| --- | --- | --- |
| tix-asyncio | Real `anyio` and `asyncio` modules; explicit common-function/class exports | Bounded task admission, readiness, result handles and bounded streaming |
| tix-transition | Real transitions module, Machine classes, extension classes and factory | Immutable definitions, serialized execution, callbacks, inspection and history |

## Concurrency

Common top-level names (`run`, `sleep`, `Event`, `Lock`, `Semaphore`, cancel scopes,
task-group creation, streams and thread/process helpers) select AnyIO and support its
asyncio and Trio backends. Asyncio-only exports (`create_task`, `gather`, `wait`,
`wait_for`, `as_completed`, `shield`, timeouts, futures, queues and Runner) retain
asyncio semantics. Use `asyncio.TaskGroup`, `asyncio.Lock`, or `asyncio.run` through the
exported stdlib module when that exact API is needed. No runtime guesses switch the
meaning of an ambiguous name.

The complete installed-version surfaces remain available through the real modules;
for example, newer AnyIO APIs are not emulated on older supported versions. Native
objects are not implicitly owned by a Tix TaskManager. A native asyncio task created
with `create_task` must be owned and awaited by the caller as usual.

Managed task enhancements preserve bounded admission. `submit` waits for capacity,
`start_soon` raises `WouldBlock` when full, and `start` follows the upstream readiness
handshake. Streaming results use an explicit async context to cancel outstanding
workers on early consumer exit. No detached workers or unbounded task-per-input lists.

## State machines

Native `Machine`, `AsyncMachine`, hierarchical/locked/graph variants and
`MachineFactory` are the actual upstream classes. This retains enum states, nested
and parallel states, dynamic model/state/transition management, generated trigger
methods, queued and automatic transitions, arbitrary machine options, native
EventData, callbacks, subclassing and diagram support. Optional graph renderers still
have their upstream dependencies.

Typed managers retain explicit serialized single-model behavior. They add upstream
style `trigger`, `may_trigger`, `get_state`, `get_triggers`, `get_transitions` and
`is_state` methods, positional event arguments, named context callbacks, conditions,
unless, prepare, machine-wide hooks and reflexive transitions. Structural edits use
validated definitions; native mutable modeling uses the native classes directly.
The native async extension's cancellation/parallel callback behavior remains unchanged
on exported classes; managed async dispatch retains ordered callbacks and propagates
cancellation. The distinction is explicit rather than silently replacing upstream
semantics everywhere.

## References and verification

Design follows the primary [transitions documentation](https://github.com/pytransitions/transitions),
[AnyIO API reference](https://anyio.readthedocs.io/en/stable/api.html), and
[asyncio API index](https://docs.python.org/3/library/asyncio-api-index.html), checked
against installed source. Compatibility tests exercise native hierarchical, parallel,
queued, multi-model and enum behavior, native async primitives and managed cancellation
on both supported AnyIO backends. Minimum supported dependency versions are tested
separately from the workspace lockfile resolution.
