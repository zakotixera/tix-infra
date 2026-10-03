# Tix infrastructure architecture

## Baseline and goals

The initial repository contained four empty directories: no implementation, public
API, dependencies, or tests existed to preserve. This document is the implementation
plan and contract for the first release. Target Python 3.11+; use a uv workspace,
src layouts, independently buildable distributions, explicit public exports, and
PEP 561 typing markers. No package depends on another Tix package.

Performance means bounded work, predictable memory, and cheap steady-state paths.
It does not mean a claim to outperform Rust or Go. Benchmarks measure local Python
operations; they are not cross-language comparisons.

## Reference research

Primary documentation reviewed for this design:

| Area | Reference | Adopted principle |
| --- | --- | --- |
| Python config | [Pydantic Settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) | Pluggable source ordering, nested environment data, validation adapters |
| Rust config | [config-rs builder](https://docs.rs/config/latest/config/builder/struct.ConfigBuilder.html) | Collect ordered sources before building a consistent configuration |
| Go config | [Viper](https://github.com/spf13/viper) | Files/environment/overrides and explicit precedence; prefer passed instances |
| Python async | [AnyIO task groups](https://anyio.readthedocs.io/en/stable/tasks.html) and [cancellation](https://anyio.readthedocs.io/en/stable/cancellation.html) | Structured ownership, readiness, level cancellation, shielded finalization |
| Rust async | [Tokio TaskTracker](https://docs.rs/tokio-util/latest/tokio_util/task/struct.TaskTracker.html) and [CancellationToken](https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html) | Release completed tasks; cooperative hierarchical stop signals |
| Go async | [errgroup](https://pkg.go.dev/golang.org/x/sync/errgroup) and [singleflight](https://pkg.go.dev/golang.org/x/sync/singleflight) | Bounded admission, failure propagation, duplicate work suppression |
| Python FSM | [transitions](https://github.com/pytransitions/transitions) | Delegate transition execution, guards, internal transitions and entry/exit callbacks |
| Rust FSM | [statig](https://docs.rs/statig/latest/statig/) | Separate machine descriptions, execution context, and state ownership |
| Go FSM | [looplab/fsm](https://pkg.go.dev/github.com/looplab/fsm) | Named events and explicit callback phases |
| Go lifecycle | [Uber Fx lifecycle](https://uber-go.github.io/fx/lifecycle.html) | Ordered acquisition, reverse release and bounded hooks |
| Python lifecycle | [contextlib.AsyncExitStack](https://docs.python.org/3/library/contextlib.html#contextlib.AsyncExitStack) | Explicit resource ownership and cleanup after partial acquisition |
| Rust lifecycle | [Tokio graceful shutdown](https://tokio.rs/tokio/topics/shutdown) | Separate stop notification from waiting for work to finish |

These are design references, not vendored implementations. Avoid broad frameworks,
global registries, hidden threads, and mandatory schema or telemetry dependencies.

## tix-config

Pipeline: source loading -> recursive merge -> optional reference resolution ->
immutable candidate -> validation -> atomic publication -> change notification.

- `ConfigSource` protocol: `name` and `load()`; mapping, callable, JSON/TOML/YAML,
  environment and explicit secret-directory sources implement it. YAML is optional.
- Later sources win; dictionaries merge recursively. Lists replace by default or
  append by policy. A custom merge callable can replace the whole strategy.
- Immutable snapshots recursively freeze mappings and sequences, expose dotted or
  tuple paths, typed decoding, origin lookup, redacted export and leaf diffs.
  Tuple paths address keys containing dots. Values are configuration data, not
  arbitrary mutable Python objects.
- Read snapshots with a short publication lock; captured snapshots need no lock.
  Reloads serialize, build candidates off the read path, and retain the previous
  version on source/resolution/validation failure. No-op reloads retain identity.
- Subscribers run synchronously after publication outside the read lock; all are
  attempted and errors report that publication already happened. Reentrant reload
  is rejected. Unsubscribe is explicit.
- Reference interpolation is opt-in, supports `${path}`, detects cycles and missing
  paths, and preserves the type of a whole-value reference. No shell evaluation.
- JSON persistence uses same-directory temporary files and atomic replace.

## tix-asyncio

- `TaskManager` is a single-use AnyIO async context manager with finite admission
  capacity. `submit` waits for capacity *before* spawning. It creates no task per
  queued item. It drains on normal exit; exceptions cancel siblings.
- Result handles support explicit cancellation and bounded waits. Completed task
  handles leave manager ownership immediately; only callers retain results.
  Fail-fast is default; collect mode exposes failures through handles and counters.
- Expose real AnyIO/asyncio modules and common native APIs without semantic rewrites.
  Shared names select AnyIO; asyncio-specific APIs retain their backend restriction.
  Bounded `start_soon`, readiness `start`, awaitable handles, `wait_idle`, `close` and
  native cancel-scope access extend managed task ownership.
- `map_concurrent` uses a fixed worker pool and a bounded memory stream, preserves
  input order, and cancels siblings on error. Results necessarily take O(n) memory.
- `map_unordered` streams completion-order results from sync or async inputs with
  fixed workers and bounded input/output buffers; consumer context exit cancels and
  joins remaining work. It does not build a growing result list.
  Ordered mapping writes directly into its result list; it avoids the output channel
  needed for streaming. Both use native AnyIO task groups and memory streams.
- `RetryPolicy` provides capped exponential backoff, optional full jitter, explicit
  retry predicates, per-attempt and total timeouts, and injectable sleep/random.
  Cancellation is never retried.
- `CancellationToken` supports downward propagation with weak child ownership.
- `SingleFlight` coalesces in-flight operations by key without caching results.
  A cancelled follower does not cancel the leader; a cancelled leader wakes all
  followers with an explicit failure. Recursive use of the same key is rejected.
- Periodic work uses monotonic fixed-rate scheduling, skips missed ticks, and
  supports cooperative stopping. Exceptions propagate to the owning task group.
- Managed primitives work on AnyIO asyncio and Trio backends and are event-loop-local.
  Thread work uses AnyIO's existing `to_thread` APIs; no competing executor layer.

## tix-transition

- Frozen `State`, `Transition`, and `MachineDefinition` descriptions can instantiate
  many independent managers. Validate names, endpoints, initial state, duplicate
  state names and terminal-state outgoing edges before creating the backend.
- `TransitionManager` uses `transitions.Machine`; `AsyncTransitionManager` uses
  `transitions.extensions.asyncio.AsyncMachine` (asyncio only). Both isolate the
  backend's dynamic model methods from application objects.
- Guards and callbacks receive a typed event with caller context and payload.
  Support ordered alternatives, wildcard sources, internal transitions, state
  entry/exit, per-transition before/after hooks and terminal states.
- Serialize dispatch; reject reentrant callback dispatch rather than deadlocking.
  Structural `available_events` never executes user guards. Callback exceptions
  propagate, and state is whatever phase the backend reached: no fictional rollback
  of external side effects. A record captures both failed and successful attempts.
- Optional bounded history and observers, immutable records, versioned snapshot
  restore, and Mermaid export are independent of business logic. Definitions
  carry explicit version identifiers; restore validates them before changing state.
- Export upstream native Machine variants, module namespaces and MachineFactory for
  hierarchy, parallel regions, enums, queues, generated methods, dynamic modeling,
  multiple models, locking and diagrams without removing upstream options.
  Native exports are direct imports in each package entry point, with no proxy classes
  or extra forwarding modules.
- Typed managers support conditions/unless/prepare, global hooks, reflexive targets,
  context callback names, guard queries, inspection and serialized structural edits.
  Typed definitions describe flat FSMs. Contracts for each layer are documented in
  [upstream compatibility](upstream-compatibility.md).

## tix-lifecycle

- Frozen `Component` definitions provide async start/stop, optional health probes,
  dependencies, and hook deadlines. A component's start result is its owned resource.
  Hooks receive a context for dependency resource lookup and cooperative shutdown.
- Compile a dependency DAG once; reject duplicate/missing dependencies and cycles
  before invoking hooks. Start independent nodes concurrently in deterministic
  topological layers with bounded workers. Stop reverse layers.
- Readiness means the start hook returned. Long-running workers belong in an
  application-owned task group and must implement readiness explicitly.
- On startup failure or cancellation, stop all successfully acquired resources
  under shielded cleanup. A failing start hook owns cleanup of its partial work.
  Attempt every stop even if others fail, and aggregate named errors.
- States: NEW -> STARTING -> RUNNING -> STOPPING -> STOPPED; failures enter FAILED.
  Each manager is single-use. Concurrent callers serialize; nested lifecycle calls
  from hooks are rejected. Repeated stop is harmless.
- Resource contexts are dependency-scoped; application code can inspect resources
  while running. Health probes return per-component status with deadlines.
- Context-manager and ASGI lifespan adapters compose into existing apps. An opt-in
  signal runner waits for SIGINT/SIGTERM or `request_shutdown`; importing the package
  never changes signal handlers. Signal installation remains platform-dependent.
- Shutdown deadlines are cooperative: async cancellation cannot forcibly terminate
  blocking Python/native work or cancellation-resistant hooks. Hook implementations
  must offload blocking I/O and honor cancellation.

## Delivery and verification

1. Create workspace metadata and typed module boundaries.
2. Implement configuration pipeline and atomicity tests.
3. Implement bounded async primitives; test both AnyIO backends, including cancellation.
4. Implement sync/async FSM adapters and callback-phase failure tests.
5. Implement lifecycle graph, ownership, rollback, adapters and deadline tests.
6. Add executable composition example, package usage docs, local benchmarks and CI.
7. Run lint, strict type checks, tests, wheel builds and installed-wheel smoke checks.

Future extensions should be separately justified: OS file watchers, remote config
stores, distributed locks, durable workflows, restart supervision, subprocess trees,
hierarchical statecharts, and metrics exporters. The initial release supplies explicit
protocol/callback seams instead of partially implementing those systems.
