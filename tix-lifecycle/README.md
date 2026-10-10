# tix-lifecycle

Dependency-aware process lifecycle and resource ownership. Python 3.11+, AnyIO;
supports asyncio and Trio. No DI framework or other Tix package is required.

```python
import anyio
from tix_lifecycle import Component, LifecycleManager


async def start_database(context):
    return {"connected": True}


async def stop_database(context, database):
    database["connected"] = False


async def start_api(context):
    return {"database": context.get("database")}


async def main():
    manager = LifecycleManager(
        [
            Component("database", start_database, stop_database),
            Component("api", start_api, requires=("database",)),
        ]
    )
    async with manager:
        assert manager.get("api")["database"]["connected"]


anyio.run(main)
```

## Dependency and ownership contract

Components are immutable definitions with `start(context)`, optional
`stop(context, resource)`, `requires`, optional `health(context, resource)`, and
per-hook deadlines. Hooks are async callables; offload blocking I/O explicitly.
The start result is the component's resource. Contexts expose only explicitly
declared dependencies via `get(name)`, plus `component`, `request_shutdown()`,
`shutdown_requested` and `wait_shutdown()`.

The graph compiles before any hook runs. Unknown/duplicate dependencies, duplicate
names and cycles fail immediately. Startup follows topological layers; independent
components run concurrently up to `concurrency=16`. Layer barriers make ordering
predictable but can delay a dependent behind unrelated slow peers in the same layer.
Use `concurrency=1` when independent hooks must execute sequentially.

Readiness means a start hook returned. Use application-owned task groups and explicit
readiness events for background workers. Do not open an AnyIO task group or cancel
scope in a start hook and exit it in a stop hook: they may run in different tasks.
The [composition example](../examples/service.py) shows tasks nested inside resource
ownership so work completes before its dependencies are released.

Shutdown requests the cooperative stop event and stops reverse dependency layers.
Within a layer, release may run concurrently because those nodes are independent.
Dependencies remain available to dependent stop hooks. Resource access through the
manager is allowed only while RUNNING; hooks use their dependency context instead.

## Failure, cancellation and deadlines

On startup failure, sibling starts are cancelled and every successfully returned
resource is released in reverse layers. A failed/cancelled start hook is responsible
for cleaning up partially acquired resources that it never returned. Rollback runs
under a shield so parent cancellation cannot skip it.
An independently cancelled start hook also aborts startup and triggers rollback;
task-group completion alone is never treated as successful resource acquisition.

Shutdown also shields cleanup and attempts all stop hooks despite ordinary hook
exceptions or deadlines. Failures are named `ComponentError` objects with the original
cause and a `start`, `stop` or `close` phase, aggregated in exception groups.
Application and cleanup failures are both preserved. A failed release is reported
once; the manager drops its reference and does not retry non-idempotent cleanup.
Cleanup also attempts the closer, remaining components and observers when a release
hook independently raises cancellation or another `BaseException`. These failures
retain their native type and are reported in a `BaseExceptionGroup` after cleanup;
ordinary exceptions remain named `ComponentError` objects.

Default deadlines are 30 seconds for start, 10 for stop, and 5 for health. For an
`OwnedResource` with both a stop hook and closer, each receives its own stop deadline.
There is no global shutdown deadline: maximum cooperative shutdown time depends on
graph depth, concurrency and component deadlines. Timeouts cannot forcibly terminate
blocking native/Python work or code that suppresses cancellation.

Managers are single-use and event-loop-local. `start()` while RUNNING is idempotent;
repeated `stop()` is harmless. Concurrent lifecycle calls serialize. Nested calls
from hooks are rejected. State progresses from NEW to STARTING to RUNNING, then
STOPPING to STOPPED; startup or cleanup errors end in FAILED. Construct a new manager
to restart. A shutdown request during startup is visible immediately to its hooks.

## Resource contexts, health and instrumentation

`resource_component(name, factory, requires=...)` adapts an async context-manager
factory. Its yielded value is the resource and its exit runs on release. This adapter
is for resources whose entry/exit can run in different tasks, not task-affine context
managers. Exit receives `(None, None, None)`; lifecycle errors are managed by the
orchestrator rather than supplied to the resource context for suppression.

Alternatively return `OwnedResource(value, close)` from start. Its closer always runs
after the optional stop hook, including when the stop hook fails. The context and
application see the unwrapped value.

`await manager.health()` probes components with bounded parallelism and deadlines.
Hooks return `Health(healthy, detail)`. Missing probes report healthy while running;
probe failures report the exception class without including secret-bearing messages.
Non-running managers report unhealthy statuses. A health call holds the lifecycle
lock, so release cannot destroy resources while their probes run.
An independently cancelled probe cancels the health call rather than returning an
incomplete report.

Synchronous `observers=[callback]` receive `LifecycleEvent(component, phase, duration,
error_type)` for start/stop. Observer failures are surfaced, and a startup observer
failure triggers rollback of the acquired resource. Observers are not detached tasks.

## Framework and process adapters

For Starlette/FastAPI, bind the framework-neutral lifespan adapter:

```python
from functools import partial
from tix_lifecycle import lifespan

app = FastAPI(lifespan=partial(lifespan, manager))
```

The yielded lifespan state contains `lifecycle`. The adapter does not import a web
framework. A manager can serve only one lifespan; create one per application run.

`await serve(manager)` starts, waits for `request_shutdown()`, and releases resources.
`install_signals=True` additionally receives SIGINT/SIGTERM. Signal registration is
opt-in and follows AnyIO's platform/backend/main-thread restrictions. Use cooperative
shutdown on unsupported platforms or when an existing server owns signal handling.
The package never installs handlers at import time and does not manage subprocess
trees, restart supervisors, or operating-system service installation.
