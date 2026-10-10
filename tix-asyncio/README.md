# tix-asyncio

Bounded structured concurrency and resilience on AnyIO. Python 3.11+; supports
asyncio and Trio. Objects belong to one event loop and are not thread-safe.

Native APIs are also preserved. Shared top-level names (`run`, `sleep`, locks, events,
cancel scopes, streams, files, thread/process helpers and `create_task_group`) select
AnyIO. Asyncio-specific `create_task`, `gather`, `wait`, `as_completed`, `wait_for`,
`shield`, `timeout`, queues, futures and `Runner` are actual stdlib objects and require
an asyncio loop. The complete installed-version APIs remain available as real modules:

```python
from tix_asyncio import anyio, asyncio, create_task_group, to_thread
```

Use `asyncio.TaskGroup`, `asyncio.Lock` or `asyncio.run` when those exact APIs are
required. Native tasks are not implicitly owned by a Tix manager. See the
[upstream compatibility guide](../docs/upstream-compatibility.md) for the policy.

```python
import anyio
from tix_asyncio import TaskManager


async def square(value: int) -> int:
    await anyio.lowlevel.checkpoint()
    return value * value


async def main():
    async with TaskManager(concurrency=8) as tasks:
        handles = [await tasks.submit(square, value) for value in range(100)]
    print([await handle.result() for handle in handles])


anyio.run(main)
```

## Ownership and backpressure

`TaskManager` is single-use and must be entered/exited in the same task, following
AnyIO's task-group rules. `submit(async_callable, *args, name=None, **kwargs)` waits for a permit
**before creating a child task**. Passing callables avoids unawaited coroutines when
admission is cancelled. Capacity counts active operations, and context variables are
inherited from the submitting task.

Normal scope exit drains admitted tasks. An exception cancels siblings and surfaces
AnyIO's exception group. `error_policy="collect"` keeps unrelated operations running;
retrieve failures using handles. The manager retains counters, not a growing list of
finished results or exceptions. Callers retaining every handle still consume O(n)
memory. `statistics` reports submitted, active, succeeded, failed and cancelled work.

`start_soon(async_callable, *args, name=None, **kwargs)` submits synchronously and
returns a handle. It raises AnyIO's `WouldBlock` if capacity is full. This differs
from an unbounded native task group; use `await submit(...)` for backpressure.
`name` is reserved for task naming; use `functools.partial` to bind an operation's
own `name` argument. `close()` stops admission while existing tasks drain.
`await wait_idle()` waits for zero admitted tasks without closing admission. A task
cannot wait for its own manager to become idle. `concurrency` exposes the capacity.

`handle.cancel()` cancels one operation; `tasks.cancel()` cancels all currently active
operations without cancelling the scope owner. `await handle.result(timeout=...)`
waits for its result, rethrows its exception, or raises `TaskCancelled` if the owned
operation was cancelled. Cancelling/timing out a result waiter does not cancel the
operation. Cancellation is cooperative; an operation that ignores it can delay exit.

Handles are directly awaitable (`value = await handle`). `await handle.wait()` waits
without propagating the operation's error. `result_nowait()` and `exception_nowait()`
raise `WouldBlock` before completion; both report cancellation through `TaskCancelled`.
The `cancelled` property distinguishes cancellation from ordinary failure. Access the
native group `cancel_scope` for AnyIO behavior that also cancels the owning task;
`tasks.cancel()` only targets admitted children.

Recursive submission to the same bounded manager is rejected to avoid exhausted
capacity deadlocks. Cross-manager dependency cycles remain the application's
responsibility. Use a nested manager for a separately bounded stage.

## Readiness handshake

`await tasks.start(operation, *args, **kwargs)` waits for the child to call
`task_status.started(value)` and returns that value, retaining AnyIO's handshake
semantics with bounded admission. A missing readiness call raises `RuntimeError`;
failures before readiness go to the starter. Cancelling the starter before readiness
cancels and joins that child. After readiness, the configured error policy applies.

```python
async def worker(stop, *, task_status):
    task_status.started("ready")
    await stop.wait()


async def serve_worker():
    stop = anyio.Event()
    async with TaskManager(concurrency=4) as tasks:
        assert await tasks.start(worker, stop) == "ready"
        stop.set()
        await tasks.wait_idle()
```

`start` returns the readiness value rather than a result handle, matching upstream.
Collect-mode failures after readiness are counted in statistics; use an application
result channel or `submit` to consume exceptions/results separately.

## Batch work

`await map_concurrent(operation, iterable, concurrency=32, buffer_size=0)` uses a
fixed worker count and bounded input stream, accepts lazy iterables, and returns
results in input order. Failure cancels siblings. The result list necessarily uses
O(n) memory; this is not an unbounded streaming-result API. Blocking iterator work and
blocking callbacks must be offloaded by the application.
Input iteration runs in one owned producer task alongside the fixed worker pool.

`map_unordered(operation, items, concurrency=32, buffer_size=0)` supplies a bounded
completion-order result stream in an async context manager. Inputs may be synchronous
or asynchronous iterables. Both input and output buffers are bounded independently;
memory is O(concurrency + buffer_size), excluding resources retained by your iterator,
operation or consumer. Exiting early cancels and joins workers and the producer.

Both mapping APIs treat independently cancelled operations or input iterators as
`TaskCancelled` failures, reported through the task group's exception group. For
example, awaiting an already-cancelled asyncio Future must not silently drop an
input or return an unfinished result. Cancellation of the mapping's caller retains
native cancellation semantics; early streaming exit still cancels and joins work.
This includes native asyncio `timeout`, `wait_for` and direct caller `Task.cancel()`.

```python
from tix_asyncio import map_unordered


async def consume():
    async with map_unordered(square, range(1_000_000), concurrency=8) as results:
        async for value in results:
            if value > 100:
                break
```

Enter/exit the context in the same task, as required by the owning task group. Input
async generators belong to the caller; close them explicitly if they own resources.
Exceptions use AnyIO's task-group propagation. Results are not retained by the manager.

## Retry

```python
from tix_asyncio import RetryPolicy, retry

result = await retry(
    fetch,
    policy=RetryPolicy(
        attempts=4, initial_delay=0.1, max_delay=2, attempt_timeout=5, total_timeout=12
    ),
    retry_if=lambda error: isinstance(error, OSError),
)
```

Policies are immutable. Delays grow exponentially to a cap; full jitter samples from
zero to the capped delay unless `jitter=False`. Attempt counts include the initial
call. The default predicate retries only `OSError` and `TimeoutError`, never native
cancellation. Exhaustion raises the last exception. Total deadlines include attempts
and delays. Supply `sleep`, `random_value`, and a synchronous `on_retry(RetryEvent)`
for deterministic testing and instrumentation. Observer errors propagate.

Only retry operations whose repeated side effects are acceptable or idempotent.

## Coalescing, stopping and periodic work

- `SingleFlight[T].do(key, async_callable)` shares an in-flight result/exception
  across callers. It does not cache completed results. A cancelled follower leaves
  the leader running; a cancelled leader wakes followers with `FlightCancelled`.
  The first caller owns the operation, and recursive same-key calls are rejected.
  Keys are application-defined; include every input that changes the result.
- `CancellationToken.child()` receives cancellation from its ancestors. Cancelling
  a child does not cancel parents or siblings. `cancelled` and `await wait()` provide
  cooperative notification; they do not forcibly interrupt work. Children are weakly
  retained by parents.
- `run_periodic(operation, interval, stop=token, immediate=True)` runs non-overlapping
  work on a monotonic fixed-rate schedule. Missed ticks are skipped instead of
  building a backlog. Stop requests wake waits immediately but allow the current
  operation to finish. Exceptions propagate; put the runner in an owned task group.

Use AnyIO's `to_thread.run_sync`, streams, locks, cancel scopes and capacity limiters
directly when those primitives are sufficient. This package adds policies and
ownership rather than another executor, event loop or synchronization framework.
