# tix-asyncio

Bounded structured concurrency and resilience on AnyIO. Python 3.11+; supports
asyncio and Trio. Objects belong to one event loop and are not thread-safe.

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
AnyIO's task-group rules. `submit(async_callable, *args, name=None)` waits for a permit
**before creating a child task**. Passing callables avoids unawaited coroutines when
admission is cancelled. Capacity counts active operations, and context variables are
inherited from the submitting task.

Normal scope exit drains admitted tasks. An exception cancels siblings and surfaces
AnyIO's exception group. `error_policy="collect"` keeps unrelated operations running;
retrieve failures using handles. The manager retains counters, not a growing list of
finished results or exceptions. Callers retaining every handle still consume O(n)
memory. `statistics` reports submitted, active, succeeded, failed and cancelled work.

`handle.cancel()` cancels one operation; `tasks.cancel()` cancels all currently active
operations without cancelling the scope owner. `await handle.result(timeout=...)`
waits for its result, rethrows its exception, or raises `TaskCancelled` if the owned
operation was cancelled. Cancelling/timing out a result waiter does not cancel the
operation. Cancellation is cooperative; an operation that ignores it can delay exit.

Recursive submission to the same bounded manager is rejected to avoid exhausted
capacity deadlocks. Cross-manager dependency cycles remain the application's
responsibility. Use a nested manager for a separately bounded stage. Use AnyIO's
`TaskGroup.start()` directly when workers need an explicit readiness handshake.

## Batch work

`await map_concurrent(operation, iterable, concurrency=32, buffer_size=0)` uses a
fixed worker count and bounded input stream, accepts lazy iterables, and returns
results in input order. Failure cancels siblings. The result list necessarily uses
O(n) memory; this is not an unbounded streaming-result API. Blocking iterator work and
blocking callbacks must be offloaded by the application.

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
