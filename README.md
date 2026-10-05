# Tix infrastructure

**Configuration, bounded concurrency, state machines, and resource lifecycles for Python.**

English | [简体中文](README.zh_CN.md)

Tix infrastructure is a workspace of four independent, typed Python 3.11+ packages.
Use one on its own, or compose them in your application. Each package keeps runtime
state local to its instances and has no dependencies on the other Tix packages.
All four include `py.typed` markers for type checking.

| Package | Import | What it provides | Runtime dependencies |
| --- | --- | --- | --- |
| [tix-config](tix-config/README.md) | `tix_config` | Layered configuration, immutable snapshots, validation, and atomic reload | None; optional PyYAML |
| [tix-asyncio](tix-asyncio/README.md) | `tix_asyncio` | Bounded tasks, worker pools, retries, duplicate-work suppression, and cancellation | AnyIO |
| [tix-transition](tix-transition/README.md) | `tix_transition` | Reusable state machine definitions, sync/async execution, and checkpoints | transitions, AnyIO |
| [tix-lifecycle](tix-lifecycle/README.md) | `tix_lifecycle` | Dependency-ordered startup, rollback, resource release, and health checks | AnyIO |

## Contents

- [Installation](#installation)
- [Quick start](#quick-start)
- [Execution and ownership](#execution-and-ownership)
- [Examples](#examples)
- [Development](#development)
- [Documentation](#documentation)
- [License](#license)

## Installation

From the repository root, install the packages you need in your application's
virtual environment:

```sh
python -m pip install ./tix-config
python -m pip install ./tix-asyncio
python -m pip install ./tix-transition
python -m pip install ./tix-lifecycle
```

Each command installs one independent distribution and its runtime dependencies.
Distribution names use hyphens; Python imports use underscores. For YAML
configuration support, install the optional extra:

```sh
python -m pip install "./tix-config[yaml]"
```

A built wheel can also be installed with `python -m pip install path/to/package.whl`.
These instructions use local sources. The repository root is a development
workspace, not an installable umbrella package. For workspace development, use
[the uv setup below](#development).

## Quick start

Each example runs independently after installing its corresponding package.

### Layered configuration

Combine defaults with application overrides and capture an immutable snapshot:

```python
from tix_config import ConfigManager, MappingSource

config = ConfigManager(
    [
        MappingSource({"db": {"host": "localhost", "port": 5432}}, name="defaults"),
        MappingSource({"db": {"port": 6543}}, name="overrides"),
    ]
)
config.reload()
snapshot = config.snapshot
assert snapshot.at("db.host") == "localhost"
assert snapshot.at("db.port") == 6543
```

Later sources win, while mapping keys merge recursively. Capture a snapshot once
per operation to read a consistent version. Source or validation failures leave the
previous snapshot intact; subscriber failures occur after publication.

For TOML/JSON/YAML files, environment variables, secrets, validation, and reload
notifications, see the [configuration guide](tix-config/README.md).

### Bounded concurrent work

Submit work with backpressure and retrieve results after the task scope drains:

```python
import anyio
from tix_asyncio import TaskManager


async def square(value: int) -> int:
    await anyio.lowlevel.checkpoint()
    return value * value


async def main() -> None:
    async with TaskManager(concurrency=2) as tasks:
        handles = [await tasks.submit(square, value) for value in range(5)]
    print([await handle.result() for handle in handles])


anyio.run(main)
```

This prints `[0, 1, 4, 9, 16]`. `submit()` waits for capacity before creating a child
task. Normal scope exit waits for admitted work; the default failure policy cancels
siblings and propagates errors. Completed handles leave manager ownership immediately,
though callers retaining results still consume memory.

For worker readiness, streaming maps, retry policies, cancellation tokens, and
singleflight, see the [async utilities guide](tix-asyncio/README.md).

### Reusable state machines

Define a workflow and dispatch named events:

```python
from tix_transition import MachineDefinition, State, Transition, TransitionManager

definition = MachineDefinition(
    states=(State("draft"), State("approved"), State("sent", final=True)),
    transitions=(
        Transition("approve", "draft", "approved"),
        Transition("send", "approved", "sent"),
    ),
    initial="draft",
    name="message",
    version="1",
)
manager = TransitionManager(definition, history_limit=16)
assert manager.dispatch("approve").accepted
assert manager.dispatch("send").accepted
assert manager.is_final
```

Definitions are immutable and reusable across independent managers. Guards and
callbacks can use application context and event payloads. Checkpoints capture
workflow state; they do not persist or replay application side effects.

For async dispatch, callback phases, structural edits, and native hierarchical
machines, see the [state machine guide](tix-transition/README.md).

### Resource lifecycles

Declare dependencies so resources start in order and are released when the scope exits:

```python
import anyio
from tix_lifecycle import Component, LifecycleContext, LifecycleManager


async def open_store(context: LifecycleContext) -> dict[str, str]:
    return {"status": "ready"}


async def close_store(context: LifecycleContext, store: dict[str, str]) -> None:
    store.clear()


async def open_service(context: LifecycleContext) -> dict[str, str]:
    return context.get("store")


async def main() -> None:
    manager = LifecycleManager(
        [
            Component("store", open_store, close_store),
            Component("service", open_service, requires=("store",)),
        ]
    )
    async with manager:
        store = manager.get("service")
        assert store["status"] == "ready"
    assert store == {}


anyio.run(main)
```

Independent components start concurrently within dependency layers. Cleanup runs
in reverse dependency order. If startup fails, successfully acquired resources are
released; a failing start hook owns cleanup of resources it has not returned.

For deadlines, health probes, owned resources, ASGI lifespan, and signal handling,
see the [lifecycle guide](tix-lifecycle/README.md).

## Execution and ownership

The application owns dependency wiring and resource lifetimes:

| Concern | Contract |
| --- | --- |
| Configuration reads | Captured snapshots are immutable. Reload is synchronous; offload blocking source I/O in async applications, for example with `anyio.to_thread.run_sync(config.reload)`. |
| Async backends | Managed async utilities and lifecycle operations support AnyIO with asyncio or Trio. Install Trio separately when selecting it outside the development workspace. Asyncio-specific native exports still require asyncio. |
| Async state machines | `AsyncTransitionManager` requires asyncio, matching the upstream `AsyncMachine` backend. Typed definitions describe flat state machines. |
| Native APIs | `tix-asyncio` and `tix-transition` expose upstream APIs directly. Native tasks are not automatically owned by Tix managers; native machines retain upstream behavior. |
| Task ownership | Task managers are single-use and must be entered and exited in the same task. Keep task scopes inside the lifecycle scopes that own their resources. |
| Resource ownership | Lifecycle managers are single-use. Start hooks return owned resources; stop hooks release them. Managed async objects belong to one event loop. |
| Cancellation and deadlines | Cancellation is cooperative. Deadlines cannot forcibly terminate blocking work or hooks that suppress cancellation. Signal handling is opt-in. |
| Workflow failures | Callback failures propagate. State depends on the phase reached; external side effects are not automatically rolled back. |

See [upstream compatibility](docs/upstream-compatibility.md) for native API behavior
and the additional guarantees provided by managed APIs.

## Examples

After [setting up the workspace](#development):

| Example | Run | Demonstrates |
| --- | --- | --- |
| [Service lifecycle](examples/service.py) | `uv run python examples/service.py` | Validated configuration, resource dependencies, bounded jobs, workflow transitions, health checks, and cleanup |
| [Upstream APIs](examples/upstream_apis.py) | `uv run python examples/upstream_apis.py` | Native hierarchical machines, typed guards, worker readiness, and streaming results |

Both examples finish on their own and need no database, network service, or
credentials after dependencies are installed. The service example composes all four
packages and finishes task scopes before releasing their resources.

## Development

Install `uv`, then run these commands from the repository root:

```sh
uv sync --locked --all-packages
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run python scripts/typecheck.py
uv run python examples/service.py
uv run python examples/upstream_apis.py
uv build --all-packages
uv run python scripts/check_wheels.py
```

`uv.lock` records the shared dependency resolution. `--locked` checks that the lock
file matches the workspace metadata. The type-check script runs strict mypy using
the build metadata's package-name mappings, keeping `tix-asyncio/src/asyncio`
distinct from the standard-library module.

Builds write source distributions and wheels to `dist/`. The wheel checker installs
each package in isolation and verifies public imports and typing markers. It defaults
to Python 3.11; pass `--python 3.13`, for example, to select another interpreter.

[CI](.github/workflows/ci.yml) is configured for Linux and Windows with Python
3.11–3.14. It checks tests with a 90% coverage threshold, lint, formatting, typing,
examples, builds, and isolated wheel installation. Async utilities and lifecycle
tests exercise both asyncio and Trio; async state machine tests use asyncio.
See the [validation record](docs/validation.md) for recorded checks and platform limits.

Run local performance baselines with:

```sh
uv run python benchmarks/run.py --iterations 10000 --repeats 5
```

The [benchmark guide](benchmarks/README.md) explains workloads and measurement limits.
Results are local baselines, not performance guarantees or comparisons with upstream
libraries. There are no machine-specific timing thresholds in CI.

## Documentation

| Guide | Contents |
| --- | --- |
| [Configuration](tix-config/README.md) | Source precedence, secrets, snapshots, validation, reload, and persistence |
| [Async utilities](tix-asyncio/README.md) | Task ownership, backpressure, readiness, mapping, retries, and cancellation |
| [State machines](tix-transition/README.md) | Definitions, guards, callbacks, native variants, and checkpoints |
| [Lifecycle](tix-lifecycle/README.md) | Dependency graphs, cleanup, health, ASGI lifespan, and signal adapters |
| [Architecture](docs/architecture.md) | Design contracts, reference research, extension points, and deferred features |
| [Upstream compatibility](docs/upstream-compatibility.md) | Native API behavior and managed additions |
| [Validation](docs/validation.md) | Recorded checks, environments, and platform limitations |
| [Benchmarks](benchmarks/README.md) | Local measurements, methodology, and interpretation |

## License

[Apache License 2.0](LICENSE).
