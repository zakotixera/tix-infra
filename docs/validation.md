# Validation record

## Review fixes — macOS, 2026-10-10

| Check | Result |
| --- | --- |
| Workspace tests, CPython 3.13.11, AnyIO 4.15.1, transitions 0.9.3 | 213 passed |
| Installed-wheel tests, CPython 3.11.15, AnyIO 4.4.0, transitions 0.9.2 | 213 passed |
| Workspace line coverage across four packages | 97.87% (1,643 statements) |
| Ruff lint and format checks | Passed |
| Strict mypy checks | Passed, 25 source files |
| Source distributions, wheels and independent wheel installations | All four passed |
| Composition and upstream API examples | Passed |

Added 68 regression cases covering independently cancelled startup, exhaustive
release and rollback, health cancellation, mapping operations and iterators,
cancellation-only and mixed exception groups, ambient cancellation, scalar type
changes, overlay provenance, and sync/async guard-query parity. AnyIO tests exercise
both asyncio and Trio; cancelled asyncio Future cases run on asyncio explicitly.

Native asyncio regressions cover `timeout`, `wait_for` and `Task.cancel` for both
mapping APIs, with zero and buffered input capacity. They verify exception types,
task cancellation state and worker cleanup; separate cases verify independently
self-cancelled workers still fail the mapping. Ordered mapping now produces inputs
in an owned child task, keeping the caller outside cancellation conversion. Streaming
exit also preserves consumer cancellation when older AnyIO versions suppress it
during explicit task-group shutdown.

Review caught an additional Trio cancellation-only exception-group case after the
first fixes. Lifecycle orchestration now retains suppressed interruptions, and the
mapping cancellation boundary handles nested cancellation groups. The final source
and newly rebuilt minimum-dependency wheels pass the complete suite. This run does
not replace the Windows/Linux and Python 3.12/3.14 CI matrix.

## Initial implementation — Windows, 2026-10-04

Local validation completed on Windows 11, 2026-10-04.

| Check | Result |
| --- | --- |
| Workspace tests, CPython 3.13.14, AnyIO 4.15.1, transitions 0.9.3 | 144 passed, 1 platform skip |
| Installed-wheel tests, CPython 3.11.9, minimum AnyIO 4.4.0 and transitions 0.9.2 | 144 passed, 1 platform skip |
| Workspace line coverage across four packages | 97.59% (1,577 statements) |
| Ruff lint and format checks | Passed |
| Strict mypy checks on all package sources | Passed, 24 source files |
| Locked workspace resolution | Passed |
| Source distributions and wheels | All four built successfully |
| Isolated installation of each wheel without other Tix packages | Passed; public imports and py.typed verified |
| Composition example | Passed; settings, resource graph, bounded jobs, workflow and release |
| Upstream API example | Passed; native hierarchy, typed guards, worker readiness and streaming |
| Benchmark runner | Completed; environment and results saved under benchmarks/ |

AnyIO-based task and lifecycle tests run on both asyncio and Trio. Async FSM tests
use asyncio, matching the upstream backend. The signal adapter is tested with an
injected receiver on both AnyIO backends. Real SIGTERM subprocess delivery is skipped
on Windows and included in the Linux CI suite.

Expanded compatibility tests exercise native enum/multi-model/dynamic/ordered/queued
machines, hierarchical/parallel states, locking, and async hierarchy. Native AnyIO
and asyncio exports retain object identity. Managed tests cover readiness before and
after failure, cancellation, capacity reuse, bounded streaming and early exit, callback
phases, named context conditions, guard queries, atomic edits and finalizer failures.
Review regressions cover interrupted completion, preservation of earlier hook and
observer failures, native cancellation propagation, history and lock release.

The GitHub Actions workflow is configured for Windows/Linux and Python 3.11–3.14;
those remote CI jobs have not been run locally. Local measurements do not establish
cross-platform throughput, long-duration production behavior or performance relative
to other libraries. No packages were published and no remote deployment was made.

Reproduce the primary checks:

```sh
uv sync --locked --all-packages
uv run pytest --cov=tix_config --cov=tix_asyncio --cov=tix_transition --cov=tix_lifecycle
uv run ruff check .
uv run ruff format --check .
uv run python scripts/typecheck.py
uv run python examples/service.py
uv build --all-packages
uv run python scripts/check_wheels.py --python 3.11
uv run python scripts/check_wheels.py --python 3.11 --minimum-tests
uv run python examples/upstream_apis.py
uv run python benchmarks/run.py
```
