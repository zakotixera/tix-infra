# Validation record

Local validation completed on Windows 11, 2026-10-04.

| Check | Result |
| --- | --- |
| Workspace tests, CPython 3.13.14, AnyIO 4.15.1, transitions 0.9.3 | 95 passed, 1 platform skip |
| Installed-wheel tests, CPython 3.11.9, minimum AnyIO 4.4.0 and transitions 0.9.2 | 95 passed, 1 platform skip |
| Workspace line coverage across four packages | 96.98% (1,258 statements) |
| Ruff lint and format checks | Passed |
| Strict mypy checks on all package sources | Passed, 20 source files |
| Locked workspace resolution | Passed |
| Source distributions and wheels | All four built successfully |
| Isolated installation of each wheel without other Tix packages | Passed; public imports and py.typed verified |
| Composition example | Passed; settings, resource graph, bounded jobs, workflow and release |
| Benchmark runner | Completed; environment and results saved under benchmarks/ |

AnyIO-based task and lifecycle tests run on both asyncio and Trio. Async FSM tests
use asyncio, matching the upstream backend. The signal adapter is tested with an
injected receiver on both AnyIO backends. Real SIGTERM subprocess delivery is skipped
on Windows and included in the Linux CI suite.

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
uv run python benchmarks/run.py
```
