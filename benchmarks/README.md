# Local performance baselines

```sh
uv run python benchmarks/run.py --iterations 10000 --repeats 5
```

The script warms each operation, collects garbage before each repeat, and reports
median/min/max elapsed time and reciprocal throughput with interpreter/platform
metadata. GC remains enabled during measurements. There are no network operations,
external services, artificial I/O delays or performance assertions in CI.

| Metric | One measured operation |
| --- | --- |
| `snapshot_lookup` | Dotted lookup on a previously captured immutable snapshot |
| `manager_snapshot_lookup` | Acquire the published snapshot, then perform a dotted lookup |
| `config_reload_1000_values` | Reload one source containing 1,000 sequence values; includes comparison/freezing |
| `internal_transition` | One internal FSM dispatch with no callbacks or history |
| `task_batch_1000` | Admit, execute and drain 1,000 trivial tasks at capacity 32 |
| `map_batch_1000` | Map 1,000 trivial items with 32 workers and a zero-buffer input stream |
| `lifecycle_100_components` | Construct, start and stop 100 independent no-op components |

Async batches include event-loop construction and shutdown; their throughput is
**batches per second**, not tasks/items per second. Callback cost, source size,
dependency graph depth, observer work and actual I/O affect application performance.
Use identical interpreters, dependencies, hardware and workloads for comparisons.
`baseline-windows-py313.json` records the initial implementation;
`expanded-windows-py313.json` records the expanded upstream APIs and managed features.
Both contain local Windows/Python measurements, not performance promises or
comparisons against upstream libraries or their Rust/Go counterparts. These runs
were taken at different times without controlling machine load; small differences
should not be interpreted as statistically established regressions or improvements.
