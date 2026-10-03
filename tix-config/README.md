# tix-config

Layered local configuration with immutable snapshots. Python 3.11+, no mandatory
runtime dependencies. Install from this directory or a built wheel. YAML support
uses the `yaml` extra.

```python
from tix_config import ConfigManager, EnvSource, FileSource, MappingSource

config = ConfigManager(
    [
        MappingSource({"db": {"host": "localhost", "port": 5432}}, name="defaults"),
        FileSource("settings.toml", required=False),
        EnvSource(prefix="APP_"),  # APP_DB__PORT=6543
    ]
)
config.reload()
snapshot = config.snapshot
port = snapshot.at("db.port")
print(snapshot.origin("db.port"))
```

## Sources and precedence

Later sources win. Mapping keys merge recursively, lists replace, and scalars replace
the previous value. Set `lists="append"` for concatenation or supply a
`merge(previous, incoming)` callable for a different policy. Source names must be
unique so provenance remains useful. No implicit singleton, filesystem discovery,
environment source, or import-time I/O is involved.

Built-ins are `MappingSource`, `CallableSource`, `FileSource`, `EnvSource`, and
`SecretDirectorySource`. Implement the `ConfigSource` protocol (`name` and `load()`)
for another source. Mapping/callable sources are re-read on every reload; callers
must synchronize mutations to their own backing data.

`FileSource` supports TOML, JSON, safe YAML, or an explicit parser. `required=False`
ignores a missing file, but still reports malformed files and permission errors.
The root must be a string-keyed mapping. Environment values parse as JSON when
possible; otherwise they stay strings. Nested keys use `__` by default, names become
lowercase, and conflicting keys fail explicitly. Supply `environ`, `parser`,
`delimiter` or `lowercase` to change these policies.

`SecretDirectorySource(directory, keys=("password",))` reads only named files and
wraps values in `Secret`. Access the raw value deliberately through `.value`.
Snapshot exports mask these wrappers by default. Plain strings from other sources
are **not automatically secrets**; use `to_dict(redact=["db.password", "*.token"])`
for application-specific redaction. Interpolation never embeds a `Secret` into a
string, though a whole-value reference preserves the wrapper.

## Snapshots, validation and reload

Snapshots recursively freeze dictionaries into read-only mappings and sequences
into tuples. Accepted values are strings, numbers, booleans, null, date/time values,
secrets and nested containers; arbitrary mutable objects are rejected. Capture a
snapshot once per request to get a consistent version without repeated manager
locking. `at(("literal.dot", "child"))` supports literal dots in keys.

```python
def validate(snapshot):
    port = snapshot.at("db.port")
    if type(port) is not int or not 1 <= port <= 65535:
        raise ValueError("db.port must be a valid port")


config = ConfigManager(sources, validators=[validate])
change = config.reload()
unsubscribe = config.subscribe(lambda result: print(result.current.version))
preview = config.overlay({"db": {"port": 9000}})  # validates; does not publish
unsubscribe()
```

Validators receive the immutable candidate and reject by raising; their return values
are ignored. `snapshot.decode(decoder, path=())` passes a mutable copy to a decoder
and returns its typed result. For example, use `MySettings.model_validate` with
Pydantic, or a dataclass constructor adapter. No schema library is required.

Reloads serialize across threads. Source, merge, interpolation and validation errors
leave the old snapshot intact. Reads take only a short publication lock, never the
source-I/O lock. Equal data **and** equal provenance keep the old snapshot/version.
`Reload.changes` contains leaf additions, removals and replacements; `MISSING`
distinguishes absent values from null. Lists are treated as single diff values.

Subscribers run synchronously in registration order after publication, outside the
read lock but inside reload serialization. All subscribers are attempted; failures
raise an `ExceptionGroup` whose message says the snapshot was already published.
Callbacks may read snapshots and unsubscribe. Reentrant reload is rejected.

`interpolate=True` enables `${db.port}` references, including nested/list values.
A whole-value reference preserves its type; embedded references require non-secret
scalar values. Missing references and cycles fail reload. Provenance identifies the
source that supplied a reference, rather than pretending its resolved value came
directly from the referenced source. Overlay operates on the already resolved current
snapshot; it does not reconstruct references lost during a previous reload.

## Persistence and async applications

`save_json(path, data)` serializes before touching the destination, flushes a
same-directory temporary file, and uses atomic replacement. Existing readers see
complete old or new contents. Files must be JSON-compatible; explicitly export
secrets to persist them. The parent directory must exist. Replacement creates a new
file with temporary-file permissions; existing mode/ACL metadata is not preserved.
This does not guarantee directory-entry durability after power loss.

This package owns no watcher threads or event loop. In an AnyIO application, reload
with `await anyio.to_thread.run_sync(config.reload)`. For periodic refresh, compose
that call with `tix_asyncio.run_periodic`; OS watcher integrations can call the same
reload API. Application code decides how to report and retry invalid updates.
