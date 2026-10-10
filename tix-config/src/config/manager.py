"""Serialized reloads and atomic snapshot publication."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from threading import Lock, RLock, get_ident
from typing import Any

from .merge import ListPolicy, deep_merge, resolve_references
from .sources import ConfigSource
from .values import MISSING, Change, Snapshot, equal, freeze, leaves, thaw


class SourceError(RuntimeError):
    """A source could not be loaded; the published snapshot is unchanged."""


@dataclass(frozen=True, slots=True)
class Reload:
    previous: Snapshot
    current: Snapshot
    changes: tuple[Change, ...]

    @property
    def changed(self) -> bool:
        return self.previous is not self.current


class ConfigManager:
    def __init__(
        self,
        sources: Iterable[ConfigSource],
        *,
        validators: Iterable[Callable[[Snapshot], object]] = (),
        lists: ListPolicy = "replace",
        merge: Callable[[Mapping[str, Any], Mapping[str, Any]], Mapping[str, Any]] | None = None,
        interpolate: bool = False,
    ) -> None:
        if lists not in ("replace", "append"):
            raise ValueError("invalid list merge policy")
        self._sources = tuple(sources)
        names = [source.name for source in self._sources]
        if len(set(names)) != len(names):
            raise ValueError("source names must be unique for provenance")
        self._validators = tuple(validators)
        self._merge = merge or (lambda left, right: deep_merge(left, right, lists=lists))
        self._interpolate = interpolate
        self._snapshot = Snapshot({})
        self._read_lock = Lock()
        self._reload_lock = RLock()
        self._reload_owner: int | None = None
        self._subscribers: dict[object, Callable[[Reload], object]] = {}

    @property
    def snapshot(self) -> Snapshot:
        with self._read_lock:
            return self._snapshot

    def subscribe(self, callback: Callable[[Reload], object]) -> Callable[[], None]:
        key = object()
        with self._read_lock:
            self._subscribers[key] = callback

        def unsubscribe() -> None:
            with self._read_lock:
                self._subscribers.pop(key, None)

        return unsubscribe

    def reload(self) -> Reload:
        with self._reload_lock:
            if self._reload_owner == get_ident():
                raise RuntimeError("reentrant configuration reload")
            self._reload_owner = get_ident()
            try:
                return self._reload()
            finally:
                self._reload_owner = None

    def _reload(self) -> Reload:
        data: Mapping[str, Any] = {}
        origins: dict[tuple[str, ...], str] = {}
        for source in self._sources:
            try:
                incoming = freeze(source.load())
                if not isinstance(incoming, Mapping):
                    raise TypeError("source root must be a mapping")
            except Exception as error:
                raise SourceError(f"failed to load source {source.name!r}") from error
            data, origins = self._merge_layer(data, origins, incoming, source.name)
        previous = self.snapshot
        candidate = self._candidate(data, origins, previous.version + 1)
        if equal(candidate.data, previous.data) and candidate.origins == previous.origins:
            return Reload(previous, previous, ())
        result = Reload(previous, candidate, previous.diff(candidate))
        with self._read_lock:
            self._snapshot = candidate
            subscribers = tuple(self._subscribers.values())
        errors: list[Exception] = []
        for subscriber in subscribers:
            try:
                subscriber(result)
            except Exception as error:
                errors.append(error)
        if errors:
            raise ExceptionGroup("configuration published; subscribers failed", errors)
        return result

    def _merge_layer(
        self,
        data: Mapping[str, Any],
        origins: Mapping[tuple[str, ...], str],
        incoming: Mapping[str, Any],
        name: str,
    ) -> tuple[Mapping[str, Any], dict[tuple[str, ...], str]]:
        before, supplied = leaves(data), leaves(incoming)
        merged = self._merge(data, incoming)
        return merged, {
            path: name
            if path in supplied or not equal(before.get(path, MISSING), value)
            else origins.get(path, name)
            for path, value in leaves(merged).items()
        }

    def _candidate(
        self, data: Mapping[str, Any], origins: Mapping[tuple[str, ...], str], version: int
    ) -> Snapshot:
        if self._interpolate:
            data = resolve_references(data)
            origins = {
                path: next(
                    (
                        origins[path[:length]]
                        for length in range(len(path), 0, -1)
                        if path[:length] in origins
                    ),
                    "interpolation",
                )
                for path in leaves(data)
            }
        candidate = Snapshot(data, version, origins)
        for validator in self._validators:
            validator(candidate)
        return candidate

    def overlay(self, values: Mapping[str, Any]) -> Snapshot:
        """Validate an independent override snapshot without publishing it."""
        current = self.snapshot
        data, origins = self._merge_layer(current.data, current.origins, freeze(values), "overlay")
        return self._candidate(data, origins, current.version)


def save_json(path: str | Path, data: Mapping[str, Any]) -> None:
    """Atomically replace a JSON file; readers observe a complete old or new file.

    Secret objects must be deliberately exported before persistence. This flushes
    file data, but does not promise directory-entry durability after power loss.
    """
    destination = Path(path)
    serialized = json.dumps(thaw(freeze(data)), indent=2, ensure_ascii=False, allow_nan=False)
    descriptor, temporary = tempfile.mkstemp(dir=destination.parent, prefix=f".{destination.name}.")
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(serialized + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, destination)
    finally:
        Path(temporary).unlink(missing_ok=True)
