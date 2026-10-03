"""Immutable configuration values and snapshot operations."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time
from fnmatch import fnmatchcase
from types import MappingProxyType
from typing import Any, TypeVar

T = TypeVar("T")
Path = str | tuple[str, ...]
MISSING = object()


@dataclass(frozen=True, slots=True)
class Secret:
    value: str = field(repr=False)

    def __repr__(self) -> str:
        return "Secret('********')"

    def __str__(self) -> str:
        return "********"


def freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("configuration keys must be strings")
        return MappingProxyType({key: freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(freeze(item) for item in value)
    if value is None or isinstance(value, (str, bool, int, float, date, datetime, time, Secret)):
        return value
    raise TypeError(f"unsupported configuration value type: {type(value).__name__}")


def thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [thaw(item) for item in value]
    return value


def parts(path: Path) -> tuple[str, ...]:
    return tuple(path.split(".")) if isinstance(path, str) else path


def lookup(data: Mapping[str, Any], path: Path) -> Any:
    value: Any = data
    for key in parts(path):
        if not isinstance(value, Mapping) or key not in value:
            raise KeyError(path)
        value = value[key]
    return value


def leaves(data: Mapping[str, Any], prefix: tuple[str, ...] = ()) -> dict[tuple[str, ...], Any]:
    result: dict[tuple[str, ...], Any] = {}
    for key, value in data.items():
        path = (*prefix, key)
        if isinstance(value, Mapping) and value:
            result.update(leaves(value, path))
        else:
            result[path] = value
    return result


@dataclass(frozen=True, slots=True)
class Change:
    path: tuple[str, ...]
    before: Any = field(repr=False)
    after: Any = field(repr=False)


@dataclass(frozen=True, slots=True, repr=False)
class Snapshot(Mapping[str, Any]):
    """A captured version stays valid after the manager reloads."""

    data: Mapping[str, Any]
    version: int = 0
    origins: Mapping[tuple[str, ...], str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "data", freeze(self.data))
        object.__setattr__(self, "origins", MappingProxyType(dict(self.origins)))

    def __repr__(self) -> str:
        return f"Snapshot(version={self.version}, keys={tuple(self.data)})"

    def __getitem__(self, key: str) -> Any:
        return self.data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.data)

    def __len__(self) -> int:
        return len(self.data)

    def at(self, path: Path, default: Any = MISSING) -> Any:
        try:
            return lookup(self.data, path)
        except KeyError:
            if default is MISSING:
                raise
            return default

    def decode(self, decoder: Callable[[Any], T], path: Path = ()) -> T:
        """Adapt dataclasses, Pydantic, msgspec, or application validators explicitly."""
        return decoder(thaw(self.at(path)))

    def origin(self, path: Path) -> str | None:
        return self.origins.get(parts(path))

    def diff(self, other: Snapshot) -> tuple[Change, ...]:
        before, after = leaves(self.data), leaves(other.data)
        return tuple(
            Change(path, before.get(path, MISSING), after.get(path, MISSING))
            for path in sorted(before.keys() | after.keys())
            if before.get(path, MISSING) != after.get(path, MISSING)
        )

    def to_dict(
        self, *, redact: Sequence[str] = (), reveal_secrets: bool = False
    ) -> dict[str, Any]:
        """Return a copy. Secret values are masked unless explicitly revealed."""

        def export(value: Any, path: tuple[str, ...]) -> Any:
            if any(fnmatchcase(".".join(path), pattern) for pattern in redact):
                return "********"
            if isinstance(value, Secret):
                return value.value if reveal_secrets else "********"
            if isinstance(value, Mapping):
                return {key: export(item, (*path, key)) for key, item in value.items()}
            if isinstance(value, tuple):
                return [export(item, (*path, str(i))) for i, item in enumerate(value)]
            return value

        return {key: export(value, (key,)) for key, value in self.data.items()}
