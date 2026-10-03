"""Local sources have no runtime or schema-library dependency."""

from __future__ import annotations

import json
import os
import tomllib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from .values import Secret


class ConfigSource(Protocol):
    @property
    def name(self) -> str: ...

    def load(self) -> Mapping[str, Any]: ...


@dataclass(frozen=True, slots=True)
class MappingSource:
    data: Mapping[str, Any] = field(repr=False)
    name: str = "mapping"

    def load(self) -> Mapping[str, Any]:
        return self.data


@dataclass(frozen=True, slots=True)
class CallableSource:
    loader: Callable[[], Mapping[str, Any]] = field(repr=False)
    name: str = "callable"

    def load(self) -> Mapping[str, Any]:
        return self.loader()


@dataclass(frozen=True, slots=True)
class FileSource:
    path: str | Path
    required: bool = True
    parser: Callable[[str], Mapping[str, Any]] | None = field(default=None, repr=False)

    @property
    def name(self) -> str:
        return str(self.path)

    def load(self) -> Mapping[str, Any]:
        path = Path(self.path)
        try:
            text = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            if self.required:
                raise
            return {}
        result: Any
        if self.parser is not None:
            result = self.parser(text)
        elif path.suffix.lower() == ".toml":
            result = tomllib.loads(text)
        elif path.suffix.lower() == ".json":
            result = json.loads(text)
        elif path.suffix.lower() in (".yaml", ".yml"):
            try:
                import yaml
            except ImportError as error:
                raise ImportError("install tix-config[yaml] to load YAML") from error
            result = yaml.safe_load(text)
        else:
            raise ValueError("unknown file format; supply a parser")
        if not isinstance(result, Mapping):
            raise TypeError("configuration file root must be a mapping")
        return result


def parse_value(value: str) -> Any:
    try:
        return json.loads(value)
    except ValueError:
        return value


def _insert(data: dict[str, Any], path: list[str], value: Any) -> None:
    if not all(path):
        raise ValueError("empty configuration path segment")
    current = data
    for part in path[:-1]:
        child = current.setdefault(part, {})
        if not isinstance(child, dict):
            raise ValueError("conflicting nested source keys")
        current = child
    if path[-1] in current:
        raise ValueError("conflicting source keys")
    current[path[-1]] = value


@dataclass(frozen=True, slots=True)
class EnvSource:
    prefix: str = "APP_"
    delimiter: str = "__"
    lowercase: bool = True
    environ: Mapping[str, str] | None = field(default=None, repr=False)
    parser: Callable[[str], Any] = field(default=parse_value, repr=False)
    name: str = "environment"

    def __post_init__(self) -> None:
        if not self.delimiter:
            raise ValueError("delimiter must not be empty")

    def load(self) -> Mapping[str, Any]:
        result: dict[str, Any] = {}
        environment = os.environ if self.environ is None else self.environ
        for key, value in sorted(environment.items()):
            if key.startswith(self.prefix):
                key = key[len(self.prefix) :]
                key = key.lower() if self.lowercase else key
                _insert(result, key.split(self.delimiter), self.parser(value))
        return result


@dataclass(frozen=True, slots=True)
class SecretDirectorySource:
    """Explicit allowlist avoids reading unrelated files from a directory."""

    directory: str | Path
    keys: tuple[str, ...]
    required: bool = True
    name: str = "secrets"

    def load(self) -> Mapping[str, Any]:
        result: dict[str, Any] = {}
        for key in self.keys:
            if not key or key in (".", "..") or any(char in key for char in ("/", "\\", ":")):
                raise ValueError("secret keys must be plain filenames")
            try:
                value = (Path(self.directory) / key).read_text(encoding="utf-8").rstrip("\r\n")
            except FileNotFoundError:
                if self.required:
                    raise
                continue
            result[key] = Secret(value)
        return result
