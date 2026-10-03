"""Merge policy and optional reference interpolation, off the read path."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any, Literal

from .values import lookup, thaw

ListPolicy = Literal["replace", "append"]


def deep_merge(
    base: Mapping[str, Any], incoming: Mapping[str, Any], *, lists: ListPolicy = "replace"
) -> dict[str, Any]:
    if lists not in ("replace", "append"):
        raise ValueError("lists must be 'replace' or 'append'")
    result = {key: thaw(value) for key, value in base.items()}
    for key, value in incoming.items():
        previous = result.get(key)
        if isinstance(previous, Mapping) and isinstance(value, Mapping):
            result[key] = deep_merge(previous, value, lists=lists)
        elif (
            lists == "append"
            and isinstance(previous, (list, tuple))
            and isinstance(value, (list, tuple))
        ):
            result[key] = [*previous, *value]
        else:
            result[key] = thaw(value)
    return result


_REFERENCE = re.compile(r"\$\{([^{}]+)\}")


def resolve_references(data: Mapping[str, Any]) -> dict[str, Any]:
    cache: dict[tuple[str, ...], Any] = {}
    visiting: set[tuple[str, ...]] = set()

    def resolve(path: tuple[str, ...]) -> Any:
        if path in cache:
            return cache[path]
        if path in visiting:
            raise ValueError(f"cyclic configuration reference at {'.'.join(path)}")
        visiting.add(path)
        try:
            value = transform(lookup(data, path), path)
            cache[path] = value
            return value
        finally:
            visiting.remove(path)

    def transform(value: Any, path: tuple[str, ...]) -> Any:
        if isinstance(value, Mapping):
            return {key: transform(item, (*path, key)) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [transform(item, path) for item in value]
        if not isinstance(value, str):
            return value
        match = _REFERENCE.fullmatch(value)
        if match:
            return resolve(tuple(match[1].split(".")))

        def substitute(match: re.Match[str]) -> str:
            referenced = resolve(tuple(match[1].split(".")))
            if not isinstance(referenced, (str, int, float, bool)):
                raise TypeError("embedded references must resolve to scalar, non-secret values")
            return str(referenced)

        return _REFERENCE.sub(substitute, value)

    return {key: resolve((key,)) for key in data}
