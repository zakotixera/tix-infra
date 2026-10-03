"""Cooperative stop notification, separate from forced task cancellation."""

from __future__ import annotations

from weakref import WeakSet

import anyio


class CancellationToken:
    def __init__(self) -> None:
        self._event = anyio.Event()
        self._children: WeakSet[CancellationToken] = WeakSet()

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()

    def cancel(self) -> None:
        pending = [self]
        while pending:
            token = pending.pop()
            if not token.cancelled:
                token._event.set()
                pending.extend(token._children)

    async def wait(self) -> None:
        await self._event.wait()

    def child(self) -> CancellationToken:
        child = CancellationToken()
        self._children.add(child)
        if self.cancelled:
            child.cancel()
        return child
