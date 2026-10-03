"""Opt-in process signals and framework-neutral ASGI lifespan integration."""

from __future__ import annotations

import signal
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import anyio

from .manager import LifecycleManager


@asynccontextmanager
async def lifespan(manager: LifecycleManager, app: Any = None) -> AsyncIterator[dict[str, Any]]:
    """Bind with partial(lifespan, manager) for Starlette/FastAPI lifespan=."""
    async with manager:
        yield {"lifecycle": manager}


async def serve(manager: LifecycleManager, *, install_signals: bool = False) -> None:
    """Wait for a cooperative stop; signal receivers are opt-in and platform-specific."""
    if not install_signals:
        async with manager:
            await manager.wait_shutdown()
        return

    with anyio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
        async with anyio.create_task_group() as group:

            async def receive_signal() -> None:
                async for _ in signals:
                    manager.request_shutdown()
                    break

            group.start_soon(receive_signal)
            async with manager:
                await manager.wait_shutdown()
            group.cancel_scope.cancel()
