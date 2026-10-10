"""Cancellation policy shared by bounded mapping producers and workers."""

from collections.abc import Iterator
from contextlib import contextmanager
from math import inf

import anyio

from .handles import TaskCancelled


@contextmanager
def report_independent_cancellation() -> Iterator[None]:
    """For child tasks only: preserve group cancellation, report independent work.

    The mapping caller must remain outside this boundary: native asyncio caller
    cancellation need not appear in AnyIO's effective deadline.
    """
    try:
        yield
    except (anyio.get_cancelled_exc_class(), BaseExceptionGroup) as error:
        if isinstance(error, BaseExceptionGroup):
            _, other_errors = error.split(anyio.get_cancelled_exc_class())
            if other_errors is not None:
                raise
        if anyio.current_effective_deadline() == -inf:
            raise
        # Backends may also suppress cancellation-only exception groups. Independent
        # cancellation must fail the mapping rather than discard unfinished inputs.
        raise TaskCancelled("mapping operation or input was cancelled") from error
