from __future__ import annotations

import asyncio
import signal

from contextlib import contextmanager
from enum import Enum
from typing import Callable, Iterator


class ShutdownState(str, Enum):
    RUNNING = "running"
    DRAINING = "draining"
    ABORTING = "aborting"


class ShutdownController:
    """Process-local two-stage shutdown state shared by CLI and runtimes."""

    def __init__(self) -> None:
        self._requests = 0
        self._stop_event = asyncio.Event()
        self._abort_event = asyncio.Event()

    @property
    def requests(self) -> int:
        return self._requests

    @property
    def stop_requested(self) -> bool:
        return self._stop_event.is_set()

    @property
    def abort_requested(self) -> bool:
        return self._abort_event.is_set()

    @property
    def state(self) -> ShutdownState:
        if self.abort_requested:
            return ShutdownState.ABORTING
        if self.stop_requested:
            return ShutdownState.DRAINING
        return ShutdownState.RUNNING

    def request_shutdown(self) -> ShutdownState:
        self._requests += 1
        self._stop_event.set()
        if self._requests >= 2:
            self._abort_event.set()
        return self.state

    async def wait_for_stop(self) -> None:
        await self._stop_event.wait()

    async def wait_for_abort(self) -> None:
        await self._abort_event.wait()


@contextmanager
def shutdown_signal_handlers(
    controller: ShutdownController,
    *,
    loop: asyncio.AbstractEventLoop | None = None,
    on_request: Callable[[ShutdownState], None] | None = None,
) -> Iterator[None]:
    """Install temporary SIGINT/SIGTERM handlers on supported event loops."""

    loop = loop or asyncio.get_running_loop()
    installed: list[signal.Signals] = []

    def request() -> None:
        state = controller.request_shutdown()
        if on_request is not None:
            on_request(state)

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, request)
        except (NotImplementedError, RuntimeError, ValueError):
            continue
        installed.append(signum)

    try:
        yield
    finally:
        for signum in installed:
            loop.remove_signal_handler(signum)
