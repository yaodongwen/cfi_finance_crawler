from __future__ import annotations

import asyncio
import signal

import pytest

from crawl_framework.core.shutdown import (
    ShutdownController,
    ShutdownState,
    shutdown_signal_handlers,
)


def test_shutdown_controller_uses_two_stage_semantics():
    controller = ShutdownController()

    assert controller.state is ShutdownState.RUNNING
    assert controller.request_shutdown() is ShutdownState.DRAINING
    assert controller.stop_requested is True
    assert controller.abort_requested is False
    assert controller.request_shutdown() is ShutdownState.ABORTING
    assert controller.abort_requested is True
    assert controller.requests == 2


@pytest.mark.asyncio
async def test_shutdown_controller_waiters_follow_requests():
    controller = ShutdownController()
    stop_waiter = asyncio.create_task(controller.wait_for_stop())
    abort_waiter = asyncio.create_task(controller.wait_for_abort())

    controller.request_shutdown()
    await asyncio.wait_for(stop_waiter, timeout=1)
    assert not abort_waiter.done()

    controller.request_shutdown()
    await asyncio.wait_for(abort_waiter, timeout=1)


def test_signal_handlers_are_installed_and_removed():
    calls = []

    class FakeLoop:
        def add_signal_handler(self, signum, callback):
            calls.append(("add", signum, callback))

        def remove_signal_handler(self, signum):
            calls.append(("remove", signum, None))
            return True

    controller = ShutdownController()
    observed = []

    with shutdown_signal_handlers(
        controller,
        loop=FakeLoop(),
        on_request=observed.append,
    ):
        callbacks = [item[2] for item in calls if item[0] == "add"]
        callbacks[0]()
        callbacks[0]()

    assert observed == [ShutdownState.DRAINING, ShutdownState.ABORTING]
    assert [item[1] for item in calls if item[0] == "add"] == [
        signal.SIGINT,
        signal.SIGTERM,
    ]
    assert [item[1] for item in calls if item[0] == "remove"] == [
        signal.SIGINT,
        signal.SIGTERM,
    ]
