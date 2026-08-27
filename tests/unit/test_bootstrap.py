import pytest

from crawl_framework.core.bootstrap import (
    BootstrapConfig,
    CrawlBootstrap,
    StartupRecoveryBlockedError,
    run_bootstrap,
)
from crawl_framework.storage.recovery_orchestrator import (
    StartupRecoveryResult,
)


# ============================================================
# Fakes
# ============================================================


class FakeRecoveryOrchestrator:

    def __init__(
        self,
        result,
    ):

        self.result = result

        self.run_count = 0


    def run(
        self,
    ):

        self.run_count += 1

        return self.result


class FakeRuntime:

    def __init__(
        self,
        result=None,
    ):

        self.result = (
            result
            if result is not None
            else {
                "runtime": "done"
            }
        )

        self.run_count = 0

        self.calls = []


    async def run(
        self,
        *,
        datasets=None,
        flush_at_end=True,
    ):

        self.run_count += 1

        self.calls.append(
            {
                "datasets": datasets,
                "flush_at_end": (
                    flush_at_end
                ),
            }
        )

        return self.result


# ============================================================
# Helpers
# ============================================================


def recovery_result(
    *,
    can_continue=True,
    scanned=0,
    attempted=0,
    recovered=0,
    skipped=0,
    failed=0,
    terminal_failed=0,
    remaining_pending=0,
):

    return StartupRecoveryResult(
        scanned=scanned,
        attempted=attempted,
        recovered=recovered,
        skipped=skipped,
        failed=failed,
        terminal_failed=(
            terminal_failed
        ),
        remaining_pending=(
            remaining_pending
        ),
        can_continue=(
            can_continue
        ),
        results=(),
    )


# ============================================================
# Tests
# ============================================================


@pytest.mark.asyncio
async def test_recovery_runs_before_runtime():

    events = []


    class OrderedRecovery:

        def run(
            self,
        ):

            events.append(
                "recovery"
            )

            return recovery_result(
                can_continue=True
            )


    class OrderedRuntime:

        async def run(
            self,
            *,
            datasets=None,
            flush_at_end=True,
        ):

            events.append(
                "runtime"
            )

            return {
                "ok": True
            }


    bootstrap = CrawlBootstrap(
        recovery_orchestrator=(
            OrderedRecovery()
        ),
        runtime=OrderedRuntime(),
    )

    result = await bootstrap.run()

    assert events == [
        "recovery",
        "runtime",
    ]

    assert (
        result.success
        is True
    )


@pytest.mark.asyncio
async def test_runtime_runs_when_recovery_allows():

    recovery = (
        FakeRecoveryOrchestrator(
            recovery_result(
                can_continue=True
            )
        )
    )

    runtime = FakeRuntime()

    bootstrap = CrawlBootstrap(
        recovery_orchestrator=(
            recovery
        ),
        runtime=runtime,
    )

    result = await bootstrap.run()

    assert (
        recovery.run_count
        == 1
    )

    assert (
        runtime.run_count
        == 1
    )

    assert (
        result.crawler_started
        is True
    )

    assert (
        result.success
        is True
    )

    assert (
        result.runtime
        == {
            "runtime": "done"
        }
    )


@pytest.mark.asyncio
async def test_runtime_not_started_when_recovery_blocks():

    recovery = (
        FakeRecoveryOrchestrator(
            recovery_result(
                can_continue=False,
                failed=1,
            )
        )
    )

    runtime = FakeRuntime()

    bootstrap = CrawlBootstrap(
        recovery_orchestrator=(
            recovery
        ),
        runtime=runtime,
    )

    result = await bootstrap.run()

    assert (
        recovery.run_count
        == 1
    )

    assert (
        runtime.run_count
        == 0
    )

    assert (
        result.crawler_started
        is False
    )

    assert (
        result.success
        is False
    )

    assert (
        result.runtime
        is None
    )


@pytest.mark.asyncio
async def test_raise_when_recovery_blocks():

    recovery = (
        FakeRecoveryOrchestrator(
            recovery_result(
                can_continue=False,
                failed=1,
            )
        )
    )

    runtime = FakeRuntime()

    bootstrap = CrawlBootstrap(
        recovery_orchestrator=(
            recovery
        ),
        runtime=runtime,
        config=BootstrapConfig(
            raise_on_recovery_block=True
        ),
    )

    with pytest.raises(
        StartupRecoveryBlockedError
    ):

        await bootstrap.run()

    assert (
        runtime.run_count
        == 0
    )


@pytest.mark.asyncio
async def test_config_datasets_passed_to_runtime():

    recovery = (
        FakeRecoveryOrchestrator(
            recovery_result()
        )
    )

    runtime = FakeRuntime()

    bootstrap = CrawlBootstrap(
        recovery_orchestrator=(
            recovery
        ),
        runtime=runtime,
        config=BootstrapConfig(
            datasets=(
                "forum_post",
                "news_article",
            ),
        ),
    )

    await bootstrap.run()

    assert (
        runtime.calls[0][
            "datasets"
        ]
        == (
            "forum_post",
            "news_article",
        )
    )


@pytest.mark.asyncio
async def test_run_datasets_override_config():

    recovery = (
        FakeRecoveryOrchestrator(
            recovery_result()
        )
    )

    runtime = FakeRuntime()

    bootstrap = CrawlBootstrap(
        recovery_orchestrator=(
            recovery
        ),
        runtime=runtime,
        config=BootstrapConfig(
            datasets=(
                "forum_post",
            ),
        ),
    )

    await bootstrap.run(
        datasets=(
            "news_article",
        )
    )

    assert (
        runtime.calls[0][
            "datasets"
        ]
        == (
            "news_article",
        )
    )


@pytest.mark.asyncio
async def test_default_flush_at_end_true():

    recovery = (
        FakeRecoveryOrchestrator(
            recovery_result()
        )
    )

    runtime = FakeRuntime()

    bootstrap = CrawlBootstrap(
        recovery_orchestrator=(
            recovery
        ),
        runtime=runtime,
    )

    await bootstrap.run()

    assert (
        runtime.calls[0][
            "flush_at_end"
        ]
        is True
    )


@pytest.mark.asyncio
async def test_config_flush_at_end_false():

    recovery = (
        FakeRecoveryOrchestrator(
            recovery_result()
        )
    )

    runtime = FakeRuntime()

    bootstrap = CrawlBootstrap(
        recovery_orchestrator=(
            recovery
        ),
        runtime=runtime,
        config=BootstrapConfig(
            flush_at_end=False
        ),
    )

    await bootstrap.run()

    assert (
        runtime.calls[0][
            "flush_at_end"
        ]
        is False
    )


@pytest.mark.asyncio
async def test_run_flush_override_config():

    recovery = (
        FakeRecoveryOrchestrator(
            recovery_result()
        )
    )

    runtime = FakeRuntime()

    bootstrap = CrawlBootstrap(
        recovery_orchestrator=(
            recovery
        ),
        runtime=runtime,
        config=BootstrapConfig(
            flush_at_end=False
        ),
    )

    await bootstrap.run(
        flush_at_end=True
    )

    assert (
        runtime.calls[0][
            "flush_at_end"
        ]
        is True
    )


@pytest.mark.asyncio
async def test_bootstrap_result_contains_recovery_result():

    recovery_result_obj = (
        recovery_result(
            scanned=5,
            attempted=4,
            recovered=3,
            skipped=1,
            remaining_pending=1,
        )
    )

    recovery = (
        FakeRecoveryOrchestrator(
            recovery_result_obj
        )
    )

    runtime = FakeRuntime()

    bootstrap = CrawlBootstrap(
        recovery_orchestrator=(
            recovery
        ),
        runtime=runtime,
    )

    result = await bootstrap.run()

    assert (
        result.recovery
        is recovery_result_obj
    )

    assert (
        result.recovery.scanned
        == 5
    )

    assert (
        result.recovery.recovered
        == 3
    )


@pytest.mark.asyncio
async def test_run_bootstrap_helper():

    recovery = (
        FakeRecoveryOrchestrator(
            recovery_result()
        )
    )

    runtime = FakeRuntime(
        result="finished"
    )

    result = await run_bootstrap(
        recovery_orchestrator=(
            recovery
        ),
        runtime=runtime,
    )

    assert (
        result.success
        is True
    )

    assert (
        result.runtime
        == "finished"
    )

    assert (
        runtime.run_count
        == 1
    )