from __future__ import annotations

import asyncio

import pytest

from crawl_framework.transports.playwright import (
    BrowserWorkerPool,
    PlaywrightTransportConfig,
)
from crawl_framework.transports.proxy import ProxyPool


class FakePage:
    def __init__(self) -> None:
        self.timeout = None
        self.navigation_timeout = None
        self.closed = False

    def set_default_timeout(self, value):
        self.timeout = value

    def set_default_navigation_timeout(self, value):
        self.navigation_timeout = value

    async def close(self):
        self.closed = True


class FakeContext:
    def __init__(self, options) -> None:
        self.options = options
        self.pages = []
        self.closed = False

    async def new_page(self):
        page = FakePage()
        self.pages.append(page)
        return page

    async def close(self):
        self.closed = True


class FakeBrowser:
    def __init__(self) -> None:
        self.contexts = []
        self.closed = False

    async def new_context(self, **options):
        context = FakeContext(options)
        self.contexts.append(context)
        return context

    async def close(self):
        self.closed = True


class FakeBrowserType:
    def __init__(self) -> None:
        self.browsers = []
        self.persistent = []

    async def launch(self, **options):
        browser = FakeBrowser()
        browser.launch_options = options
        self.browsers.append(browser)
        return browser

    async def launch_persistent_context(self, **options):
        context = FakeContext(options)
        self.persistent.append(context)
        return context


class FakePlaywright:
    def __init__(self) -> None:
        self.chromium = FakeBrowserType()
        self.stopped = False

    async def stop(self):
        self.stopped = True


def starter_for(playwright):
    async def start():
        return playwright

    return start


@pytest.mark.asyncio
async def test_pool_is_bounded_and_sets_page_defaults():
    playwright = FakePlaywright()
    pool = BrowserWorkerPool(
        PlaywrightTransportConfig(
            workers=1,
            page_timeout_ms=123,
            navigation_timeout_ms=456,
        ),
        playwright_starter=starter_for(playwright),
    )

    entered = asyncio.Event()
    release = asyncio.Event()

    async def first_lease():
        async with pool.lease() as lease:
            assert lease.page.timeout == 123
            assert lease.page.navigation_timeout == 456
            entered.set()
            await release.wait()

    first = asyncio.create_task(first_lease())
    await entered.wait()
    second_entered = False

    async def second_lease():
        nonlocal second_entered
        async with pool.lease():
            second_entered = True

    second = asyncio.create_task(second_lease())
    await asyncio.sleep(0)
    assert second_entered is False
    assert pool.stats.max_active_leases == 1

    release.set()
    await asyncio.gather(first, second)
    await pool.close()
    assert playwright.stopped is True
    assert pool.stats.channel_max_active["default"] == 1


@pytest.mark.asyncio
async def test_workers_have_isolated_contexts_and_round_robin_proxies():
    playwright = FakePlaywright()
    proxy_pool = ProxyPool(["http://proxy-a", "http://proxy-b"])
    async with BrowserWorkerPool(
        PlaywrightTransportConfig(workers=2),
        proxy_pool=proxy_pool,
        playwright_starter=starter_for(playwright),
    ) as pool:
        async with pool.lease() as first, pool.lease() as second:
            assert first.worker_id != second.worker_id
            assert first.context is not second.context
            assert {first.proxy.url, second.proxy.url} == {
                "http://proxy-a",
                "http://proxy-b",
            }
            assert first.context.options["proxy"]["server"].startswith("http://")

    assert all(browser.closed for browser in playwright.chromium.browsers)


@pytest.mark.asyncio
async def test_failed_lease_reports_proxy_failure_and_recycles_worker():
    playwright = FakePlaywright()
    proxy_pool = ProxyPool(["http://proxy-a", "http://proxy-b"])
    pool = BrowserWorkerPool(
        PlaywrightTransportConfig(workers=1),
        proxy_pool=proxy_pool,
        playwright_starter=starter_for(playwright),
    )

    with pytest.raises(RuntimeError, match="crawl failed"):
        async with pool.lease() as lease:
            assert lease.proxy.url == "http://proxy-a"
            raise RuntimeError("crawl failed")

    async with pool.lease() as lease:
        assert lease.proxy.url == "http://proxy-b"

    assert proxy_pool.states[0].failures == 1
    assert pool.stats.lease_failures == 1
    assert pool.stats.worker_recycles == 1
    await pool.close()


@pytest.mark.asyncio
async def test_persistent_profiles_are_isolated_and_recycled(tmp_path):
    playwright = FakePlaywright()
    async with BrowserWorkerPool(
        PlaywrightTransportConfig(
            workers=2,
            profile_root=tmp_path / "profiles",
            recycle_after_scopes=1,
        ),
        playwright_starter=starter_for(playwright),
    ) as pool:
        async with pool.lease() as lease:
            first_worker = lease.worker_id

        assert pool.stats.worker_recycles == 1
        profile_dirs = sorted((tmp_path / "profiles").iterdir())
        assert [path.name for path in profile_dirs] == ["worker-000", "worker-001"]
        assert first_worker in {0, 1}

    assert all(context.closed for context in playwright.chromium.persistent)


@pytest.mark.asyncio
async def test_named_worker_budget_applies_backpressure_with_free_workers():
    playwright = FakePlaywright()
    pool = BrowserWorkerPool(
        PlaywrightTransportConfig(
            workers=3,
            worker_budgets=(("news_detail", 1), ("forum", 2)),
        ),
        playwright_starter=starter_for(playwright),
    )
    assert pool.channel_capacity("news_detail") == 1
    assert pool.channel_capacity("forum") == 2
    assert pool.channel_capacity("unconfigured") == 3
    first_entered = asyncio.Event()
    release = asyncio.Event()
    second_entered = False

    async def first():
        async with pool.lease("news_detail"):
            first_entered.set()
            await release.wait()

    async def second():
        nonlocal second_entered
        async with pool.lease("news_detail"):
            second_entered = True

    first_task = asyncio.create_task(first())
    await first_entered.wait()
    second_task = asyncio.create_task(second())
    await asyncio.sleep(0)
    assert second_entered is False
    assert pool.stats.active_leases == 1
    release.set()
    await asyncio.gather(first_task, second_task)
    assert pool.stats.channel_max_active["news_detail"] == 1
    await pool.close()


@pytest.mark.asyncio
async def test_run_retries_transient_navigation_error_with_recycled_worker():
    playwright = FakePlaywright()
    pool = BrowserWorkerPool(
        PlaywrightTransportConfig(
            workers=1,
            operation_attempts=3,
            retry_backoff_ms=0,
        ),
        playwright_starter=starter_for(playwright),
    )
    attempts = 0

    async def operation(lease):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("Page.goto: net::ERR_CONNECTION_CLOSED")
        return lease.worker_id

    assert await pool.run("forum", operation) == 0
    assert attempts == 2
    assert pool.stats.operation_retries == 1
    assert pool.stats.lease_failures == 1
    assert pool.stats.worker_recycles == 1
    await pool.close()


@pytest.mark.asyncio
async def test_run_does_not_retry_non_transport_error():
    playwright = FakePlaywright()
    pool = BrowserWorkerPool(
        PlaywrightTransportConfig(workers=1, retry_backoff_ms=0),
        playwright_starter=starter_for(playwright),
    )
    attempts = 0

    async def operation(_lease):
        nonlocal attempts
        attempts += 1
        raise ValueError("invalid parsed record")

    with pytest.raises(ValueError, match="invalid parsed record"):
        await pool.run("forum", operation)
    assert attempts == 1
    assert pool.stats.operation_retries == 0
    await pool.close()


@pytest.mark.asyncio
async def test_cancelled_lease_does_not_recycle_or_penalize_proxy():
    playwright = FakePlaywright()
    proxy_pool = ProxyPool(["http://proxy-a"])
    pool = BrowserWorkerPool(
        PlaywrightTransportConfig(workers=1),
        proxy_pool=proxy_pool,
        playwright_starter=starter_for(playwright),
    )
    entered = asyncio.Event()

    async def operation():
        async with pool.lease("news_list"):
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(operation())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert pool.stats.active_leases == 0
    assert pool.stats.lease_cancellations == 1
    assert pool.stats.lease_failures == 0
    assert pool.stats.worker_recycles == 0
    assert proxy_pool.states[0].failures == 0
    await pool.close()
