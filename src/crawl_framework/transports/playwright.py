from __future__ import annotations

import asyncio

from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, TypeVar

from crawl_framework.transports.proxy import ProxyEndpoint, ProxyPool


PlaywrightStarter = Callable[[], Awaitable[Any]]
BrowserOperationResult = TypeVar("BrowserOperationResult")


@dataclass(frozen=True, slots=True)
class PlaywrightTransportConfig:
    workers: int = 1
    headless: bool = True
    browser_type: str = "chromium"
    locale: str = "ko-KR"
    timezone_id: str = "Asia/Seoul"
    viewport: tuple[int, int] = (1500, 1000)
    page_timeout_ms: int = 30_000
    navigation_timeout_ms: int = 45_000
    profile_root: Path | None = None
    recycle_after_scopes: int = 50
    operation_attempts: int = 3
    retry_backoff_ms: int = 500
    launch_args: tuple[str, ...] = ()
    worker_budgets: tuple[tuple[str, int], ...] = ()

    def __post_init__(self) -> None:
        if self.workers < 1:
            raise ValueError("workers must be >= 1")
        if self.recycle_after_scopes < 1:
            raise ValueError("recycle_after_scopes must be >= 1")
        if self.operation_attempts < 1:
            raise ValueError("operation_attempts must be >= 1")
        if self.retry_backoff_ms < 0:
            raise ValueError("retry_backoff_ms must be >= 0")
        if self.page_timeout_ms < 1 or self.navigation_timeout_ms < 1:
            raise ValueError("Playwright timeouts must be >= 1ms")
        width, height = self.viewport
        if width < 1 or height < 1:
            raise ValueError("viewport dimensions must be >= 1")
        for name, count in self.worker_budgets:
            if not str(name).strip() or count < 1:
                raise ValueError("browser worker budgets require a name and count >= 1")


@dataclass(slots=True)
class BrowserPoolStats:
    configured_workers: int
    active_leases: int = 0
    max_active_leases: int = 0
    leases_completed: int = 0
    lease_failures: int = 0
    lease_cancellations: int = 0
    worker_recycles: int = 0
    operation_retries: int = 0
    channel_max_active: dict[str, int] = field(default_factory=dict)


@dataclass(slots=True)
class _BrowserWorker:
    worker_id: int
    browser: Any = None
    context: Any = None
    proxy: ProxyEndpoint | None = None
    scopes_completed: int = 0


async def _default_playwright_starter() -> Any:
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise RuntimeError(
            "Playwright support requires the 'playwright' optional dependency"
        ) from exc
    return await async_playwright().start()


class BrowserContextLease(AbstractAsyncContextManager["BrowserContextLease"]):
    def __init__(self, pool: "BrowserWorkerPool", channel: str) -> None:
        self._pool = pool
        self.channel = channel
        self._channel_acquired = False
        self._channel_active = False
        self._worker: _BrowserWorker | None = None
        self.page: Any = None

    @property
    def worker_id(self) -> int:
        if self._worker is None:
            raise RuntimeError("browser lease has not been entered")
        return self._worker.worker_id

    @property
    def context(self) -> Any:
        if self._worker is None:
            raise RuntimeError("browser lease has not been entered")
        return self._worker.context

    @property
    def proxy(self) -> ProxyEndpoint | None:
        if self._worker is None:
            raise RuntimeError("browser lease has not been entered")
        return self._worker.proxy

    async def __aenter__(self) -> "BrowserContextLease":
        await self._pool._acquire_channel(self.channel)
        self._channel_acquired = True
        try:
            self._worker = await self._pool._acquire_worker()
            self._pool._mark_channel_active(self.channel)
            self._channel_active = True
            self.page = await self._worker.context.new_page()
            self.page.set_default_timeout(self._pool.config.page_timeout_ms)
            self.page.set_default_navigation_timeout(
                self._pool.config.navigation_timeout_ms
            )
        except BaseException as exc:
            if self._worker is not None:
                await self._pool._release_worker(
                    self._worker,
                    failed=not isinstance(exc, asyncio.CancelledError),
                    cancelled=isinstance(exc, asyncio.CancelledError),
                )
            self._pool._release_channel(
                self.channel,
                active=self._channel_active,
            )
            self._channel_acquired = False
            self._channel_active = False
            self._worker = None
            raise
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        worker = self._worker
        if worker is None:
            return
        cancelled = isinstance(exc, asyncio.CancelledError)
        failed = exc is not None and not cancelled
        try:
            if self.page is not None:
                await self.page.close()
        except Exception:
            failed = True
        finally:
            self.page = None
            self._worker = None
            try:
                await self._pool._release_worker(
                    worker,
                    failed=failed,
                    cancelled=cancelled,
                )
            finally:
                if self._channel_acquired:
                    self._pool._release_channel(
                        self.channel,
                        active=self._channel_active,
                    )
                    self._channel_acquired = False
                    self._channel_active = False


class BrowserWorkerPool:
    """Bounded, reusable Playwright browser/context worker pool."""

    def __init__(
        self,
        config: PlaywrightTransportConfig | None = None,
        *,
        proxy_pool: ProxyPool | None = None,
        playwright_starter: PlaywrightStarter | None = None,
    ) -> None:
        self.config = config or PlaywrightTransportConfig()
        self.proxy_pool = proxy_pool
        self._playwright_starter = playwright_starter or _default_playwright_starter
        self._playwright: Any = None
        self._available: asyncio.Queue[_BrowserWorker] = asyncio.Queue(
            maxsize=self.config.workers
        )
        self._workers: list[_BrowserWorker] = []
        self._start_lock = asyncio.Lock()
        self._started = False
        self._closing = False
        self.stats = BrowserPoolStats(configured_workers=self.config.workers)
        self._channel_limits = dict(self.config.worker_budgets)
        self._channel_semaphores = {
            name: asyncio.Semaphore(min(count, self.config.workers))
            for name, count in self._channel_limits.items()
        }
        self._channel_active = {name: 0 for name in self._channel_limits}

    async def __aenter__(self) -> "BrowserWorkerPool":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def start(self) -> None:
        async with self._start_lock:
            if self._started:
                return
            if self._closing:
                raise RuntimeError("browser worker pool is closing")
            self._playwright = await self._playwright_starter()
            try:
                for worker_id in range(self.config.workers):
                    worker = _BrowserWorker(worker_id=worker_id)
                    await self._start_worker(worker)
                    self._workers.append(worker)
                    self._available.put_nowait(worker)
            except BaseException:
                await self._close_workers()
                await self._stop_playwright()
                raise
            self._started = True

    def lease(self, channel: str = "default") -> BrowserContextLease:
        return BrowserContextLease(self, str(channel).strip() or "default")

    async def run(
        self,
        channel: str,
        operation: Callable[[BrowserContextLease], Awaitable[BrowserOperationResult]],
    ) -> BrowserOperationResult:
        """Run a retryable browser operation with a fresh lease per attempt."""
        for attempt in range(self.config.operation_attempts):
            try:
                async with self.lease(channel) as lease:
                    return await operation(lease)
            except Exception as exc:
                if (
                    attempt + 1 >= self.config.operation_attempts
                    or not is_retryable_playwright_error(exc)
                ):
                    raise
                self.stats.operation_retries += 1
                if self.config.retry_backoff_ms:
                    await asyncio.sleep(
                        self.config.retry_backoff_ms * (attempt + 1) / 1_000
                    )
        raise AssertionError("browser retry loop exhausted")

    def channel_capacity(self, channel: str = "default") -> int:
        """Return the effective bounded lease capacity for a named channel."""
        configured = self._channel_limits.get(str(channel).strip() or "default")
        return min(configured, self.config.workers) if configured else self.config.workers

    async def _acquire_channel(self, channel: str) -> None:
        semaphore = self._channel_semaphores.get(channel)
        if semaphore is not None:
            await semaphore.acquire()

    def _mark_channel_active(self, channel: str) -> None:
        self._channel_active[channel] = self._channel_active.get(channel, 0) + 1
        self.stats.channel_max_active[channel] = max(
            self.stats.channel_max_active.get(channel, 0),
            self._channel_active[channel],
        )

    def _release_channel(self, channel: str, *, active: bool) -> None:
        semaphore = self._channel_semaphores.get(channel)
        if active:
            self._channel_active[channel] -= 1
        if semaphore is not None:
            semaphore.release()

    async def close(self) -> None:
        async with self._start_lock:
            if self._closing:
                return
            self._closing = True
            await self._close_workers()
            await self._stop_playwright()
            self._started = False

    async def _acquire_worker(self) -> _BrowserWorker:
        if not self._started:
            await self.start()
        if self._closing:
            raise RuntimeError("browser worker pool is closing")
        worker = await self._available.get()
        self.stats.active_leases += 1
        self.stats.max_active_leases = max(
            self.stats.max_active_leases,
            self.stats.active_leases,
        )
        return worker

    async def _release_worker(
        self,
        worker: _BrowserWorker,
        *,
        failed: bool,
        cancelled: bool = False,
    ) -> None:
        self.stats.active_leases -= 1
        self.stats.leases_completed += 1
        if cancelled:
            self.stats.lease_cancellations += 1
        else:
            worker.scopes_completed += 1
        if cancelled:
            pass
        elif failed:
            self.stats.lease_failures += 1
            if self.proxy_pool is not None and worker.proxy is not None:
                self.proxy_pool.report_failure(worker.proxy)
        elif self.proxy_pool is not None and worker.proxy is not None:
            self.proxy_pool.report_success(worker.proxy)

        recycle = (
            not cancelled
            and (
                failed
                or worker.scopes_completed >= self.config.recycle_after_scopes
            )
        )
        if recycle and not self._closing:
            await self._recycle_worker(worker)
        if not self._closing:
            await self._available.put(worker)
        else:
            self._available.task_done()

    async def _start_worker(self, worker: _BrowserWorker) -> None:
        browser_type = getattr(self._playwright, self.config.browser_type, None)
        if browser_type is None:
            raise ValueError(f"unsupported browser type: {self.config.browser_type!r}")

        worker.proxy = self.proxy_pool.select() if self.proxy_pool is not None else None
        context_options = self._context_options(worker.proxy)
        if self.config.profile_root is not None:
            profile_dir = self.config.profile_root / f"worker-{worker.worker_id:03d}"
            profile_dir.mkdir(parents=True, exist_ok=True)
            worker.context = await browser_type.launch_persistent_context(
                user_data_dir=str(profile_dir),
                headless=self.config.headless,
                args=list(self.config.launch_args),
                **context_options,
            )
            worker.browser = None
        else:
            worker.browser = await browser_type.launch(
                headless=self.config.headless,
                args=list(self.config.launch_args),
            )
            worker.context = await worker.browser.new_context(**context_options)
        worker.scopes_completed = 0

    def _context_options(self, proxy: ProxyEndpoint | None) -> dict[str, Any]:
        width, height = self.config.viewport
        options: dict[str, Any] = {
            "locale": self.config.locale,
            "timezone_id": self.config.timezone_id,
            "viewport": {"width": width, "height": height},
        }
        if proxy is not None:
            options["proxy"] = {"server": proxy.url}
        return options

    async def _recycle_worker(self, worker: _BrowserWorker) -> None:
        await self._close_worker(worker)
        self.stats.worker_recycles += 1
        await self._start_worker(worker)

    async def _close_worker(self, worker: _BrowserWorker) -> None:
        context, browser = worker.context, worker.browser
        worker.context = None
        worker.browser = None
        if context is not None:
            try:
                await context.close()
            except Exception:
                pass
        if browser is not None:
            try:
                await browser.close()
            except Exception:
                pass

    async def _close_workers(self) -> None:
        await asyncio.gather(
            *(self._close_worker(worker) for worker in self._workers),
            return_exceptions=True,
        )
        self._workers.clear()
        while not self._available.empty():
            try:
                self._available.get_nowait()
                self._available.task_done()
            except asyncio.QueueEmpty:
                break

    async def _stop_playwright(self) -> None:
        playwright, self._playwright = self._playwright, None
        if playwright is not None:
            try:
                await playwright.stop()
            except Exception:
                pass


class PlaywrightTransport(BrowserWorkerPool):
    """Compatibility name for the generic browser worker pool transport."""


BrowserLease = BrowserContextLease


def is_retryable_playwright_error(exc: BaseException) -> bool:
    """Classify transient Playwright transport failures without importing it."""
    current: BaseException | None = exc
    visited: set[int] = set()
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        name = type(current).__name__.lower()
        module = type(current).__module__.lower()
        message = str(current).lower()
        if "playwright" in module and name in {"timeouterror", "targetclosederror"}:
            return True
        if any(
            marker in message
            for marker in (
                "net::err_",
                "target page, context or browser has been closed",
                "timeout exceeded",
            )
        ):
            return True
        current = current.__cause__ or current.__context__
    return False


async def find_scroll_parent(locator: Any) -> Any:
    """Return the closest vertically scrollable ancestor for a locator."""
    return await locator.evaluate_handle(
        """(node) => {
          let el = node;
          while (el && el !== document.body) {
            const style = getComputedStyle(el);
            if ((style.overflowY === 'auto' || style.overflowY === 'scroll') &&
                el.scrollHeight > el.clientHeight + 4) return el;
            el = el.parentElement;
          }
          return document.scrollingElement || document.documentElement;
        }"""
    )


async def scroll_element_once(handle: Any) -> dict[str, float]:
    """Advance a virtual-list scroll container by most of one viewport."""
    return await handle.evaluate(
        """(el) => {
          const before = el.scrollTop || 0;
          const client = el.clientHeight || window.innerHeight;
          el.scrollTop = before + Math.max(500, client * 0.88);
          el.dispatchEvent(new Event('scroll', {bubbles: true}));
          return {before, after: el.scrollTop || 0,
                  height: el.scrollHeight || 0, client};
        }"""
    )
