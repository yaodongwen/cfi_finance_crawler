from datetime import datetime, timezone

import pytest
import asyncio

from crawl_framework.transports.http import (
    HttpRequest,
    HttpResponse,
    HttpRetryConfig,
    HttpTransport,
)
from crawl_framework.transports.proxy import (
    ProxyPool,
    ProxyPoolConfig,
)
from crawl_framework.transports.rate_limit import (
    AdaptiveRateLimitConfig,
    AdaptiveRateLimiter,
)


def test_proxy_pool_round_robin_selects_available_proxies():

    pool = ProxyPool(
        [
            "http://proxy-a",
            "http://proxy-b",
        ]
    )

    assert (
        pool.select().url
        == "http://proxy-a"
    )

    assert (
        pool.select().url
        == "http://proxy-b"
    )

    assert (
        pool.select().url
        == "http://proxy-a"
    )


def test_proxy_pool_failure_cooldown_skips_endpoint():

    now = datetime(
        2026,
        8,
        27,
        tzinfo=timezone.utc,
    )

    pool = ProxyPool(
        [
            "http://proxy-a",
            "http://proxy-b",
        ],
        config=ProxyPoolConfig(
            failure_cooldown_seconds=60,
        ),
    )

    pool.report_failure(
        "http://proxy-a",
        now=now,
    )

    assert (
        pool.select(
            now=now
        ).url
        == "http://proxy-b"
    )


def test_proxy_pool_success_clears_cooldown():

    now = datetime(
        2026,
        8,
        27,
        tzinfo=timezone.utc,
    )

    pool = ProxyPool(
        [
            "http://proxy-a",
        ],
        config=ProxyPoolConfig(
            failure_cooldown_seconds=60,
        ),
    )

    pool.report_failure(
        "http://proxy-a",
        now=now,
    )

    assert (
        pool.select(
            now=now
        )
        is None
    )

    pool.report_success(
        "http://proxy-a"
    )

    assert (
        pool.select(
            now=now
        ).url
        == "http://proxy-a"
    )


@pytest.mark.asyncio
async def test_http_transport_passes_selected_proxy():

    calls = []

    async def requester(
        url,
        kwargs,
    ):

        calls.append(
            (
                url,
                kwargs,
            )
        )

        return {
            "ok": True,
        }

    transport = HttpTransport(
        requester=requester,
        proxy_pool=ProxyPool(
            [
                "http://proxy-a",
            ]
        ),
    )

    result = await transport.request(
        HttpRequest(
            url="https://example.com",
            params={
                "q": "1",
            },
        )
    )

    assert (
        result
        == {
            "ok": True,
        }
    )

    assert (
        calls[0][1][
            "proxy"
        ]
        == "http://proxy-a"
    )


@pytest.mark.asyncio
async def test_http_transport_passes_form_data():
    calls = []

    async def requester(url, kwargs):
        calls.append((url, kwargs))
        return HttpResponse(200, b"ok", url)

    transport = HttpTransport(requester=requester)
    await transport.request(
        HttpRequest(
            "https://example.com/form",
            method="POST",
            data={"key": "value"},
        )
    )

    assert calls[0][1]["method"] == "POST"
    assert calls[0][1]["data"] == {"key": "value"}


@pytest.mark.asyncio
async def test_http_transport_reports_proxy_failure():

    async def requester(
        url,
        kwargs,
    ):

        raise RuntimeError(
            "boom"
        )

    pool = ProxyPool(
        [
            "http://proxy-a",
        ]
    )

    transport = HttpTransport(
        requester=requester,
        proxy_pool=pool,
    )

    with pytest.raises(
        RuntimeError,
    ):

        await transport.request(
            HttpRequest(
                url="https://example.com"
            )
        )

    assert (
        pool.states[0].failures
        == 1
    )


def test_adaptive_rate_limiter_tracks_throttle_and_circuit():

    now = [
        100.0,
    ]

    sleeps = []

    limiter = AdaptiveRateLimiter(
        AdaptiveRateLimitConfig(
            throttle_delay_seconds=2,
            max_delay_seconds=10,
            failure_threshold=2,
        ),
        sleep=sleeps.append,
        monotonic=lambda: now[0],
    )

    limiter.record_failure(
        "https://example.com/a",
        throttled=True,
    )
    limiter.record_failure(
        "https://example.com/a",
        throttled=True,
    )
    limiter.before_request(
        "https://example.com/a"
    )

    state = limiter.state_for(
        "https://example.com/a"
    )

    assert state.throttles == 2
    assert state.consecutive_failures == 2
    assert sleeps == [8]

    limiter.record_success(
        "https://example.com/a"
    )

    assert state.consecutive_failures == 0
    assert state.current_delay_seconds == 0
    assert state.circuit_open_until is None


@pytest.mark.asyncio
async def test_http_transport_reports_rate_limiter_status():

    class Response:
        status_code = 429

    events = []

    class Limiter:

        def before_request(
            self,
            endpoint,
        ):

            events.append(
                (
                    "before",
                    endpoint,
                )
            )


        def record_failure(
            self,
            endpoint,
            *,
            throttled=False,
        ):

            events.append(
                (
                    "failure",
                    endpoint,
                    throttled,
                )
            )


        def record_success(
            self,
            endpoint,
        ):

            events.append(
                (
                    "success",
                    endpoint,
                )
            )

    async def requester(
        url,
        kwargs,
    ):

        del kwargs
        return Response()

    transport = HttpTransport(
        requester=requester,
        rate_limiter=Limiter(),
    )

    await transport.request(
        HttpRequest(
            url="https://example.com/throttle"
        )
    )

    assert events == [
        (
            "before",
            "https://example.com/throttle",
        ),
        (
            "failure",
            "https://example.com/throttle",
            True,
        ),
    ]


@pytest.mark.asyncio
async def test_http_transport_awaits_adaptive_delay_without_blocking_sleep():
    async_sleeps = []

    def blocking_sleep(delay):
        raise AssertionError(f"synchronous sleep called for {delay}")

    limiter = AdaptiveRateLimiter(
        AdaptiveRateLimitConfig(
            base_delay_seconds=0.25,
        ),
        sleep=blocking_sleep,
    )

    async def requester(url, kwargs):
        del kwargs
        return HttpResponse(200, b"ok", url)

    async def async_sleep(delay):
        async_sleeps.append(delay)
        await asyncio.sleep(0)

    transport = HttpTransport(
        requester=requester,
        rate_limiter=limiter,
        sleep=async_sleep,
    )

    response = await transport.request(
        HttpRequest("https://example.com/nonblocking")
    )

    assert response.status_code == 200
    assert async_sleeps == [0.25]


@pytest.mark.asyncio
async def test_http_transport_retries_configured_statuses():
    statuses = [500, 503, 200]
    sleeps = []

    async def requester(url, kwargs):
        del url, kwargs
        return HttpResponse(
            status_code=statuses.pop(0),
            content=b"ok",
            url="https://example.com",
        )

    transport = HttpTransport(
        requester=requester,
        retry_config=HttpRetryConfig(max_attempts=3, backoff_seconds=0.25),
        sleep=lambda delay: _record_sleep(sleeps, delay),
    )

    response = await transport.request(HttpRequest("https://example.com"))

    assert response.status_code == 200
    assert statuses == []
    assert sleeps == [0.25, 0.5]


async def _record_sleep(values, delay):
    values.append(delay)


@pytest.mark.asyncio
async def test_http_transport_enforces_generic_concurrency_bound():
    active = 0
    maximum = 0
    release = asyncio.Event()

    async def requester(url, kwargs):
        nonlocal active, maximum
        del url, kwargs
        active += 1
        maximum = max(maximum, active)
        await release.wait()
        active -= 1
        return HttpResponse(200, b"ok", "https://example.com")

    transport = HttpTransport(requester=requester, max_concurrency=2)
    tasks = [
        asyncio.create_task(transport.request(HttpRequest(f"https://example.com/{i}")))
        for i in range(5)
    ]
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert maximum == 2
    release.set()
    await asyncio.gather(*tasks)
