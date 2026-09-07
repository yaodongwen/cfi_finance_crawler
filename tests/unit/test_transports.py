from datetime import datetime, timezone

import pytest

from crawl_framework.transports.http import (
    HttpRequest,
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
    assert sleeps[0] == 4

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
