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
