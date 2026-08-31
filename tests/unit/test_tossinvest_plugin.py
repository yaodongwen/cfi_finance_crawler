from __future__ import annotations

import pytest

from datetime import (
    datetime,
    timezone,
)

from crawl_framework.core.plugin import (
    CrawlCheckpoint,
    CrawlContext,
    CrawlScope,
)
from crawl_framework.sites.tossinvest import (
    TOSSINVEST_COUNTRY,
    TOSSINVEST_SITE_ID,
    TOSSINVEST_TIMEZONE,
    TossInvestPlugin,
    normalize_toss_symbol,
    toss_instrument_id,
)


UTC = timezone.utc


@pytest.mark.asyncio
async def test_tossinvest_discover_forum_instruments():

    plugin = TossInvestPlugin()

    scopes = [
        scope
        async for scope in plugin.discover(
            "forum_post",
            CrawlContext(
                extra={
                    "instruments": [
                        "A005930",
                        "000660",
                    ]
                }
            ),
        )
    ]

    assert [
        scope.source_key
        for scope in scopes
    ] == [
        "A005930",
        "A000660",
    ]

    assert [
        scope.scope_id
        for scope in scopes
    ] == [
        "XKRX:005930",
        "XKRX:000660",
    ]


@pytest.mark.asyncio
async def test_tossinvest_crawl_uses_fixture_and_checkpoint():

    plugin = TossInvestPlugin()

    scope = CrawlScope(
        scope_type="instrument",
        scope_id="XKRX:005930",
        source_key="A005930",
    )

    rows = [
        row
        async for row in plugin.crawl(
            "forum_post",
            scope,
            CrawlCheckpoint(
                state={
                    "last_source_id": "100",
                }
            ),
            CrawlContext(
                extra={
                    "tossinvest_raw": {
                        "forum_post": {
                            "A005930": [
                                {
                                    "id": "099",
                                },
                                {
                                    "id": "101",
                                },
                            ]
                        }
                    }
                }
            ),
        )
    ]

    assert rows == [
        {
            "id": "101",
        }
    ]


def test_tossinvest_normalize_forum_post():

    plugin = TossInvestPlugin()

    record = plugin.normalize(
        "forum_post",
        {
            "id": "101",
            "symbol": "A005930",
            "created_at": "2026-08-26T09:00:00+09:00",
            "title": "Samsung thread",
            "content": "hello toss",
            "author_id": "u1",
            "author_name": "alice",
            "url": "https://tossinvest.example/posts/101",
        },
        CrawlScope(
            scope_type="instrument",
            scope_id="XKRX:005930",
            source_key="A005930",
        ),
    )

    assert record is not None
    assert record.site_id == TOSSINVEST_SITE_ID
    assert record.country == TOSSINVEST_COUNTRY
    assert record.dataset == "forum_post"
    assert record.instrument_id == "XKRX:005930"
    assert record.event_time == datetime(
        2026,
        8,
        26,
        0,
        0,
        tzinfo=UTC,
    )


def test_tossinvest_normalize_news_article_relations():

    plugin = TossInvestPlugin()

    record = plugin.normalize(
        "news_article",
        {
            "id": "news-1",
            "published_at": "2026-08-26T10:00:00+09:00",
            "title": "Market news",
            "summary": "Samsung and SK Hynix moved.",
            "symbols": [
                "A005930",
                "A000660",
            ],
            "publisher": "Toss News",
        },
        CrawlScope(
            scope_type="global",
            scope_id=None,
            source_key="global_news",
        ),
    )

    assert record is not None
    assert record.dataset == "news_article"
    assert record.instrument_id is None
    assert [
        relation.instrument_id
        for relation in record.relations
    ] == [
        "XKRX:005930",
        "XKRX:000660",
    ]


def test_tossinvest_metadata_and_symbol_helpers():

    plugin = TossInvestPlugin()

    assert plugin.site_id == TOSSINVEST_SITE_ID
    assert plugin.country == TOSSINVEST_COUNTRY
    assert plugin.timezone == TOSSINVEST_TIMEZONE
    assert plugin.validate_datasets() == (
        "forum_post",
        "news_article",
    )
    assert normalize_toss_symbol(
        "A005930"
    ) == "005930"
    assert toss_instrument_id(
        "A005930"
    ) == "XKRX:005930"
