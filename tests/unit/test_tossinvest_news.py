import asyncio

from urllib.parse import quote

import pytest

from crawl_framework.core.plugin import CrawlCheckpoint, CrawlContext, CrawlScope
from crawl_framework.sites.tossinvest import TossInvestPlugin, TossNewsListConfig, parse_news_id
from crawl_framework.sites.tossinvest.news import (
    TossInvestNewsClient,
    _is_generic_title,
    extract_structured_news_metadata,
    published_date_from_news_id,
    publisher_from_news_id,
)


def test_parse_news_id_prefers_content_params_id():
    url = "https://www.tossinvest.com/news?contentType=news&contentParams=" + quote(
        '{"id":"news-123"}'
    )
    assert parse_news_id(url) == "news-123"


def test_parse_news_id_has_stable_url_hash_fallback():
    url = "https://www.tossinvest.com/news/no-id"
    assert parse_news_id(url) == parse_news_id(url)
    assert len(parse_news_id(url)) == 64


def test_news_id_date_is_stable_for_article_and_relation_records():
    assert published_date_from_news_id("hankyung_X20260908.115227") == "2026-09-08"
    assert published_date_from_news_id("news-without-date") == ""


def test_historical_baseline_and_incremental_are_distinct():
    full = TossNewsListConfig(baseline_complete=False)
    incremental = TossNewsListConfig(baseline_complete=True)
    assert full.baseline_complete is False
    assert incremental.baseline_complete is True
    assert incremental.history_stop_rounds == 5


class FakeNewsClient:
    def __init__(self):
        self.scope_states = {}

    async def crawl_stock(self, stock_key, **kwargs):
        assert stock_key == "A005930"
        assert kwargs["config"].baseline_complete is False
        return [{"news_id": "n1", "news_url": "https://example/news/n1"}]


@pytest.mark.asyncio
async def test_production_plugin_uses_real_news_list_client():
    plugin = TossInvestPlugin(news_client=FakeNewsClient())
    scope = CrawlScope(
        scope_type="instrument", scope_id="XKRX:005930", source_key="A005930"
    )
    rows = [
        row async for row in plugin.crawl(
            "news_article", scope, CrawlCheckpoint(), CrawlContext()
        )
    ]
    assert rows[0]["news_id"] == "n1"


@pytest.mark.asyncio
async def test_full_news_mode_disables_checkpoint_incremental_stop():
    client = FakeNewsClient()
    plugin = TossInvestPlugin(news_client=client)
    scope = CrawlScope(
        scope_type="instrument",
        scope_id="XKRX:005930",
        source_key="A005930",
    )
    checkpoint = CrawlCheckpoint(
        state={"news_baseline_complete": True}
    )

    rows = [
        row async for row in plugin.crawl(
            "news_article",
            scope,
            checkpoint,
            CrawlContext(extra={"news_mode": "full"}),
        )
    ]

    assert rows[0]["news_id"] == "n1"


@pytest.mark.asyncio
async def test_incremental_news_mode_preserves_completed_baseline():
    received = {}

    class ConfigClient:
        scope_states = {}

        async def crawl_stock(self, stock_key, **kwargs):
            received["config"] = kwargs["config"]
            return []

    plugin = TossInvestPlugin(news_client=ConfigClient())
    scope = CrawlScope(
        scope_type="instrument",
        scope_id="XKRX:005930",
        source_key="A005930",
    )

    rows = [
        row async for row in plugin.crawl(
            "news_article",
            scope,
            CrawlCheckpoint(
                state={"news_baseline_complete": True}
            ),
            CrawlContext(extra={"news_mode": "incremental"}),
        )
    ]

    assert rows == []
    assert received["config"].baseline_complete is True


class ResumeNewsClient:
    def __init__(self):
        self.scope_states = {}
        self.pending_received = None

    async def crawl_stock(self, stock_key, **kwargs):
        self.pending_received = kwargs["pending_links"]
        self.scope_states[stock_key] = {
            "news_baseline_complete": False,
            "pending_news_links": [{"news_id": "still-pending", "news_url": "u2"}],
            "pending_news_count": 1,
        }
        return []


@pytest.mark.asyncio
async def test_pending_news_links_resume_and_scope_checkpoint_without_records():
    client = ResumeNewsClient()
    plugin = TossInvestPlugin(news_client=client)
    scope = CrawlScope(scope_type="instrument", scope_id="XKRX:005930", source_key="A005930")
    checkpoint = CrawlCheckpoint(state={
        "news_baseline_complete": False,
        "pending_news_links": [{"news_id": "retry-me", "news_url": "u1"}],
    })
    rows = [row async for row in plugin.crawl("news_article", scope, checkpoint, CrawlContext())]
    assert rows == []
    assert client.pending_received[0]["news_id"] == "retry-me"
    updated = plugin.checkpoint_after_scope("news_article", scope, checkpoint, CrawlContext())
    assert updated.state["pending_news_count"] == 1
    assert updated.state["pending_news_links"][0]["news_id"] == "still-pending"


def test_known_ids_do_not_enable_incremental_before_baseline_complete():
    full = TossNewsListConfig(baseline_complete=False)
    incremental = TossNewsListConfig(baseline_complete=True)
    existing_ids = {f"known-{i}" for i in range(700)}
    assert (full.baseline_complete and bool(existing_ids)) is False
    assert (incremental.baseline_complete and bool(existing_ids)) is True


def test_structured_news_metadata_prefers_exact_news_object():
    data = {"items": [{
        "id": "n1", "title": "정확한 제목", "content": "본문",
        "publisher": {"name": "연합뉴스"},
        "publishedAt": "2026-09-07T10:00:00+09:00",
        "author": {"name": "홍길동"},
    }]}
    metadata = extract_structured_news_metadata(data, "n1")
    assert metadata == {
        "title": "정확한 제목", "publisher": "연합뉴스",
        "published_at": "2026-09-07T10:00:00+09:00", "author": "홍길동",
        "title_source": "page_json_exact",
    }


def test_publisher_prefix_fallback_is_auditable():
    assert publisher_from_news_id("maekyung_0000123") == "매일경제"
    assert publisher_from_news_id("newspim_20260907001016") == "뉴스핌"
    assert publisher_from_news_id("unknown_123") == ""


def test_toss_navigation_heading_is_not_an_article_title():
    assert _is_generic_title("주요 뉴스") is True


def test_news_normalization_preserves_detail_provenance_and_relation():
    plugin = TossInvestPlugin()
    scope = CrawlScope(scope_type="instrument", scope_id="XKRX:005930", source_key="A005930")
    record = plugin.normalize("news_article", {
        "news_id": "n1", "stock_key": "A005930", "title": "제목",
        "title_available": True, "title_source": "page_json_exact",
        "publisher": "연합뉴스", "publisher_source": "page_json",
        "published_at": "2026-09-07T10:00:00+09:00", "published_date": "2026-09-07",
        "published_at_source": "page_json", "author": "홍길동",
        "author_source": "page_json", "content": "본문", "raw_text": "raw",
        "list_text": "list", "news_url": "https://toss/news/n1",
    }, scope)
    assert record.source_id == "n1"
    assert record.relations[0].instrument_id == "XKRX:005930"
    assert record.payload["title_source"] == "page_json_exact"
    assert record.payload["author_source"] == "page_json"


class BoundedDetailPool:
    def channel_capacity(self, channel):
        assert channel == "news_detail"
        return 2


class ConcurrentDetailClient(TossInvestNewsClient):
    def __init__(self):
        super().__init__(BoundedDetailPool())
        self.active = 0
        self.max_active = 0
        self.release = asyncio.Event()

    async def collect_links(self, stock_key, **kwargs):
        return [
            {"news_id": str(index), "news_url": f"https://example/{index}"}
            for index in range(5)
        ]

    async def parse_article(self, stock, item):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        if self.max_active == 2:
            self.release.set()
        await self.release.wait()
        await asyncio.sleep(0)
        self.active -= 1
        if item["news_id"] == "3":
            return None
        return {**item, **stock}


@pytest.mark.asyncio
async def test_news_details_use_bounded_parallel_workers_and_preserve_pending():
    client = ConcurrentDetailClient()
    rows = await client.crawl_stock("A005930")

    assert client.max_active == 2
    assert [row["news_id"] for row in rows] == ["0", "1", "2", "4"]
    assert client.scope_states["A005930"]["pending_news_count"] == 1
    assert client.scope_states["A005930"]["pending_news_links"][0]["news_id"] == "3"


class RelationListClient(TossInvestNewsClient):
    def __init__(self):
        super().__init__(BoundedDetailPool())
        self.detail_calls = 0

    async def collect_links(self, stock_key, **kwargs):
        self.list_states[stock_key] = {"reached_bottom": True}
        return [{
            "news_id": "shared-news",
            "news_url": "https://example/shared-news",
            "list_text": "headline",
            "stock_key": stock_key,
        }]

    async def parse_article(self, stock, item):
        self.detail_calls += 1
        raise AssertionError("relation discovery must not fetch article detail")


@pytest.mark.asyncio
async def test_news_relations_use_list_evidence_without_detail_fetch():
    client = RelationListClient()
    rows = await client.crawl_relations("A005930")

    assert client.detail_calls == 0
    assert rows[0]["news_id"] == "shared-news"
    assert rows[0]["stock_key"] == "A005930"
    assert rows[0]["published_at"] == ""
    assert client.scope_states["A005930"]["news_baseline_complete"] is True


class RunBoundaryPool:
    def __init__(self, result):
        self.result = result
        self.channels = []

    async def run(self, channel, operation):
        self.channels.append((channel, operation))
        return self.result


@pytest.mark.asyncio
async def test_real_news_list_client_uses_generic_retry_boundary():
    expected = [{"news_id": "n1"}]
    pool = RunBoundaryPool(expected)
    client = TossInvestNewsClient(pool)

    assert await client.collect_links("A005930") == expected
    assert [channel for channel, _ in pool.channels] == ["news_list"]


@pytest.mark.asyncio
async def test_real_news_detail_client_uses_generic_retry_boundary():
    expected = {"news_id": "n1", "content": "body"}
    pool = RunBoundaryPool(expected)
    client = TossInvestNewsClient(pool)

    assert await client.parse_article(
        {"stock_key": "A005930"},
        {"news_id": "n1", "news_url": "https://example.invalid/n1"},
    ) == expected
    assert [channel for channel, _ in pool.channels] == ["news_detail"]
