from datetime import (
    datetime,
    timezone,
)
from pathlib import Path

import pytest

from crawl_framework.core.plugin import (
    CrawlCheckpoint,
    CrawlContext,
    CrawlScope,
)

from crawl_framework.sites.naver_finance import (
    NAVER_FINANCE_SITE_ID,
    NaverFinancePlugin,
)


# ============================================================
# Basic
# ============================================================


def test_site_id():

    plugin = (
        NaverFinancePlugin()
    )

    assert (
        plugin.site_id
        == "naver_finance"
    )

    assert (
        NAVER_FINANCE_SITE_ID
        == "naver_finance"
    )


def test_country():

    plugin = (
        NaverFinancePlugin()
    )

    assert (
        plugin.country
        == "KR"
    )


def test_timezone():

    plugin = (
        NaverFinancePlugin()
    )

    assert (
        plugin.timezone
        == "Asia/Seoul"
    )


def test_datasets():

    plugin = (
        NaverFinancePlugin()
    )

    assert (
        plugin.datasets()
        == (
            "forum_post",
            "news_article",
            "news_instrument",
            "research_report",
            "research_instrument",
            "attachment",
        )
    )


def test_validate_datasets():

    plugin = (
        NaverFinancePlugin()
    )

    datasets = (
        plugin
        .validate_datasets()
    )

    assert (
        "forum_post"
        in datasets
    )

    assert (
        "news_article"
        in datasets
    )

    assert (
        "news_instrument"
        in datasets
    )

    assert (
        "research_report"
        in datasets
    )

    assert (
        "research_instrument"
        in datasets
    )


def test_default_clients_share_plugin_rate_limiter():

    plugin = NaverFinancePlugin()

    assert (
        plugin.forum_client.rate_limiter
        is plugin.rate_limiter
    )

    assert (
        plugin.news_client.rate_limiter
        is plugin.rate_limiter
    )

    assert (
        plugin.research_client.rate_limiter
        is plugin.rate_limiter
    )


# ============================================================
# Instrument id
# ============================================================


def test_instrument_id():

    assert (
        NaverFinancePlugin
        ._instrument_id(
            "005930"
        )
        == "XKRX:005930"
    )


def test_instrument_id_accepts_canonical_id():

    assert (
        NaverFinancePlugin
        ._instrument_id(
            "XKRX:005930"
        )
        == "XKRX:005930"
    )


def test_empty_instrument_rejected():

    with pytest.raises(
        ValueError
    ):

        NaverFinancePlugin._instrument_id(
            ""
        )


@pytest.mark.asyncio
async def test_discover_accepts_canonical_instrument_ids():

    plugin = NaverFinancePlugin()

    scopes = [
        scope
        async for scope in plugin.discover(
            "forum_post",
            CrawlContext(
                extra={
                    "instrument_codes": (
                        "XKRX:005930",
                    )
                }
            ),
        )
    ]

    assert (
        scopes[0].source_key
        == "005930"
    )

    assert (
        scopes[0].scope_id
        == "XKRX:005930"
    )


# ============================================================
# Forum normalize
# ============================================================


def test_normalize_forum_post():

    plugin = (
        NaverFinancePlugin()
    )

    scope = CrawlScope(
        scope_type="instrument",
        source_key="005930",
        scope_id="XKRX:005930",
        metadata={
            "code": "005930",
        },
    )

    raw = {
        "nid": "12345",
        "title": "삼성전자 게시글",
        "nickname": "tester",
        "written_at": datetime(
            2026,
            8,
            25,
            1,
            0,
            tzinfo=timezone.utc,
        ),
        "content": "hello",
        "view_count": 10,
        "recommend": 2,
        "dislike": 1,
        "detail_url": (
            "https://finance.naver.com/"
            "item/board_read.naver"
        ),
    }

    record = plugin.normalize(
        "forum_post",
        raw,
        scope,
    )

    assert (
        record is not None
    )

    assert (
        record.dataset
        == "forum_post"
    )

    assert (
        record.source_id
        == "12345"
    )

    assert (
        record.instrument_id
        == "XKRX:005930"
    )

    assert (
        record.author_name
        == "tester"
    )

    assert (
        record.content
        == "hello"
    )

    assert (
        record.payload
        ==
        {
            "nid": "12345",
            "code": "005930",
        }
    )

    assert (
        record.source_url
        ==
        (
            "https://finance.naver.com/"
            "item/board_read.naver"
            "?code=005930"
            "&nid=12345"
        )
    )
    

def test_forum_post_requires_nid():

    plugin = (
        NaverFinancePlugin()
    )

    scope = CrawlScope(
        scope_type="instrument",
        source_key="005930",
    )

    with pytest.raises(
        ValueError
    ):

        plugin.normalize(
            "forum_post",
            {
                "title": "missing nid"
            },
            scope,
        )


# ============================================================
# News normalize
# ============================================================


def test_normalize_news():

    plugin = (
        NaverFinancePlugin()
    )

    scope = CrawlScope(
        scope_type="instrument",
        source_key="005930",
    )

    raw = {
        "article_id": "000123",
        "office_id": "001",
        "title": "Samsung news",
        "published_at": (
            "2026-08-25T01:00:00+00:00"
        ),
        "author": "reporter",
        "content": "news body",
        "url": "https://example.com/news",
        "source": "naver",
    }

    record = plugin.normalize(
        "news_article",
        raw,
        scope,
    )

    assert (
        record is not None
    )

    assert (
        record.source_id
        == "001:000123"
    )

    assert (
        record.instrument_id
        is None
    )

    assert (
        record.scope_type
        == "global"
    )

    assert (
        record.scope_id
        is None
    )

    assert (
        record.content
        == "news body"
    )

    assert (
        record.author_name
        == "reporter"
    )

    assert (
        [
            relation.instrument_id
            for relation in record.relations
        ]
        == [
            "XKRX:005930"
        ]
    )

    assert (
        record.relations[0].relation_type
        == "primary"
    )

    assert (
        record.payload[
            "office_id"
        ]
        == "001"
    )


def test_normalize_news_same_article_has_stable_global_uid_across_scopes():

    plugin = (
        NaverFinancePlugin()
    )

    raw = {
        "article_id": "000123",
        "office_id": "001",
        "title": "Shared news",
        "published_at": (
            "2026-08-25T01:00:00+00:00"
        ),
        "content": "same body",
        "url": "https://example.com/news",
    }

    samsung = plugin.normalize(
        "news_article",
        raw,
        CrawlScope(
            scope_type="instrument",
            source_key="005930",
            scope_id="XKRX:005930",
        ),
    )

    hynix = plugin.normalize(
        "news_article",
        raw,
        CrawlScope(
            scope_type="instrument",
            source_key="000660",
            scope_id="XKRX:000660",
        ),
    )

    assert (
        samsung.record_uid
        == hynix.record_uid
    )

    assert (
        samsung.version_hash
        != hynix.version_hash
    )

    assert (
        samsung.relations[0].instrument_id
        == "XKRX:005930"
    )

    assert (
        hynix.relations[0].instrument_id
        == "XKRX:000660"
    )


def test_normalize_news_instrument_materializes_relation():

    plugin = (
        NaverFinancePlugin()
    )

    record = plugin.normalize(
        "news_instrument",
        {
            "article_id": "000123",
            "office_id": "001",
            "title": "Samsung relation",
            "canonical_url": (
                "https://n.news.naver.com/"
                "mnews/article/001/000123"
            ),
        },
        CrawlScope(
            scope_type="instrument",
            source_key="005930",
            scope_id="XKRX:005930",
        ),
    )

    assert record.dataset == "news_instrument"

    assert record.source_id == (
        "001:000123:XKRX:005930"
    )

    assert record.instrument_id == "XKRX:005930"

    assert (
        record.payload[
            "article_source_id"
        ]
        == "001:000123"
    )

    assert (
        record.relations[0].relation_type
        == "primary"
    )


def test_normalize_research_report_in_plugin():

    plugin = NaverFinancePlugin()

    record = plugin.normalize(
        "research_report",
        {
            "report_id": "market-123",
            "category": "market",
            "title": "Market outlook",
            "institution": "NH투자증권",
            "analyst": "Analyst",
            "published_at": "2026-08-07",
            "summary": "research body",
            "source_url": (
                "https://finance.naver.com/"
                "research/market_info_read.naver?nid=123"
            ),
            "pdf_url": (
                "https://finance.naver.com/"
                "research/123.pdf"
            ),
            "instrument_ids": [
                "XKRX:005930",
            ],
        },
        CrawlScope(
            scope_type="global",
            source_key="research",
        ),
    )

    assert record.dataset == "research_report"

    assert record.source_id == "market-123"

    assert record.instrument_id == "XKRX:005930"

    assert record.content == "research body"

    assert record.author_name == "Analyst"

    assert record.payload["category"] == "market"

    assert record.payload["pdf_url"] == (
        "https://finance.naver.com/"
        "research/123.pdf"
    )

    assert record.relations[0].instrument_id == (
        "XKRX:005930"
    )


def test_normalize_research_report_global_without_instrument():

    plugin = NaverFinancePlugin()

    record = plugin.normalize(
        "research_report",
        {
            "report_id": "market-123",
            "category": "market",
            "title": "Market outlook",
            "published_at": "2026-08-07",
            "summary": "market body",
        },
        CrawlScope(
            scope_type="global",
            source_key="research",
        ),
    )

    assert record.instrument_id is None

    assert record.relations == []

    assert record.record_uid


def test_normalize_research_instrument_materializes_relation():

    plugin = NaverFinancePlugin()

    record = plugin.normalize(
        "research_instrument",
        {
            "report_id": "company-123",
            "category": "company",
            "instrument_id": "XKRX:005930",
            "stock_code": "005930",
            "stock_name": "삼성전자",
            "title": "Company report",
            "published_at": "2026-08-07",
        },
        CrawlScope(
            scope_type="global",
            source_key="research",
        ),
    )

    assert record.dataset == "research_instrument"

    assert record.source_id == (
        "company-123:XKRX:005930"
    )

    assert record.instrument_id == "XKRX:005930"

    assert record.payload[
        "report_id"
    ] == "company-123"

    assert record.relations[0].relation_type == "related"


def test_normalize_attachment_metadata():

    plugin = NaverFinancePlugin()

    record = plugin.normalize(
        "attachment",
        {
            "attachment_id": "sha",
            "parent_record_uid": "parent",
            "report_id": "market-1",
            "category": "market",
            "source_url": "https://example.com/report.pdf",
            "filename": "report.pdf",
            "mime_type": "application/pdf",
            "sha256": "sha",
            "file_size": 12,
            "local_path": "/tmp/report.pdf",
            "remote_path": "remote/report.pdf",
            "upload_status": "verified",
        },
        CrawlScope(
            scope_type="global",
            source_key="research",
        ),
    )

    assert record.dataset == "attachment"

    assert record.source_id == "sha"

    assert record.payload[
        "parent_record_uid"
    ] == "parent"

    assert record.payload[
        "upload_status"
    ] == "verified"


# ============================================================
# Checkpoint
# ============================================================


def test_forum_checkpoint():

    plugin = (
        NaverFinancePlugin()
    )

    scope = CrawlScope(
        scope_type="instrument",
        source_key="005930",
    )

    raw = {
        "nid": "777",
        "title": "test",
    }

    record = plugin.normalize(
        "forum_post",
        raw,
        scope,
    )

    checkpoint = (
        plugin
        .checkpoint_after_record(
            "forum_post",
            scope,
            raw,
            record,
            None,
            None,
        )
    )

    assert (
        checkpoint.state[
            "last_nid"
        ]
        == "777"
    )

    assert (
        checkpoint.state[
            "dataset"
        ]
        == "forum_post"
    )

    assert (
        checkpoint.state[
            "source_key"
        ]
        == "005930"
    )

def test_news_checkpoint():

    plugin = (
        NaverFinancePlugin()
    )

    scope = CrawlScope(
        scope_type="instrument",
        source_key="005930",
    )

    raw = {
        "article_id": "123",
        "office_id": "001",
        "title": "news",
    }

    record = plugin.normalize(
        "news_article",
        raw,
        scope,
    )

    checkpoint = (
        plugin
        .checkpoint_after_record(
            "news_article",
            scope,
            raw,
            record,
            None,
            None,
        )
    )

    assert (
        checkpoint.state[
            "last_article_id"
        ]
        == "123"
    )

    assert (
        checkpoint.state[
            "last_office_id"
        ]
        == "001"
    )

    assert (
        checkpoint.state[
            "dataset"
        ]
        == "news_article"
    )

    assert (
        checkpoint.state[
            "source_key"
        ]
        == "005930"
    )


def test_research_checkpoint():

    plugin = NaverFinancePlugin()

    scope = CrawlScope(
        scope_type="global",
        source_key="research",
    )

    raw = {
        "report_id": "market-123",
        "category": "market",
        "page": 2,
        "title": "research",
    }

    record = plugin.normalize(
        "research_report",
        raw,
        scope,
    )

    checkpoint = plugin.checkpoint_after_record(
        "research_report",
        scope,
        raw,
        record,
        None,
        None,
    )

    assert checkpoint.state["last_report_id"] == (
        "market-123"
    )

    assert checkpoint.state["category"] == "market"

    assert checkpoint.state["page"] == 2


def test_attachment_checkpoint():

    plugin = NaverFinancePlugin()

    scope = CrawlScope(
        scope_type="global",
        source_key="research",
    )

    raw = {
        "attachment_id": "sha",
        "parent_record_uid": "parent",
        "sha256": "sha",
        "report_id": "market-1",
    }

    record = plugin.normalize(
        "attachment",
        raw,
        scope,
    )

    checkpoint = plugin.checkpoint_after_record(
        "attachment",
        scope,
        raw,
        record,
        None,
        None,
    )

    assert checkpoint.state[
        "last_attachment_id"
    ] == "sha"

    assert checkpoint.state[
        "last_report_id"
    ] == "market-1"

# ============================================================
# Unknown dataset
# ============================================================


def test_unknown_dataset_rejected():

    plugin = (
        NaverFinancePlugin()
    )

    scope = CrawlScope(
        scope_type="instrument",
        source_key="005930",
    )

    with pytest.raises(
        ValueError
    ):

        plugin.normalize(
            "unknown",
            {},
            scope,
        )

# ============================================================
# Fake forum client
# ============================================================


class FakeForumClient:

    def __init__(
        self,
        rows=None,
    ):

        self.rows = (
            rows
            or []
        )

        self.calls = []


    async def crawl_pages(
        self,
        code,
        *,
        start_page=1,
        max_pages=None,
        fetch_detail=True,
    ):

        self.calls.append(
            {
                "code": code,
                "start_page": (
                    start_page
                ),
                "max_pages": (
                    max_pages
                ),
                "fetch_detail": (
                    fetch_detail
                ),
            }
        )

        for row in self.rows:

            yield dict(
                row
            )


class FakeNewsClient:

    def __init__(
        self,
        rows=None,
    ):

        self.rows = (
            rows
            or []
        )

        self.calls = []


    async def crawl_pages(
        self,
        code,
        *,
        start_page=1,
        max_pages=None,
        mode="incremental",
    ):

        self.calls.append(
            {
                "code": code,
                "start_page": (
                    start_page
                ),
                "max_pages": (
                    max_pages
                ),
                "mode": mode,
            }
        )

        for row in self.rows:

            yield dict(
                row
            )


    async def crawl_relation_pages(
        self,
        code,
        *,
        start_page=1,
        max_pages=None,
    ):

        self.calls.append(
            {
                "code": code,
                "start_page": (
                    start_page
                ),
                "max_pages": (
                    max_pages
                ),
                "relation_only": True,
            }
        )

        for row in self.rows:

            yield dict(
                row
            )


class FakeResearchClient:

    def __init__(
        self,
        rows=None,
    ):

        self.rows = (
            rows
            or []
        )

        self.calls = []


    async def crawl_pages(
        self,
        *,
        categories=None,
        start_page=1,
        max_pages=None,
        mode="incremental",
    ):

        self.calls.append(
            {
                "categories": categories,
                "start_page": (
                    start_page
                ),
                "max_pages": (
                    max_pages
                ),
                "mode": mode,
            }
        )

        for row in self.rows:

            yield dict(
                row
            )


    async def crawl_relation_pages(
        self,
        *,
        categories=None,
        start_page=1,
        max_pages=None,
        mode="incremental",
    ):

        self.calls.append(
            {
                "categories": categories,
                "start_page": (
                    start_page
                ),
                "max_pages": (
                    max_pages
                ),
                "mode": mode,
                "relation_only": True,
            }
        )

        for row in self.rows:

            yield dict(
                row
            )


class FakeAttachment:

    attachment_id = "sha"
    parent_record_uid = "parent"
    source_url = "https://example.com/report.pdf"
    filename = "report.pdf"
    mime_type = "application/pdf"
    sha256 = "sha"
    file_size = 12
    local_path = Path(
        "/tmp/report.pdf"
    )


class FakeUploadResult:

    remote_path = "remote/report.pdf"
    status = "verified"


class FakeAttachmentPipeline:

    def __init__(
        self,
    ):

        self.calls = []


    async def process(
        self,
        request,
        *,
        site_id,
        country,
        dataset,
        event_time=None,
        require_pdf=False,
    ):

        self.calls.append(
            {
                "request": request,
                "site_id": site_id,
                "country": country,
                "dataset": dataset,
                "event_time": event_time,
                "require_pdf": require_pdf,
            }
        )

        class Result:
            attachment = FakeAttachment()
            upload_result = FakeUploadResult()

        return Result()


# ============================================================
# Forum crawl
# ============================================================


@pytest.mark.asyncio
async def test_forum_crawl_uses_client():

    client = FakeForumClient(
        rows=[
            {
                "nid": "100",
                "title": "hello",
                "content": "body",
            }
        ]
    )

    plugin = (
        NaverFinancePlugin(
            forum_client=client,
            default_forum_max_pages=2,
        )
    )

    scope = CrawlScope(
        scope_type="instrument",
        source_key="005930",
        scope_id="XKRX:005930",
        metadata={
            "code": "005930",
        },
    )

    rows = []

    async for raw in plugin.crawl(
        "forum_post",
        scope,
        None,
        None,
    ):

        rows.append(
            raw
        )

    assert (
        len(rows)
        == 1
    )

    assert (
        rows[0]["nid"]
        == "100"
    )

    assert (
        rows[0]["code"]
        == "005930"
    )

    assert (
        client.calls[0][
            "code"
        ]
        == "005930"
    )

    assert (
        client.calls[0][
            "start_page"
        ]
        == 1
    )

    assert (
        client.calls[0][
            "max_pages"
        ]
        == 2
    )


@pytest.mark.asyncio
async def test_forum_crawl_resumes_page():

    client = FakeForumClient(
        rows=[]
    )

    plugin = (
        NaverFinancePlugin(
            forum_client=client
        )
    )

    scope = CrawlScope(
        scope_type="instrument",
        source_key="005930",
    )

    checkpoint = CrawlCheckpoint(
        state={
            "page": 7,
            "last_nid": "123",
        }
    )

    async for _ in plugin.crawl(
        "forum_post",
        scope,
        checkpoint,
        None,
    ):

        pass

    assert (
        client.calls[0][
            "start_page"
        ]
        == 7
    )


@pytest.mark.asyncio
async def test_forum_crawl_adds_page():

    client = FakeForumClient(
        rows=[
            {
                "nid": "100",
                "title": "hello",
            }
        ]
    )

    plugin = (
        NaverFinancePlugin(
            forum_client=client
        )
    )

    scope = CrawlScope(
        scope_type="instrument",
        source_key="005930",
    )

    rows = []

    async for raw in plugin.crawl(
        "forum_post",
        scope,
        None,
        None,
    ):

        rows.append(
            raw
        )

    assert (
        rows[0]["page"]
        == 1
    )


def test_forum_start_page_default():

    plugin = (
        NaverFinancePlugin(
            forum_client=FakeForumClient()
        )
    )

    assert (
        plugin._forum_start_page(
            None
        )
        == 1
    )


def test_forum_start_page_from_checkpoint():

    plugin = (
        NaverFinancePlugin(
            forum_client=FakeForumClient()
        )
    )

    checkpoint = CrawlCheckpoint(
        state={
            "page": 9
        }
    )

    assert (
        plugin._forum_start_page(
            checkpoint
        )
        == 9
    )


def test_invalid_default_max_pages():

    with pytest.raises(
        ValueError
    ):

        NaverFinancePlugin(
            forum_client=FakeForumClient(),
            default_forum_max_pages=0,
        )


def test_invalid_default_news_max_pages():

    with pytest.raises(
        ValueError
    ):

        NaverFinancePlugin(
            forum_client=FakeForumClient(),
            news_client=FakeNewsClient(),
            default_news_max_pages=0,
        )


def test_invalid_news_mode():

    with pytest.raises(
        ValueError
    ):

        NaverFinancePlugin(
            forum_client=FakeForumClient(),
            news_client=FakeNewsClient(),
            news_mode="recent",
        )


@pytest.mark.asyncio
async def test_news_crawl_accepts_unbounded_profile_max_pages():

    client = FakeNewsClient(
        rows=[]
    )

    plugin = NaverFinancePlugin(
        forum_client=FakeForumClient(),
        news_client=client,
        default_news_max_pages=1,
    )

    scope = CrawlScope(
        scope_type="instrument",
        source_key="005930",
    )

    async for _ in plugin.crawl(
        "news_article",
        scope,
        None,
        CrawlContext(
            extra={
                "news_max_pages": "all",
            }
        ),
    ):

        pass

    assert client.calls[0][
        "max_pages"
    ] is None


@pytest.mark.asyncio
async def test_news_crawl_uses_real_client_surface():

    client = FakeNewsClient(
        rows=[
            {
                "article_id": "000123",
                "office_id": "001",
                "title": "hello news",
                "content": "body",
            }
        ]
    )

    plugin = NaverFinancePlugin(
        forum_client=FakeForumClient(),
        news_client=client,
        default_news_max_pages=2,
        news_mode="full",
    )

    scope = CrawlScope(
        scope_type="instrument",
        source_key="005930",
        scope_id="XKRX:005930",
        metadata={
            "code": "005930",
        },
    )

    rows = []

    async for raw in plugin.crawl(
        "news_article",
        scope,
        None,
        None,
    ):

        rows.append(
            raw
        )

    assert len(
        rows
    ) == 1

    assert rows[0]["article_id"] == "000123"

    assert rows[0]["code"] == "005930"

    assert rows[0]["page"] == 1

    assert client.calls[0] == {
        "code": "005930",
        "start_page": 1,
        "max_pages": 2,
        "mode": "full",
    }


@pytest.mark.asyncio
async def test_news_crawl_resumes_page_and_context_overrides():

    client = FakeNewsClient(
        rows=[]
    )

    plugin = NaverFinancePlugin(
        forum_client=FakeForumClient(),
        news_client=client,
        default_news_max_pages=1,
    )

    scope = CrawlScope(
        scope_type="instrument",
        source_key="005930",
    )

    checkpoint = CrawlCheckpoint(
        state={
            "page": 7,
            "last_article_id": "000123",
        }
    )

    context = CrawlContext(
        extra={
            "news_max_pages": 5,
            "news_mode": "full",
        }
    )

    async for _ in plugin.crawl(
        "news_article",
        scope,
        checkpoint,
        context,
    ):

        pass

    assert client.calls[0] == {
        "code": "005930",
        "start_page": 7,
        "max_pages": 5,
        "mode": "full",
    }


@pytest.mark.asyncio
async def test_news_instrument_crawl_uses_relation_pages():

    client = FakeNewsClient(
        rows=[
            {
                "article_id": "000123",
                "office_id": "001",
                "title": "hello news",
            }
        ]
    )

    plugin = NaverFinancePlugin(
        forum_client=FakeForumClient(),
        news_client=client,
        default_news_max_pages=2,
    )

    scope = CrawlScope(
        scope_type="instrument",
        source_key="005930",
        scope_id="XKRX:005930",
    )

    rows = [
        row
        async for row in plugin.crawl(
            "news_instrument",
            scope,
            None,
            None,
        )
    ]

    assert rows[0]["article_id"] == "000123"

    assert rows[0]["code"] == "005930"

    assert client.calls[0] == {
        "code": "005930",
        "start_page": 1,
        "max_pages": 2,
        "relation_only": True,
    }


@pytest.mark.asyncio
async def test_research_report_crawl_uses_client():

    client = FakeResearchClient(
        rows=[
            {
                "report_id": "market-123",
                "category": "market",
                "title": "research",
            }
        ]
    )

    plugin = NaverFinancePlugin(
        forum_client=FakeForumClient(),
        news_client=FakeNewsClient(),
        research_client=client,
        default_research_max_pages=2,
        research_mode="full",
    )

    scope = CrawlScope(
        scope_type="global",
        source_key="research",
    )

    context = CrawlContext(
        extra={
            "research_categories": (
                "market",
                "company",
            )
        }
    )

    rows = [
        row
        async for row in plugin.crawl(
            "research_report",
            scope,
            None,
            context,
        )
    ]

    assert rows[0]["report_id"] == "market-123"

    assert rows[0]["page"] == 1

    assert client.calls[0] == {
        "categories": (
            "market",
            "company",
        ),
        "start_page": 1,
        "max_pages": 2,
        "mode": "full",
    }


@pytest.mark.asyncio
async def test_research_instrument_crawl_uses_relation_pages():

    client = FakeResearchClient(
        rows=[
            {
                "report_id": "company-123",
                "category": "company",
                "instrument_id": "XKRX:005930",
            }
        ]
    )

    plugin = NaverFinancePlugin(
        forum_client=FakeForumClient(),
        news_client=FakeNewsClient(),
        research_client=client,
        default_research_max_pages=2,
    )

    scope = CrawlScope(
        scope_type="global",
        source_key="research",
    )

    rows = [
        row
        async for row in plugin.crawl(
            "research_instrument",
            scope,
            None,
            CrawlContext(
                extra={
                    "research_categories": (
                        "company",
                    )
                }
            ),
        )
    ]

    assert rows[0]["report_id"] == "company-123"

    assert rows[0]["page"] == 1

    assert client.calls[0] == {
        "categories": (
            "company",
        ),
        "start_page": 1,
        "max_pages": 2,
        "mode": "incremental",
        "relation_only": True,
    }


@pytest.mark.asyncio
async def test_attachment_crawl_uses_generic_attachment_pipeline():

    research_client = FakeResearchClient(
        rows=[
            {
                "report_id": "market-1",
                "category": "market",
                "title": "research",
                "published_at": "2026-08-07",
                "summary": "body",
                "source_url": (
                    "https://finance.naver.com/"
                    "research/market_info_read.naver?nid=1"
                ),
                "pdf_url": "https://example.com/report.pdf",
                "pdf_filename": "report.pdf",
            },
            {
                "report_id": "market-2",
                "category": "market",
                "title": "research 2",
                "published_at": "2026-08-07",
                "summary": "body",
                "source_url": (
                    "https://finance.naver.com/"
                    "research/market_info_read.naver?nid=2"
                ),
                "pdf_url": "https://example.com/report2.pdf",
                "pdf_filename": "report2.pdf",
            }
        ]
    )

    attachment_pipeline = FakeAttachmentPipeline()

    plugin = NaverFinancePlugin(
        forum_client=FakeForumClient(),
        news_client=FakeNewsClient(),
        research_client=research_client,
        attachment_pipeline=attachment_pipeline,
    )

    scope = CrawlScope(
        scope_type="global",
        source_key="research",
    )

    rows = [
        row
        async for row in plugin.crawl(
            "attachment",
            scope,
            None,
            CrawlContext(
                extra={
                    "research_categories": (
                        "market",
                    ),
                    "attachment_limit": 1,
                }
            ),
        )
    ]

    assert rows[0]["attachment_id"] == "sha"

    assert rows[0]["remote_path"] == "remote/report.pdf"

    assert attachment_pipeline.calls[0][
        "dataset"
    ] == "research_report"

    assert attachment_pipeline.calls[0][
        "require_pdf"
    ] is True

    assert attachment_pipeline.calls[0][
        "request"
    ].source_url == "https://example.com/report.pdf"

    assert len(
        attachment_pipeline.calls
    ) == 1


@pytest.mark.asyncio
async def test_attachment_crawl_respects_download_research_pdf_false():

    research_client = FakeResearchClient(
        rows=[
            {
                "report_id": "market-1",
                "category": "market",
                "title": "research",
                "published_at": "2026-08-07",
                "summary": "body",
                "source_url": (
                    "https://finance.naver.com/"
                    "research/market_info_read.naver?nid=1"
                ),
                "pdf_url": "https://example.com/report.pdf",
                "pdf_filename": "report.pdf",
            }
        ]
    )

    attachment_pipeline = FakeAttachmentPipeline()

    plugin = NaverFinancePlugin(
        forum_client=FakeForumClient(),
        news_client=FakeNewsClient(),
        research_client=research_client,
        attachment_pipeline=attachment_pipeline,
    )

    rows = [
        row
        async for row in plugin.crawl(
            "attachment",
            CrawlScope(
                scope_type="global",
                source_key="research",
            ),
            None,
            CrawlContext(
                extra={
                    "download_research_pdf": False,
                }
            ),
        )
    ]

    assert rows == []
    assert attachment_pipeline.calls == []


@pytest.mark.asyncio
async def test_discover_instruments_from_context_extra():

    plugin = (
        NaverFinancePlugin(
            forum_client=FakeForumClient()
        )
    )

    context = CrawlContext(
        extra={
            "instrument_codes": (
                "005930",
                "000660",
                "042700",
            )
        }
    )

    scopes = []

    async for scope in (
        plugin.discover(
            "forum_post",
            context,
        )
    ):

        scopes.append(
            scope
        )

    assert (
        len(scopes)
        == 3
    )

    assert [
        scope.source_key
        for scope
        in scopes
    ] == [
        "005930",
        "000660",
        "042700",
    ]

    assert [
        scope.scope_id
        for scope
        in scopes
    ] == [
        "XKRX:005930",
        "XKRX:000660",
        "XKRX:042700",
    ]

@pytest.mark.asyncio
async def test_discover_single_instrument():

    plugin = (
        NaverFinancePlugin(
            forum_client=FakeForumClient()
        )
    )

    context = CrawlContext(
        extra={
            "instrument_codes": (
                "005930",
            )
        }
    )

    scopes = []

    async for scope in plugin.discover(
        "forum_post",
        context,
    ):

        scopes.append(
            scope
        )

    assert (
        len(scopes)
        == 1
    )

    assert (
        scopes[0].source_key
        == "005930"
    )

    assert (
        scopes[0].scope_id
        == "XKRX:005930"
    )

    assert (
        scopes[0].metadata[
            "code"
        ]
        == "005930"
    )

def test_forum_post_dynamic_metrics_do_not_change_version_hash():

    from crawl_framework.core.plugin import (
        CrawlScope,
    )

    from crawl_framework.sites.naver_finance.plugin import (
        NaverFinancePlugin,
    )

    plugin = NaverFinancePlugin()

    scope = CrawlScope(
        scope_type="instrument",
        source_key="042700",
        scope_id="XKRX:042700",
    )

    raw_old = {
        "nid": "428482953",
        "code": "042700",
        "title": "test title",
        "content": "same content",
        "nickname": "tester",
        "written_at": "2026.08.26 14:00",
        "view_count": 25,
        "recommend": 1,
        "dislike": 0,
        "detail_url": (
            "https://finance.naver.com/"
            "item/board_read.naver"
            "?code=042700"
            "&nid=428482953"
            "&page=1"
        ),
        "page": 1,
    }

    raw_new = {
        "nid": "428482953",
        "code": "042700",
        "title": "test title",
        "content": "same content",
        "nickname": "tester",
        "written_at": "2026.08.26 14:00",
        "view_count": 999,
        "recommend": 100,
        "dislike": 50,
        "detail_url": (
            "https://finance.naver.com/"
            "item/board_read.naver"
            "?code=042700"
            "&nid=428482953"
            "&page=7"
        ),
        "page": 7,
    }

    record_old = plugin.normalize(
        "forum_post",
        raw_old,
        scope,
    )

    record_new = plugin.normalize(
        "forum_post",
        raw_new,
        scope,
    )

    assert record_old is not None

    assert record_new is not None

    assert (
        record_old.record_uid
        ==
        record_new.record_uid
    )

    assert (
        record_old.version_hash
        ==
        record_new.version_hash
    )

    assert (
        record_old.payload
        ==
        {
            "nid": "428482953",
            "code": "042700",
        }
    )

    assert (
        record_new.payload
        ==
        record_old.payload
    )

    assert (
        record_old.source_url
        ==
        (
            "https://finance.naver.com/"
            "item/board_read.naver"
            "?code=042700"
            "&nid=428482953"
        )
    )

def test_forum_post_content_change_changes_version_hash():

    from crawl_framework.core.plugin import (
        CrawlScope,
    )

    from crawl_framework.sites.naver_finance.plugin import (
        NaverFinancePlugin,
    )

    plugin = NaverFinancePlugin()

    scope = CrawlScope(
        scope_type="instrument",
        source_key="042700",
        scope_id="XKRX:042700",
    )

    raw_old = {
        "nid": "428482953",
        "code": "042700",
        "title": "same title",
        "content": "old content",
        "nickname": "tester",
        "written_at": "2026.08.26 14:00",
        "view_count": 10,
    }

    raw_new = {
        "nid": "428482953",
        "code": "042700",
        "title": "same title",
        "content": "new content",
        "nickname": "tester",
        "written_at": "2026.08.26 14:00",
        "view_count": 10,
    }

    record_old = plugin.normalize(
        "forum_post",
        raw_old,
        scope,
    )

    record_new = plugin.normalize(
        "forum_post",
        raw_new,
        scope,
    )

    assert record_old is not None

    assert record_new is not None

    assert (
        record_old.record_uid
        ==
        record_new.record_uid
    )

    assert (
        record_old.version_hash
        !=
        record_new.version_hash
    )
