from datetime import (
    datetime,
    timezone,
)

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


def test_empty_instrument_rejected():

    with pytest.raises(
        ValueError
    ):

        NaverFinancePlugin._instrument_id(
            ""
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
        == "XKRX:005930"
    )

    assert (
        record.payload[
            "office_id"
        ]
        == "001"
    )


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

