import pytest

from crawl_framework.core.adapter import (
    AdapterCapability,
    CrawlTask,
    RawFetchResult,
)
from crawl_framework.core.plugin import (
    CrawlCheckpoint,
    CrawlContext,
    CrawlScope,
)
from crawl_framework.sites.naver_finance import (
    NaverFinanceAdapter,
)
from crawl_framework.web.naver.market_sum import (
    NaverListedInstrument,
)


class FakeMarketSumClient:

    def discover_all(
        self,
    ):

        return (
            NaverListedInstrument(
                code="005930",
                name="삼성전자",
                market="KOSPI",
                main_url=(
                    "https://finance.naver.com/"
                    "item/main.naver?code=005930"
                ),
                instrument_id="XKRX:005930",
            ),
            NaverListedInstrument(
                code="035720",
                name="카카오",
                market="KOSDAQ",
                main_url=(
                    "https://finance.naver.com/"
                    "item/main.naver?code=035720"
                ),
                instrument_id="XKRX:035720",
            ),
        )


class FakeNewsClient:

    def __init__(
        self,
    ):

        self.discover_calls = []
        self.fetch_calls = []


    async def discover_tasks(
        self,
        *,
        dataset,
        scope,
        checkpoint,
        context,
    ):

        self.discover_calls.append(
            {
                "dataset": dataset,
                "scope": scope,
                "checkpoint": checkpoint,
                "context": context,
            }
        )

        yield CrawlTask(
            task_id="news:001:000123",
            source_key="001:000123",
            scope=scope,
            metadata={
                "article_id": "000123",
            },
        )


    async def fetch(
        self,
        *,
        dataset,
        task,
        context,
    ):

        self.fetch_calls.append(
            {
                "dataset": dataset,
                "task": task,
                "context": context,
            }
        )

        return {
            "article_id": "000123",
            "office_id": "001",
            "title": "Samsung news",
            "published_at": "2026-08-25T01:00:00+00:00",
            "author": "reporter",
            "content": "news body",
            "url": "https://example.com/news",
            "source": "naver",
        }


class FakeForumClient:

    async def discover_tasks(
        self,
        *,
        dataset,
        scope,
        checkpoint,
        context,
    ):

        yield CrawlTask(
            task_id="forum:005930:12345",
            source_key="12345",
            scope=scope,
            metadata={
                "nid": "12345",
            },
        )


    async def fetch(
        self,
        *,
        dataset,
        task,
        context,
    ):

        return {
            "nid": "12345",
            "title": "삼성전자 게시글",
            "nickname": "tester",
            "written_at": "2026-08-25T01:00:00+00:00",
            "content": "forum body",
            "detail_url": (
                "https://finance.naver.com/"
                "item/board_read.naver?code=005930&nid=12345&page=1"
            ),
        }


class FakeResearchClient:

    async def discover_tasks(
        self,
        *,
        dataset,
        scope,
        checkpoint,
        context,
    ):

        yield CrawlTask(
            task_id="research:RPT-1",
            source_key="RPT-1",
            scope=scope,
        )


    async def fetch(
        self,
        *,
        dataset,
        task,
        context,
    ):

        return {
            "report_id": "RPT-1",
            "title": "Semiconductor outlook",
            "institution": "Demo Securities",
            "analyst": "Analyst A",
            "published_at": "2026-08-25T01:00:00+00:00",
            "summary": "research body",
            "source_url": "https://finance.naver.com/research/RPT-1",
            "pdf_url": "https://finance.naver.com/research/RPT-1.pdf",
            "instrument_ids": [
                "XKRX:005930",
                "XKRX:000660",
            ],
        }


class RecordingLegacyPlugin:

    def __init__(
        self,
    ):

        self.calls = []


    def normalize(
        self,
        dataset,
        payload,
        scope,
    ):

        self.calls.append(
            (
                dataset,
                payload,
                scope,
            )
        )

        return {
            "normalized_dataset": dataset,
            "payload": payload,
        }


def test_naver_adapter_declares_required_discovery_capability():

    adapter = NaverFinanceAdapter(
        market_sum_client=FakeMarketSumClient()
    )

    assert (
        adapter.supported_datasets()
        == (
            "instrument",
            "forum_post",
            "news_article",
            "news_instrument",
            "research_report",
            "research_instrument",
            "attachment",
        )
    )

    assert (
        AdapterCapability(
            dataset="instrument",
            discovers_instruments=True,
            discovers_tasks=False,
        )
        in adapter.capabilities()
    )

    assert (
        AdapterCapability(
            dataset="research_report",
            fetches_attachments=True,
        )
        in adapter.capabilities()
    )

    assert (
        AdapterCapability(
            dataset="attachment",
        )
        in adapter.capabilities()
    )


@pytest.mark.parametrize(
    "dataset",
    (
        "attachment",
        "forum_post",
        "news_article",
        "news_instrument",
        "research_report",
        "research_instrument",
    ),
)
def test_naver_adapter_normalization_delegates_to_production_plugin(
    dataset,
):

    legacy_plugin = RecordingLegacyPlugin()

    adapter = NaverFinanceAdapter(
        market_sum_client=FakeMarketSumClient(),
        legacy_plugin=legacy_plugin,
    )

    scope = CrawlScope(
        scope_type="global",
        source_key="compat",
    )

    payload = {
        "id": "raw-1",
    }

    normalized = adapter.normalize(
        dataset,
        RawFetchResult(
            task=CrawlTask(
                task_id="task-1",
                source_key="raw-1",
                scope=scope,
            ),
            payload=payload,
        ),
        scope,
    )

    assert normalized == {
        "normalized_dataset": dataset,
        "payload": payload,
    }

    assert legacy_plugin.calls == [
        (
            dataset,
            payload,
            scope,
        )
    ]


@pytest.mark.asyncio
async def test_naver_adapter_discovers_canonical_instrument_refs():

    adapter = NaverFinanceAdapter(
        market_sum_client=FakeMarketSumClient()
    )

    instruments = [
        instrument
        async for instrument in adapter.discover_instruments(
            CrawlContext()
        )
    ]

    assert [
        instrument.instrument_id
        for instrument in instruments
    ] == [
        "XKRX:005930",
        "XKRX:035720",
    ]

    assert (
        instruments[0].source_symbol
        == "005930"
    )

    assert (
        instruments[0].name
        == "삼성전자"
    )

    assert (
        instruments[0].market
        == "KOSPI"
    )

    assert (
        instruments[0].country
        == "KR"
    )

    assert (
        instruments[0].source_url
        ==
        (
            "https://finance.naver.com/"
            "item/main.naver?code=005930"
        )
    )


@pytest.mark.asyncio
async def test_naver_news_adapter_discovers_fetches_and_normalizes():

    news_client = FakeNewsClient()

    adapter = NaverFinanceAdapter(
        market_sum_client=FakeMarketSumClient(),
        news_client=news_client,
    )

    scope = CrawlScope(
        scope_type="instrument",
        source_key="005930",
        scope_id="XKRX:005930",
    )

    checkpoint = CrawlCheckpoint(
        {
            "last_article_id": "000100",
        }
    )

    context = CrawlContext()

    tasks = [
        task
        async for task in adapter.discover_tasks(
            "news_article",
            scope,
            checkpoint,
            context,
        )
    ]

    assert (
        len(
            tasks
        )
        == 1
    )

    raw = await adapter.fetch(
        "news_article",
        tasks[0],
        context,
    )

    assert isinstance(
        raw,
        RawFetchResult,
    )

    record = adapter.normalize(
        "news_article",
        raw,
        scope,
    )

    assert record is not None

    assert (
        record.dataset
        == "news_article"
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
        record.relations[0].instrument_id
        == "XKRX:005930"
    )

    assert (
        record.content
        == "news body"
    )


@pytest.mark.asyncio
async def test_naver_news_adapter_requires_news_client_for_tasks():

    adapter = NaverFinanceAdapter(
        market_sum_client=FakeMarketSumClient()
    )

    scope = CrawlScope(
        scope_type="instrument",
        source_key="005930",
        scope_id="XKRX:005930",
    )

    with pytest.raises(
        NotImplementedError,
        match="news_article",
    ):

        [
            task
            async for task in adapter.discover_tasks(
                "news_article",
                scope,
                None,
                CrawlContext(),
            )
        ]


@pytest.mark.asyncio
async def test_naver_forum_adapter_discovers_fetches_and_normalizes():

    adapter = NaverFinanceAdapter(
        market_sum_client=FakeMarketSumClient(),
        forum_client=FakeForumClient(),
    )

    scope = CrawlScope(
        scope_type="instrument",
        source_key="005930",
        scope_id="XKRX:005930",
    )

    tasks = [
        task
        async for task in adapter.discover_tasks(
            "forum_post",
            scope,
            None,
            CrawlContext(),
        )
    ]

    raw = await adapter.fetch(
        "forum_post",
        tasks[0],
        CrawlContext(),
    )

    record = adapter.normalize(
        "forum_post",
        raw,
        scope,
    )

    assert record is not None

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
        record.source_url
        ==
        (
            "https://finance.naver.com/"
            "item/board_read.naver"
            "?code=005930&nid=12345"
        )
    )


@pytest.mark.asyncio
async def test_naver_forum_adapter_requires_forum_client_for_tasks():

    adapter = NaverFinanceAdapter(
        market_sum_client=FakeMarketSumClient()
    )

    scope = CrawlScope(
        scope_type="instrument",
        source_key="005930",
        scope_id="XKRX:005930",
    )

    with pytest.raises(
        NotImplementedError,
        match="forum_post",
    ):

        [
            task
            async for task in adapter.discover_tasks(
                "forum_post",
                scope,
                None,
                CrawlContext(),
            )
        ]


@pytest.mark.asyncio
async def test_naver_research_adapter_discovers_fetches_and_normalizes_metadata():

    adapter = NaverFinanceAdapter(
        market_sum_client=FakeMarketSumClient(),
        research_client=FakeResearchClient(),
    )

    scope = CrawlScope(
        scope_type="global",
        source_key="research",
    )

    tasks = [
        task
        async for task in adapter.discover_tasks(
            "research_report",
            scope,
            None,
            CrawlContext(),
        )
    ]

    raw = await adapter.fetch(
        "research_report",
        tasks[0],
        CrawlContext(),
    )

    record = adapter.normalize(
        "research_report",
        raw,
        scope,
    )

    assert record is not None

    assert (
        record.dataset
        == "research_report"
    )

    assert (
        record.source_id
        == "RPT-1"
    )

    assert (
        record.instrument_id
        is None
    )

    assert [
        relation.instrument_id
        for relation in record.relations
    ] == [
        "XKRX:005930",
        "XKRX:000660",
    ]

    assert (
        record.payload[
            "pdf_url"
        ]
        == "https://finance.naver.com/research/RPT-1.pdf"
    )


def test_naver_research_report_requires_report_id():

    adapter = NaverFinanceAdapter(
        market_sum_client=FakeMarketSumClient()
    )

    scope = CrawlScope(
        scope_type="global",
        source_key="research",
    )

    with pytest.raises(
        ValueError,
        match="report_id",
    ):

        adapter.normalize(
            "research_report",
            RawFetchResult(
                task=CrawlTask(
                    task_id="research",
                    source_key="research",
                    scope=scope,
                ),
                payload={
                    "title": "missing id",
                },
            ),
            scope,
        )


def test_naver_research_instrument_adapter_normalizes_relation():

    adapter = NaverFinanceAdapter(
        market_sum_client=FakeMarketSumClient()
    )

    scope = CrawlScope(
        scope_type="global",
        source_key="research",
    )

    record = adapter.normalize(
        "research_instrument",
        RawFetchResult(
            task=CrawlTask(
                task_id="research:company-123:XKRX:005930",
                source_key="company-123",
                scope=scope,
            ),
            payload={
                "report_id": "company-123",
                "category": "company",
                "instrument_id": "XKRX:005930",
                "stock_code": "005930",
            },
        ),
        scope,
    )

    assert record.dataset == "research_instrument"

    assert record.source_id == (
        "company-123:XKRX:005930"
    )

    assert record.instrument_id == "XKRX:005930"


@pytest.mark.asyncio
async def test_naver_research_pdf_discovery_returns_generic_attachment_request():

    adapter = NaverFinanceAdapter(
        market_sum_client=FakeMarketSumClient()
    )

    scope = CrawlScope(
        scope_type="global",
        source_key="research",
    )

    raw = RawFetchResult(
        task=CrawlTask(
            task_id="research:RPT-1",
            source_key="RPT-1",
            scope=scope,
        ),
        payload={
            "pdf_url": "https://finance.naver.com/research/RPT-1.pdf",
            "pdf_filename": "RPT-1.pdf",
        },
        metadata={
            "parent_record_uid": "record-uid",
        },
    )

    attachments = [
        attachment
        async for attachment in adapter.discover_attachments(
            "research_report",
            raw,
            CrawlContext(),
        )
    ]

    assert (
        len(
            attachments
        )
        == 1
    )

    assert (
        attachments[0].parent_record_uid
        == "record-uid"
    )

    assert (
        attachments[0].source_url
        == "https://finance.naver.com/research/RPT-1.pdf"
    )

    assert (
        attachments[0].mime_type
        == "application/pdf"
    )


@pytest.mark.asyncio
async def test_naver_research_pdf_discovery_requires_parent_record_uid():

    adapter = NaverFinanceAdapter(
        market_sum_client=FakeMarketSumClient()
    )

    scope = CrawlScope(
        scope_type="global",
        source_key="research",
    )

    raw = RawFetchResult(
        task=CrawlTask(
            task_id="research:RPT-1",
            source_key="RPT-1",
            scope=scope,
        ),
        payload={
            "pdf_url": "https://finance.naver.com/research/RPT-1.pdf",
        },
    )

    attachments = [
        attachment
        async for attachment in adapter.discover_attachments(
            "research_report",
            raw,
            CrawlContext(),
        )
    ]

    assert (
        attachments
        == []
    )
