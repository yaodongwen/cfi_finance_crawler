from typing import AsyncIterator

import pytest

from crawl_framework.core.adapter import (
    AdapterCapability,
    AttachmentRequest,
    CrawlTask,
    RawFetchResult,
    SiteAdapter,
)
from crawl_framework.core.models import (
    CanonicalRecord,
    InstrumentRef,
)
from crawl_framework.core.plugin import (
    CrawlCheckpoint,
    CrawlContext,
    CrawlScope,
)


class DemoAdapter(
    SiteAdapter
):

    @property
    def site_id(
        self,
    ) -> str:
        return "demo"

    @property
    def country(
        self,
    ) -> str:
        return "KR"

    @property
    def timezone(
        self,
    ) -> str:
        return "Asia/Seoul"

    def supported_datasets(
        self,
    ):
        return (
            "forum_post",
        )

    async def discover_instruments(
        self,
        context: CrawlContext,
    ) -> AsyncIterator[
        InstrumentRef
    ]:

        yield InstrumentRef(
            instrument_id="XKRX:005930",
            source_symbol="005930",
            country="KR",
        )

    async def discover_tasks(
        self,
        dataset: str,
        scope: CrawlScope,
        checkpoint: CrawlCheckpoint | None,
        context: CrawlContext,
    ) -> AsyncIterator[
        CrawlTask
    ]:

        yield CrawlTask(
            task_id="task-1",
            source_key="source-1",
            scope=scope,
        )

    async def fetch(
        self,
        dataset: str,
        task: CrawlTask,
        context: CrawlContext,
    ) -> RawFetchResult:

        return RawFetchResult(
            task=task,
            payload={
                "source_id": "1",
                "title": "hello",
            },
        )

    def normalize(
        self,
        dataset: str,
        raw: RawFetchResult,
        scope: CrawlScope,
    ) -> CanonicalRecord | None:

        return CanonicalRecord(
            site_id=self.site_id,
            country=self.country,
            dataset=dataset,
            source_id=raw.payload[
                "source_id"
            ],
            scope_type=scope.scope_type,
            scope_id=scope.scope_id,
            instrument_id=scope.scope_id,
            title=raw.payload[
                "title"
            ],
        )


def test_crawl_task_requires_opaque_ids():

    scope = CrawlScope(
        scope_type="instrument",
        source_key="005930",
        scope_id="XKRX:005930",
    )

    task = CrawlTask(
        task_id=" task-1 ",
        source_key=" source-1 ",
        scope=scope,
    )

    assert (
        task.task_id
        == "task-1"
    )

    assert (
        task.source_key
        == "source-1"
    )


def test_crawl_task_rejects_empty_values():

    scope = CrawlScope(
        scope_type="instrument",
        source_key="005930",
        scope_id="XKRX:005930",
    )

    with pytest.raises(
        ValueError,
        match="task_id",
    ):

        CrawlTask(
            task_id=" ",
            source_key="source",
            scope=scope,
        )


def test_attachment_request_requires_parent_and_url():

    request = AttachmentRequest(
        parent_record_uid=" uid ",
        source_url=" https://example.com/a.pdf ",
    )

    assert (
        request.parent_record_uid
        == "uid"
    )

    assert (
        request.source_url
        == "https://example.com/a.pdf"
    )

    with pytest.raises(
        ValueError,
        match="source_url",
    ):

        AttachmentRequest(
            parent_record_uid="uid",
            source_url=" ",
        )


def test_default_capabilities_come_from_supported_datasets():

    adapter = DemoAdapter()

    assert (
        adapter.capabilities()
        == (
            AdapterCapability(
                dataset="forum_post",
            ),
        )
    )


@pytest.mark.asyncio
async def test_adapter_boundary_round_trip():

    adapter = DemoAdapter()

    instruments = [
        instrument
        async for instrument in adapter.discover_instruments(
            CrawlContext()
        )
    ]

    assert (
        instruments[0].instrument_id
        == "XKRX:005930"
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
        record.record_uid
    )


@pytest.mark.asyncio
async def test_default_attachment_discovery_is_empty():

    adapter = DemoAdapter()

    scope = CrawlScope(
        scope_type="instrument",
        source_key="005930",
        scope_id="XKRX:005930",
    )

    raw = RawFetchResult(
        task=CrawlTask(
            task_id="task",
            source_key="source",
            scope=scope,
        ),
        payload={},
    )

    attachments = [
        item
        async for item in adapter.discover_attachments(
            "forum_post",
            raw,
            CrawlContext(),
        )
    ]

    assert (
        attachments
        == []
    )
