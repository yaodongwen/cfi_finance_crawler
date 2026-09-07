from __future__ import annotations

from typing import AsyncIterator

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
from crawl_framework.sites.naver_finance.plugin import (
    NAVER_FINANCE_COUNTRY,
    NAVER_FINANCE_SITE_ID,
    NAVER_FINANCE_TIMEZONE,
    NaverFinancePlugin,
)
from crawl_framework.web.naver.market_sum import (
    NaverMarketSumClient,
)


class NaverFinanceAdapter(
    SiteAdapter
):
    """
    Naver-specific adapter for discovery/fetch/normalize.
    """

    site_id = NAVER_FINANCE_SITE_ID

    country = NAVER_FINANCE_COUNTRY

    timezone = NAVER_FINANCE_TIMEZONE


    def __init__(
        self,
        *,
        market_sum_client: NaverMarketSumClient | None = None,
        news_client=None,
        forum_client=None,
        research_client=None,
        legacy_plugin: NaverFinancePlugin | None = None,
    ) -> None:

        self.market_sum_client = (
            market_sum_client
            or NaverMarketSumClient()
        )

        self.news_client = news_client

        self.forum_client = forum_client

        self.research_client = research_client

        self.legacy_plugin = (
            legacy_plugin
            or NaverFinancePlugin()
        )


    def supported_datasets(
        self,
    ) -> tuple[
        str,
        ...
    ]:

        return (
            "instrument",
            "forum_post",
            "news_article",
            "news_instrument",
            "research_report",
            "research_instrument",
            "attachment",
        )


    def capabilities(
        self,
    ) -> tuple[
        AdapterCapability,
        ...
    ]:

        return (
            AdapterCapability(
                dataset="instrument",
                discovers_instruments=True,
                discovers_tasks=False,
            ),
            AdapterCapability(
                dataset="forum_post",
            ),
            AdapterCapability(
                dataset="news_article",
            ),
            AdapterCapability(
                dataset="news_instrument",
            ),
            AdapterCapability(
                dataset="research_report",
                fetches_attachments=True,
            ),
            AdapterCapability(
                dataset="research_instrument",
            ),
            AdapterCapability(
                dataset="attachment",
            ),
        )


    async def discover_instruments(
        self,
        context: CrawlContext,
    ) -> AsyncIterator[
        InstrumentRef
    ]:

        for instrument in self.market_sum_client.discover_all():

            yield InstrumentRef(
                instrument_id=instrument.instrument_id,
                source_symbol=instrument.code,
                name=instrument.name,
                market=instrument.market,
                country=self.country,
                source_url=instrument.main_url,
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

        if (
            dataset in {
                "news_article",
                "news_instrument",
            }
            and self.news_client is not None
        ):

            async for task in self.news_client.discover_tasks(
                dataset=dataset,
                scope=scope,
                checkpoint=checkpoint,
                context=context,
            ):

                yield task

            return

        if (
            dataset == "forum_post"
            and self.forum_client is not None
        ):

            async for task in self.forum_client.discover_tasks(
                dataset=dataset,
                scope=scope,
                checkpoint=checkpoint,
                context=context,
            ):

                yield task

            return

        if (
            dataset in {
                "research_report",
                "research_instrument",
            }
            and self.research_client is not None
        ):

            async for task in self.research_client.discover_tasks(
                dataset=dataset,
                scope=scope,
                checkpoint=checkpoint,
                context=context,
            ):

                yield task

            return

        raise NotImplementedError(
            f"Naver task discovery is not implemented "
            f"for dataset {dataset!r}"
        )



    async def fetch(
        self,
        dataset: str,
        task: CrawlTask,
        context: CrawlContext,
    ) -> RawFetchResult:

        if (
            dataset in {
                "news_article",
                "news_instrument",
            }
            and self.news_client is not None
        ):

            payload = await self.news_client.fetch(
                dataset=dataset,
                task=task,
                context=context,
            )

            if isinstance(
                payload,
                RawFetchResult,
            ):

                return payload

            return RawFetchResult(
                task=task,
                payload=payload,
            )

        if (
            dataset == "forum_post"
            and self.forum_client is not None
        ):

            payload = await self.forum_client.fetch(
                dataset=dataset,
                task=task,
                context=context,
            )

            if isinstance(
                payload,
                RawFetchResult,
            ):

                return payload

            return RawFetchResult(
                task=task,
                payload=payload,
            )

        if (
            dataset in {
                "research_report",
                "research_instrument",
            }
            and self.research_client is not None
        ):

            payload = await self.research_client.fetch(
                dataset=dataset,
                task=task,
                context=context,
            )

            if isinstance(
                payload,
                RawFetchResult,
            ):

                return payload

            return RawFetchResult(
                task=task,
                payload=payload,
            )

        raise NotImplementedError(
            f"Naver fetch is not implemented for "
            f"dataset {dataset!r}"
        )


    def normalize(
        self,
        dataset: str,
        raw: RawFetchResult,
        scope: CrawlScope,
    ) -> CanonicalRecord | None:

        if dataset in {
            "attachment",
            "forum_post",
            "news_article",
            "news_instrument",
            "research_report",
            "research_instrument",
        }:

            return self.legacy_plugin.normalize(
                dataset,
                raw.payload,
                scope,
            )

        raise NotImplementedError(
            f"Naver normalization is not implemented "
            f"for dataset {dataset!r}"
        )


    async def discover_attachments(
        self,
        dataset: str,
        raw: RawFetchResult,
        context: CrawlContext,
    ) -> AsyncIterator[
        AttachmentRequest
    ]:

        if dataset != "research_report":

            return

        payload = raw.payload

        if not isinstance(
            payload,
            dict,
        ):

            return

        pdf_url = str(
            payload.get(
                "pdf_url"
            )
            or ""
        ).strip()

        parent_record_uid = str(
            raw.metadata.get(
                "parent_record_uid"
            )
            or payload.get(
                "parent_record_uid"
            )
            or ""
        ).strip()

        if (
            pdf_url
            and parent_record_uid
        ):

            yield AttachmentRequest(
                parent_record_uid=parent_record_uid,
                source_url=pdf_url,
                filename=payload.get(
                    "pdf_filename"
                ),
                mime_type="application/pdf",
                metadata={
                    "site_id": self.site_id,
                    "dataset": dataset,
                },
            )
