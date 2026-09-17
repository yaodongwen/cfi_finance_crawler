from __future__ import annotations

from typing import AsyncIterator

from crawl_framework.core.models import CanonicalRecord
from crawl_framework.core.plugin import (
    CrawlCheckpoint,
    CrawlContext,
    CrawlScope,
    SitePlugin,
)
from crawl_framework.sites.kabutan.market_news import (
    current_tokyo_month,
    parse_month,
    resolve_month_window,
)


KABUTAN_SITE_ID = "kabutan"
KABUTAN_COUNTRY = "JP"
KABUTAN_TIMEZONE = "Asia/Tokyo"
KABUTAN_DATASET = "news_article"


class KabutanPlugin(SitePlugin):
    requires_http = True

    def __init__(self, client=None) -> None:
        self.client = client

    @property
    def site_id(self) -> str:
        return KABUTAN_SITE_ID

    @property
    def country(self) -> str:
        return KABUTAN_COUNTRY

    @property
    def timezone(self) -> str:
        return KABUTAN_TIMEZONE

    def datasets(self) -> tuple[str, ...]:
        return (KABUTAN_DATASET,)

    async def discover(
        self,
        dataset: str,
        ctx: CrawlContext,
    ) -> AsyncIterator[CrawlScope]:
        self._validate_dataset(dataset)
        extra = ctx.extra or {}
        mode = str(extra.get("kabutan_mode", "incremental"))
        if mode == "full":
            mode = "free_full"
        months = resolve_month_window(
            mode=mode,
            start_month=extra.get("kabutan_start_month"),
            end_month=extra.get("kabutan_end_month"),
            overlap_months=int(extra.get("kabutan_overlap_months", 1)),
            now=extra.get("kabutan_now"),
        )

        for month_id in months:
            year, month = parse_month(month_id)
            boundary = False
            if mode == "free_full":
                client = self._resolve_client(ctx)
                boundary = (
                    await client.probe_month(year, month)
                    == "free_access_boundary"
                )
            yield CrawlScope(
                scope_type="month",
                scope_id=month_id,
                source_key=f"{year:04d}{month:02d}",
                metadata={"year": year, "month": month},
            )
            if boundary:
                return

    async def crawl(
        self,
        dataset: str,
        scope: CrawlScope,
        checkpoint: CrawlCheckpoint,
        ctx: CrawlContext,
    ) -> AsyncIterator[dict]:
        self._validate_dataset(dataset)
        client = self._resolve_client(ctx)

        mode = str((ctx.extra or {}).get("kabutan_mode", "incremental"))
        if mode == "full":
            mode = "free_full"
        accepted_checkpoint = (
            (
                checkpoint.state.get("month_complete") is True
                and checkpoint.state.get("archive_access_validated") is True
            )
            or (
                checkpoint.state.get("free_access_complete") is True
                and checkpoint.state.get("stop_reason") == "free_access_boundary"
            )
        )
        current_month = current_tokyo_month((ctx.extra or {}).get("kabutan_now"))
        if accepted_checkpoint and mode == "free_full" and scope.scope_id != current_month:
            return

        year = int(scope.metadata.get("year", scope.source_key[:4]))
        month = int(scope.metadata.get("month", scope.source_key[4:6]))
        start_page = (
            1
            if mode == "incremental" or scope.scope_id == current_month
            else int(checkpoint.state.get("last_completed_page", 0)) + 1
        )
        max_pages = int((ctx.extra or {}).get("kabutan_max_pages_per_month", 500))

        async for raw in client.crawl_month(
            year,
            month,
            start_page=start_page,
            max_pages=max_pages,
            prior_last_news_id=(
                None if mode == "incremental" else checkpoint.state.get("last_news_id")
            ),
        ):
            yield raw

    def is_checkpoint_durable_complete(
        self,
        dataset: str,
        scope: CrawlScope,
        checkpoint: CrawlCheckpoint,
        ctx: CrawlContext,
    ) -> bool:
        self._validate_dataset(dataset)
        mode = str((ctx.extra or {}).get("kabutan_mode", "incremental"))
        if mode == "full":
            mode = "free_full"
        accepted = (
            (
                checkpoint.state.get("month_complete") is True
                and checkpoint.state.get("archive_access_validated") is True
            )
            or (
                checkpoint.state.get("free_access_complete") is True
                and checkpoint.state.get("stop_reason")
                == "free_access_boundary"
            )
        )
        return bool(
            accepted
            and mode == "free_full"
            and scope.scope_id
            != current_tokyo_month((ctx.extra or {}).get("kabutan_now"))
        )

    def normalize(
        self,
        dataset: str,
        raw: dict,
        scope: CrawlScope,
    ) -> CanonicalRecord:
        self._validate_dataset(dataset)
        source_id = str(raw.get("news_id", "")).strip()
        if not source_id:
            raise ValueError("Kabutan record requires news_id")

        return CanonicalRecord(
            site_id=KABUTAN_SITE_ID,
            country=KABUTAN_COUNTRY,
            dataset=KABUTAN_DATASET,
            source_id=source_id,
            scope_type="month",
            scope_id=scope.scope_id,
            identity_scope_type="global",
            identity_scope_id=None,
            instrument_id=None,
            event_time=raw.get("published_at"),
            title=raw.get("title"),
            content=raw.get("content"),
            source_url=raw.get("url"),
            payload={
                "category": str(raw.get("category", "")).strip(),
                "list_time": str(raw.get("list_time", "")).strip(),
                "source_section": "marketnews",
                "access_tier": "free",
                "archive_access": "public",
                "article_found": bool(raw.get("article_found", True)),
                "body_found": bool(raw.get("body_found", True)),
            },
            relations=[],
            mutation_policy="versioned",
        )

    def checkpoint_after_scope(
        self,
        dataset: str,
        scope: CrawlScope,
        checkpoint: CrawlCheckpoint,
        ctx: CrawlContext,
    ) -> CrawlCheckpoint:
        self._validate_dataset(dataset)
        client = self.client
        if client is None and ctx.extra:
            client = ctx.extra.get("kabutan_client")
        state = getattr(client, "scope_states", {}).get(scope.source_key) if client else None
        if not state:
            return checkpoint
        return CrawlCheckpoint(state={**checkpoint.state, **state})

    def _validate_dataset(self, dataset: str) -> None:
        if dataset not in self.validate_datasets():
            raise ValueError(f"unsupported Kabutan dataset: {dataset!r}")

    def _resolve_client(self, ctx: CrawlContext):
        client = self.client
        if client is None and ctx.extra:
            client = ctx.extra.get("kabutan_client")
        if client is None and ctx.http is not None:
            from crawl_framework.sites.kabutan.market_news import KabutanMarketNewsClient

            client = KabutanMarketNewsClient.from_transport(ctx.http)
        if client is None:
            raise RuntimeError("Kabutan crawl requires a KabutanMarketNewsClient")
        self.client = client
        return client
