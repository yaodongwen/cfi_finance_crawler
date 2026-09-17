from __future__ import annotations

from datetime import datetime
from typing import Any, AsyncIterator

from crawl_framework.core.models import (
    CanonicalRecord,
    InstrumentRef,
    RecordRelation,
)
from crawl_framework.core.plugin import (
    CrawlCheckpoint,
    CrawlContext,
    CrawlScope,
    SitePlugin,
)


TOSSINVEST_SITE_ID = "tossinvest"

TOSSINVEST_COUNTRY = "KR"

TOSSINVEST_TIMEZONE = "Asia/Seoul"


def normalize_toss_symbol(
    value: Any,
) -> str:

    text = str(
        value
        or ""
    ).strip()

    if text.upper().startswith(
        "XKRX:"
    ):

        text = text.split(":", 1)[1]

    if text.startswith(
        "A"
    ):

        text = text[1:]

    return text


def toss_instrument_id(
    value: Any,
) -> str | None:

    symbol = normalize_toss_symbol(
        value
    )

    if not symbol:

        return None

    return (
        f"XKRX:{symbol}"
    )


def _raw_items(
    ctx: CrawlContext,
    dataset: str,
    source_key: str,
) -> tuple[dict[str, Any], ...]:

    by_dataset = (
        ctx.extra.get(
            "tossinvest_raw",
            {},
        )
        if ctx.extra
        else {}
    )

    by_scope = by_dataset.get(
        dataset,
        {},
    )

    return tuple(
        by_scope.get(
            source_key,
            (),
        )
    )


class TossInvestPlugin(
    SitePlugin
):
    requires_browser = True
    """
    TossInvest plugin with offline fixture-backed crawling.

    It proves that a structurally different site can use the existing
    runtime/storage/query core without core changes. Real transport can
    later replace the fixture source inside crawl().
    """

    def __init__(self, instrument_client=None, forum_client=None, news_client=None) -> None:
        self.instrument_client = instrument_client
        self.forum_client = forum_client
        self.news_client = news_client

    @property
    def site_id(
        self,
    ) -> str:

        return TOSSINVEST_SITE_ID

    @property
    def country(
        self,
    ) -> str:

        return TOSSINVEST_COUNTRY

    @property
    def timezone(
        self,
    ) -> str:

        return TOSSINVEST_TIMEZONE

    def datasets(
        self,
    ):

        return (
            "forum_post",
            "news_article",
            "news_instrument",
        )

    async def discover(
        self,
        dataset: str,
        ctx: CrawlContext,
    ) -> AsyncIterator[CrawlScope]:

        if dataset not in self.validate_datasets():

            raise ValueError(
                f"unsupported TossInvest dataset: {dataset!r}"
            )

        raw_instruments = (
            ctx.extra.get(
                "instruments",
                ctx.extra.get("instrument_codes", ()),
            )
            if ctx.extra
            else ()
        )

        if not raw_instruments:
            raw_instruments = [
                instrument.source_symbol
                async for instrument in self.discover_instruments(ctx)
            ]

        for value in raw_instruments:

            source_symbol = normalize_toss_symbol(
                value
            )

            instrument_id = toss_instrument_id(
                source_symbol
            )

            if not instrument_id:

                continue

            yield CrawlScope(
                scope_type="instrument",
                scope_id=instrument_id,
                source_key=(
                    "A"
                    +
                    source_symbol
                ),
                metadata={
                    "source_symbol":
                        source_symbol,
                },
            )

    async def discover_instruments(self, ctx: CrawlContext) -> AsyncIterator[InstrumentRef]:
        client = self.instrument_client
        if client is None and ctx.extra:
            client = ctx.extra.get("tossinvest_instrument_client")
        if client is None and ctx.browser is not None:
            from crawl_framework.sites.tossinvest.instruments import TossInvestInstrumentClient

            client = TossInvestInstrumentClient(ctx.browser)
        if client is None:
            raise RuntimeError(
                "Toss instrument discovery requires a TossInvestInstrumentClient "
                "or generic browser pool"
            )
        for item in await client.discover():
            yield item.to_ref()

    async def crawl(
        self,
        dataset: str,
        scope: CrawlScope,
        checkpoint: CrawlCheckpoint,
        ctx: CrawlContext,
    ) -> AsyncIterator[dict[str, Any]]:

        fixture_items = _raw_items(ctx, dataset, scope.source_key)
        if dataset == "forum_post" and not fixture_items:
            client = self.forum_client
            if client is None and ctx.extra:
                client = ctx.extra.get("tossinvest_forum_client")
            if client is None and ctx.browser is not None:
                from crawl_framework.sites.tossinvest.comments import TossInvestForumClient

                client = TossInvestForumClient(ctx.browser)
            if client is None:
                raise RuntimeError("Toss forum crawl requires a real forum client or browser pool")
            from crawl_framework.sites.tossinvest.comments import TossForumCrawlConfig

            extra = ctx.extra or {}
            mode = str(extra.get(
                "forum_mode",
                extra.get("toss_forum_mode", "incremental"),
            ))
            known_ids = set(ctx.extra.get("toss_forum_known_ids", ())) if ctx.extra else set()
            rows = await client.crawl(
                scope.source_key,
                stock_name=str(scope.metadata.get("stock_name", "")),
                stock_url=str(scope.metadata.get("stock_url", "")) or None,
                known_ids=known_ids,
                config=TossForumCrawlConfig(
                    mode=mode,
                    max_scroll_rounds=int(extra.get(
                        "toss_forum_max_scroll_rounds",
                        extra.get("forum_max_pages", 3_000),
                    )),
                    stable_bottom_rounds=int(extra.get("toss_forum_stable_bottom_rounds", 5)),
                    history_stop_rounds=int(extra.get("toss_forum_history_stop_rounds", 5)),
                    scroll_pause_ms=int(extra.get("toss_forum_scroll_pause_ms", 900)),
                ),
            )
            for raw in rows:
                yield raw
            return

        if dataset in {"news_article", "news_instrument"} and not fixture_items:
            client = self.news_client
            if client is None and ctx.extra:
                client = ctx.extra.get("tossinvest_news_client")
            if client is None and ctx.browser is not None:
                from crawl_framework.sites.tossinvest.news import TossInvestNewsClient

                client = TossInvestNewsClient(ctx.browser)
                self.news_client = client
            if client is None:
                raise RuntimeError("Toss news crawl requires a real news client or browser pool")
            from crawl_framework.sites.tossinvest.news import TossNewsListConfig

            extra = ctx.extra or {}
            list_config = TossNewsListConfig(
                    baseline_complete=(
                        False
                        if extra.get("news_mode") == "full"
                        else bool(
                            extra.get(
                                "toss_news_baseline_complete",
                                checkpoint.state.get("news_baseline_complete", False),
                            )
                        )
                    ),
                    max_scroll_rounds=int(extra.get(
                        "toss_news_max_scroll_rounds",
                        extra.get("news_max_pages", 3_000),
                    )),
                    stable_bottom_rounds=int(extra.get("toss_news_stable_bottom_rounds", 5)),
                    history_stop_rounds=int(extra.get("toss_news_history_stop_rounds", 5)),
                    scroll_pause_ms=int(extra.get("toss_news_scroll_pause_ms", 900)),
                )
            common_kwargs = {
                "stock_name": str(scope.metadata.get("stock_name", "")),
                "stock_url": str(scope.metadata.get("stock_url", "")) or None,
                "existing_ids": set(extra.get("toss_news_known_ids", ())),
                "config": list_config,
            }
            if dataset == "news_instrument":
                rows = await client.crawl_relations(
                    scope.source_key,
                    **common_kwargs,
                )
            else:
                rows = await client.crawl_stock(
                    scope.source_key,
                    **common_kwargs,
                    detail_limit=(
                        int(extra["toss_news_detail_limit"])
                        if extra.get("toss_news_detail_limit") is not None
                        else None
                    ),
                    pending_links=list(checkpoint.state.get("pending_news_links", ())),
                )
            for raw in rows:
                yield raw
            return

        last_seen = str(
            checkpoint.state.get(
                "last_source_id",
                ""
            )
        )

        for raw in fixture_items:

            source_id = str(
                raw.get(
                    "id",
                    raw.get(
                        "source_id",
                        "",
                    ),
                )
            ).strip()

            if (
                last_seen
                and
                source_id
                <= last_seen
            ):

                continue

            yield raw

    def normalize(
        self,
        dataset: str,
        raw: dict[str, Any],
        scope: CrawlScope,
    ) -> CanonicalRecord | None:

        if dataset == "forum_post":

            return self._normalize_forum_post(
                raw,
                scope,
            )

        if dataset == "news_article":

            return self._normalize_news_article(
                raw,
                scope,
            )

        if dataset == "news_instrument":
            return self._normalize_news_instrument(raw, scope)

        raise ValueError(
            f"unsupported TossInvest dataset: {dataset!r}"
        )

    def checkpoint_after_record(
        self,
        dataset: str,
        scope: CrawlScope,
        raw: dict[str, Any],
        record: CanonicalRecord,
        checkpoint: CrawlCheckpoint,
        ctx: CrawlContext,
    ) -> CrawlCheckpoint:

        return CrawlCheckpoint(
            state={
                **checkpoint.state,
                "last_source_id":
                    record.source_id,
            }
        )

    def checkpoint_after_scope(
        self,
        dataset: str,
        scope: CrawlScope,
        checkpoint: CrawlCheckpoint,
        ctx: CrawlContext,
    ) -> CrawlCheckpoint:
        if dataset not in {"news_article", "news_instrument"}:
            return checkpoint
        client = self.news_client
        if client is None and ctx.extra:
            client = ctx.extra.get("tossinvest_news_client")
        state = getattr(client, "scope_states", {}).get(scope.source_key, {}) if client else {}
        return CrawlCheckpoint(state={**checkpoint.state, **state})

    def _normalize_forum_post(
        self,
        raw: dict[str, Any],
        scope: CrawlScope,
    ) -> CanonicalRecord | None:

        source_id = str(
            raw.get(
                "id",
                raw.get(
                    "post_id",
                    "",
                ),
            )
        ).strip()

        if not source_id:

            return None

        instrument_id = (
            scope.scope_id
            or
            toss_instrument_id(
                raw.get(
                    "symbol"
                )
            )
        )

        from crawl_framework.sites.tossinvest.comments import parse_relative_comment_time

        event_time = raw.get("created_at")
        created_at_quality = raw.get("created_at_quality")
        if not event_time:
            event_time, created_at_quality = parse_relative_comment_time(
                raw.get("created_at_text"), raw.get("crawl_time")
            )

        return CanonicalRecord(
            site_id=TOSSINVEST_SITE_ID,
            country=TOSSINVEST_COUNTRY,
            dataset="forum_post",
            source_id=source_id,
            scope_type=scope.scope_type,
            scope_id=scope.scope_id,
            instrument_id=instrument_id,
            event_time=event_time,
            updated_at=raw.get(
                "updated_at"
            ),
            title=raw.get(
                "title"
            ),
            content=raw.get(
                "content"
            ),
            author_id=raw.get(
                "author_id"
            ),
            author_name=raw.get("author_name", raw.get("author")),
            source_url=raw.get("url", raw.get("stock_url")),
            payload={
                "symbol": raw.get("symbol", scope.source_key),
                "post_id": source_id,
                "created_at_quality": created_at_quality or "unknown",
                "created_at_text": raw.get("created_at_text"),
                "created_at_raw": raw.get("created_at_raw"),
                "follower_count": raw.get("follower_count"),
                "is_shareholder": bool(raw.get("is_shareholder", False)),
                "raw_text": raw.get("raw_text"),
                "like_count": raw.get("like_count"),
                "comment_count": raw.get("comment_count"),
                "profile_image": raw.get("profile_image", raw.get("author_profile_image")),
                "stock_name": raw.get("stock_name"),
                "stock_url": raw.get("stock_url"),
                "toss_stock_key": raw.get("stock_key", scope.source_key),
            },
            mutation_policy="latest",
        )

    def _normalize_news_article(
        self,
        raw: dict[str, Any],
        scope: CrawlScope,
    ) -> CanonicalRecord | None:

        source_id = str(
            raw.get(
                "id",
                raw.get(
                    "article_id",
                    raw.get("news_id", ""),
                ),
            )
        ).strip()

        if not source_id:

            return None

        symbols = list(raw.get("symbols", ()))
        stock_key = raw.get("stock_key")
        if stock_key and stock_key not in symbols:
            symbols.append(stock_key)
        instruments = [
            toss_instrument_id(
                value
            )
            for value in symbols
        ]

        instruments = [
            value
            for value in instruments
            if value is not None
        ]

        return CanonicalRecord(
            site_id=TOSSINVEST_SITE_ID,
            country=TOSSINVEST_COUNTRY,
            dataset="news_article",
            source_id=source_id,
            scope_type="global",
            scope_id=None,
            instrument_id=(
                instruments[0]
                if len(
                    instruments
                )
                == 1
                else None
            ),
            event_time=raw.get(
                "published_at"
            ),
            updated_at=raw.get(
                "updated_at"
            ),
            title=raw.get(
                "title"
            ),
            content=raw.get("content", raw.get("summary")),
            author_id=raw.get(
                "publisher_id"
            ),
            author_name=raw.get("author") or raw.get("publisher"),
            source_url=raw.get("news_url", raw.get("url")),
            relations=[
                RecordRelation(
                    instrument_id=value,
                    relation_type="mentions",
                )
                for value in instruments
            ],
            payload={
                "symbols": symbols,
                "provider": raw.get("provider", raw.get("publisher")),
                "title_available": bool(raw.get("title_available", raw.get("title"))),
                "title_source": raw.get("title_source"),
                "publisher": raw.get("publisher"),
                "publisher_source": raw.get("publisher_source"),
                "published_date": raw.get("published_date"),
                "published_at_source": raw.get("published_at_source"),
                "author": raw.get("author"),
                "author_source": raw.get("author_source"),
                "raw_text": raw.get("raw_text"),
                "list_text": raw.get("list_text"),
                "news_url": raw.get("news_url"),
            },
            mutation_policy="immutable",
        )

    def _normalize_news_instrument(
        self,
        raw: dict[str, Any],
        scope: CrawlScope,
    ) -> CanonicalRecord | None:
        news_id = str(raw.get("news_id", raw.get("id", ""))).strip()
        instrument_id = scope.scope_id or toss_instrument_id(raw.get("stock_key"))
        if not news_id or not instrument_id:
            return None
        return CanonicalRecord(
            site_id=TOSSINVEST_SITE_ID,
            country=TOSSINVEST_COUNTRY,
            dataset="news_instrument",
            source_id=f"{news_id}:{instrument_id}",
            scope_type="global",
            scope_id=None,
            instrument_id=instrument_id,
            event_time=raw.get("published_at"),
            source_url=raw.get("news_url", raw.get("url")),
            payload={
                "article_source_id": news_id,
                "toss_stock_key": raw.get("stock_key", scope.source_key),
                "relation_evidence": "toss_stock_news_list",
                "relation_schema_version": 2,
            },
            relations=[RecordRelation(
                instrument_id=instrument_id,
                relation_type="primary",
                confidence=1.0,
            )],
            mutation_policy="immutable",
        )
