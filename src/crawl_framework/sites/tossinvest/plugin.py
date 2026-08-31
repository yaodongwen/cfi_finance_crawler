from __future__ import annotations

from datetime import datetime
from typing import Any, AsyncIterator

from crawl_framework.core.models import (
    CanonicalRecord,
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
    """
    TossInvest plugin with offline fixture-backed crawling.

    It proves that a structurally different site can use the existing
    runtime/storage/query core without core changes. Real transport can
    later replace the fixture source inside crawl().
    """

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
                ()
            )
            if ctx.extra
            else ()
        )

        if dataset == "news_article":

            yield CrawlScope(
                scope_type="global",
                source_key="global_news",
                scope_id=None,
            )

            return

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

    async def crawl(
        self,
        dataset: str,
        scope: CrawlScope,
        checkpoint: CrawlCheckpoint,
        ctx: CrawlContext,
    ) -> AsyncIterator[dict[str, Any]]:

        last_seen = str(
            checkpoint.state.get(
                "last_source_id",
                ""
            )
        )

        for raw in _raw_items(
            ctx,
            dataset,
            scope.source_key,
        ):

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

        return CanonicalRecord(
            site_id=TOSSINVEST_SITE_ID,
            country=TOSSINVEST_COUNTRY,
            dataset="forum_post",
            source_id=source_id,
            scope_type=scope.scope_type,
            scope_id=scope.scope_id,
            instrument_id=instrument_id,
            event_time=raw.get(
                "created_at"
            ),
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
            author_name=raw.get(
                "author_name"
            ),
            source_url=raw.get(
                "url"
            ),
            payload={
                "symbol":
                    raw.get(
                        "symbol",
                        scope.source_key,
                    ),
                "like_count":
                    raw.get(
                        "like_count"
                    ),
                "comment_count":
                    raw.get(
                        "comment_count"
                    ),
            },
            mutation_policy="immutable",
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
                    "",
                ),
            )
        ).strip()

        if not source_id:

            return None

        instruments = [
            toss_instrument_id(
                value
            )
            for value in raw.get(
                "symbols",
                (),
            )
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
            scope_type=scope.scope_type,
            scope_id=scope.scope_id,
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
            content=raw.get(
                "summary",
                raw.get(
                    "content"
                ),
            ),
            author_id=raw.get(
                "publisher_id"
            ),
            author_name=raw.get(
                "publisher"
            ),
            source_url=raw.get(
                "url"
            ),
            relations=[
                RecordRelation(
                    instrument_id=value,
                    relation_type="mentions",
                )
                for value in instruments
            ],
            payload={
                "symbols":
                    raw.get(
                        "symbols",
                        [],
                    ),
                "provider":
                    raw.get(
                        "provider"
                    ),
            },
            mutation_policy="immutable",
        )
