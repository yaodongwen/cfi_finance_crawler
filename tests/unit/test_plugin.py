from typing import Any, AsyncIterator

import pytest

from crawl_framework.core.models import (
    CanonicalRecord,
)
from crawl_framework.core.plugin import (
    CrawlCheckpoint,
    CrawlContext,
    CrawlScope,
    SitePlugin,
)


class DemoPlugin(
    SitePlugin
):

    @property
    def site_id(
        self,
    ) -> str:
        return "demo_site"

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

    def datasets(
        self,
    ):
        return [
            "news_article",
            "forum_post",
        ]

    async def discover(
        self,
        dataset: str,
        ctx: CrawlContext,
    ) -> AsyncIterator[CrawlScope]:

        yield CrawlScope(
            scope_type="instrument",
            scope_id="XKRX:005930",
            source_key="005930",
        )

    async def crawl(
        self,
        dataset: str,
        scope: CrawlScope,
        checkpoint: CrawlCheckpoint,
        ctx: CrawlContext,
    ) -> AsyncIterator[
        dict[str, Any]
    ]:

        yield {
            "id": "123",
            "title": "hello",
        }

    def normalize(
        self,
        dataset: str,
        raw: dict[str, Any],
        scope: CrawlScope,
    ) -> CanonicalRecord | None:

        return CanonicalRecord(
            site_id=self.site_id,
            country=self.country,
            dataset=dataset,
            source_id=raw["id"],
            scope_type=scope.scope_type,
            scope_id=scope.scope_id,
            instrument_id=scope.scope_id,
            title=raw.get(
                "title"
            ),
        )


def test_scope():

    scope = CrawlScope(
        scope_type="instrument",
        scope_id="XKRX:005930",
        source_key="005930",
    )

    assert (
        scope.scope_type
        == "instrument"
    )

    assert (
        scope.scope_id
        == "XKRX:005930"
    )

    assert (
        scope.source_key
        == "005930"
    )


def test_checkpoint():

    checkpoint = CrawlCheckpoint(
        state={
            "page": 100
        }
    )

    assert (
        checkpoint.state["page"]
        == 100
    )


def test_plugin_datasets():

    plugin = DemoPlugin()

    assert (
        plugin.validate_datasets()
        == (
            "news_article",
            "forum_post",
        )
    )

    assert plugin.supports_dataset(
        "forum_post"
    )

    assert not plugin.supports_dataset(
        "comment"
    )


def test_normalize_and_validate():

    plugin = DemoPlugin()

    scope = CrawlScope(
        scope_type="instrument",
        scope_id="XKRX:005930",
        source_key="005930",
    )

    record = (
        plugin.normalize_and_validate(
            "forum_post",
            {
                "id": "123",
                "title": "test",
            },
            scope,
        )
    )

    assert record is not None

    assert (
        record.site_id
        == "demo_site"
    )

    assert (
        record.instrument_id
        == "XKRX:005930"
    )


def test_unsupported_dataset():

    plugin = DemoPlugin()

    scope = CrawlScope(
        scope_type="global",
        source_key="global",
    )

    with pytest.raises(
        ValueError
    ):
        plugin.normalize_and_validate(
            "comment",
            {
                "id": "1"
            },
            scope,
        )


class BadDatasetPlugin(
    DemoPlugin
):

    def datasets(
        self,
    ):
        return [
            "made_up_dataset"
        ]


def test_invalid_plugin_dataset():

    plugin = BadDatasetPlugin()

    with pytest.raises(
        KeyError
    ):
        plugin.validate_datasets()


class BadSitePlugin(
    DemoPlugin
):

    def normalize(
        self,
        dataset: str,
        raw: dict[str, Any],
        scope: CrawlScope,
    ) -> CanonicalRecord | None:

        return CanonicalRecord(
            site_id="wrong_site",
            country="KR",
            dataset=dataset,
            source_id="123",
        )


def test_site_mismatch():

    plugin = BadSitePlugin()

    scope = CrawlScope(
        scope_type="global",
        source_key="global",
    )

    with pytest.raises(
        ValueError
    ):
        plugin.normalize_and_validate(
            "news_article",
            {},
            scope,
        )