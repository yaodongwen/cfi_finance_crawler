from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from crawl_framework.core.plugin import CrawlCheckpoint, CrawlContext, CrawlScope
from crawl_framework.sites.kabutan import KabutanPlugin


def test_kabutan_plugin_has_month_news_contract():
    plugin = KabutanPlugin()

    assert plugin.site_id == "kabutan"
    assert plugin.country == "JP"
    assert plugin.timezone == "Asia/Tokyo"
    assert plugin.validate_datasets() == ("news_article",)


@pytest.mark.asyncio
async def test_kabutan_rejects_unsupported_dataset():
    plugin = KabutanPlugin()

    with pytest.raises(ValueError, match="unsupported Kabutan dataset"):
        await anext(plugin.discover("forum_post", CrawlContext()))


def test_kabutan_resume_policy_skips_only_old_complete_free_full_month():
    plugin = KabutanPlugin()
    context = CrawlContext(extra={
        "kabutan_mode": "free_full",
        "kabutan_now": datetime(2026, 9, 16, tzinfo=ZoneInfo("Asia/Tokyo")),
    })
    checkpoint = CrawlCheckpoint(state={
        "month_complete": True,
        "archive_access_validated": True,
    })
    old_scope = CrawlScope(
        scope_type="month",
        scope_id="2026-08",
        source_key="202608",
        metadata={"year": 2026, "month": 8},
    )
    current_scope = CrawlScope(
        scope_type="month",
        scope_id="2026-09",
        source_key="202609",
        metadata={"year": 2026, "month": 9},
    )

    assert plugin.is_checkpoint_durable_complete(
        "news_article", old_scope, checkpoint, context
    )
    assert not plugin.is_checkpoint_durable_complete(
        "news_article", current_scope, checkpoint, context
    )
