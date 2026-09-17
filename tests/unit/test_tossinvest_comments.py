from __future__ import annotations

import pytest

from crawl_framework.core.plugin import CrawlCheckpoint, CrawlContext, CrawlScope
from crawl_framework.sites.tossinvest import (
    TossForumCrawlConfig,
    TossInvestPlugin,
    parse_relative_comment_time,
)


def test_forum_config_has_distinct_full_and_incremental_modes():
    assert TossForumCrawlConfig(mode="full").mode == "full"
    assert TossForumCrawlConfig(mode="incremental").history_stop_rounds == 5
    with pytest.raises(ValueError):
        TossForumCrawlConfig(mode="unknown")


class FakeRealForumClient:
    def __init__(self):
        self.calls = []

    async def crawl(self, stock_key, **kwargs):
        self.calls.append((stock_key, kwargs))
        return [{"post_id": "real-1", "content": "real Toss row"}]


@pytest.mark.asyncio
async def test_plugin_crawl_uses_production_forum_client_without_fixture():
    client = FakeRealForumClient()
    plugin = TossInvestPlugin(forum_client=client)
    scope = CrawlScope(
        scope_type="instrument",
        scope_id="XKRX:005930",
        source_key="A005930",
    )
    rows = [
        row async for row in plugin.crawl(
            "forum_post", scope, CrawlCheckpoint(),
            CrawlContext(extra={"forum_mode": "full"}),
        )
    ]
    assert rows == [{"post_id": "real-1", "content": "real Toss row"}]
    assert client.calls[0][0] == "A005930"
    assert client.calls[0][1]["config"].mode == "full"


def test_relative_comment_time_and_full_field_parity():
    event_time, quality = parse_relative_comment_time(
        "8분", "2026-09-07T08:32:27+00:00"
    )
    assert event_time == "2026-09-07T08:24:27+00:00"
    assert quality == "approximate_relative"

    plugin = TossInvestPlugin()
    scope = CrawlScope(
        scope_type="instrument", scope_id="XKRX:005930", source_key="A005930"
    )
    raw = {
        "post_id": "319793428", "author": "ecclesiastic",
        "created_at_text": "8분", "created_at_raw": "8분 · 팔로워 12",
        "crawl_time": "2026-09-07T08:32:27+00:00", "follower_count": 12,
        "is_shareholder": True, "content": "상승인가?", "raw_text": "raw",
        "like_count": 31, "comment_count": 1, "author_profile_image": "https://img",
        "stock_name": "삼성전자", "stock_url": "https://www.tossinvest.com/stocks/A005930/order",
        "stock_key": "A005930",
    }
    record = plugin.normalize("forum_post", raw, scope)
    assert record.author_name == "ecclesiastic"
    assert record.payload["created_at_quality"] == "approximate_relative"
    assert record.payload["follower_count"] == 12
    assert record.payload["is_shareholder"] is True
    assert record.payload["profile_image"] == "https://img"
    assert record.payload["toss_stock_key"] == "A005930"
    assert record.mutation_policy == "latest"

    changed = plugin.normalize("forum_post", {**raw, "like_count": 32}, scope)
    assert changed.record_uid == record.record_uid
    assert changed.version_hash != record.version_hash
