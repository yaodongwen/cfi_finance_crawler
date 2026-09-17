from __future__ import annotations

import pytest

from pathlib import Path

from crawl_framework.core.plugin import CrawlContext
from crawl_framework.rollout_universe import (
    read_universe_snapshot,
    stage_instruments,
)
from crawl_framework.sites.tossinvest import (
    TossInvestInstrumentClient,
    TossInvestPlugin,
    build_toss_instrument,
)


def test_build_toss_instrument_accepts_only_domestic_stock_keys():
    item = build_toss_instrument(
        stock_key=" A005930 ",
        stock_name=" 삼성전자 ",
        href="/stocks/A005930/order",
        row_text="삼성전자 005930",
    )
    assert item is not None
    assert item.instrument_id == "XKRX:005930"
    assert item.stock_url == "https://www.tossinvest.com/stocks/A005930/order"
    assert item.to_ref().source_symbol == "A005930"
    assert build_toss_instrument(stock_key="NAS0250213012") is None
    assert build_toss_instrument(stock_key="A123") is None


class FakeInstrumentClient:
    async def discover(self):
        return [
            build_toss_instrument(stock_key="A005930", stock_name="삼성전자"),
            build_toss_instrument(stock_key="A000660", stock_name="SK하이닉스"),
        ]


@pytest.mark.asyncio
async def test_plugin_formal_discovery_uses_instrument_client():
    plugin = TossInvestPlugin(instrument_client=FakeInstrumentClient())
    instruments = [
        item async for item in plugin.discover_instruments(CrawlContext())
    ]
    assert [item.instrument_id for item in instruments] == [
        "XKRX:005930",
        "XKRX:000660",
    ]
    scopes = [
        scope
        async for scope in plugin.discover("forum_post", CrawlContext())
    ]
    assert [scope.source_key for scope in scopes] == ["A005930", "A000660"]


def test_screener_client_rejects_invalid_scroll_configuration():
    client = TossInvestInstrumentClient(
        object(), max_scroll_rounds=1, stable_bottom_rounds=1
    )
    assert client.screener_url.endswith("/screener/3")


def test_committed_toss_rollout_universe_is_canonical_and_reproducible():
    path = Path("config/universes/tossinvest_kr_rollout_universe.txt")
    snapshot = read_universe_snapshot(path)
    reread = read_universe_snapshot(path)

    assert snapshot == reread
    assert snapshot.metadata["site_id"] == "tossinvest"
    assert snapshot.metadata["count"] == len(snapshot.instrument_ids)
    assert len(snapshot.instrument_ids) >= 500
    assert len(snapshot.instrument_ids) == len(set(snapshot.instrument_ids))
    assert all(
        value.startswith("XKRX:") and len(value.removeprefix("XKRX:")) == 6
        for value in snapshot.instrument_ids
    )
    assert stage_instruments(snapshot.instrument_ids, 50) == snapshot.instrument_ids[:50]
    assert stage_instruments(snapshot.instrument_ids, 500) == snapshot.instrument_ids[:500]
