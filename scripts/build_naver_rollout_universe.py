from __future__ import annotations

import argparse
import asyncio

from pathlib import Path

from crawl_framework.core.plugin import (
    CrawlContext,
)
from crawl_framework.rollout_universe import (
    write_universe_snapshot,
)
from crawl_framework.sites.naver_finance import (
    NaverFinanceAdapter,
)
from crawl_framework.web.naver import (
    NAVER_MARKET_SUM_URL,
)


DEFAULT_OUTPUT = (
    Path(
        "config"
    )
    / "universes"
    / "naver_finance_kr_rollout_universe.txt"
)


def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description=(
            "Build deterministic Naver Finance KR "
            "rollout universe snapshot."
        ),
    )

    parser.add_argument(
        "--output",
        default=str(
            DEFAULT_OUTPUT
        ),
        help=(
            "Snapshot output path."
        ),
    )

    return parser.parse_args()


def main() -> int:

    args = parse_args()

    adapter = NaverFinanceAdapter()

    async def discover():

        return [
            instrument
            async for instrument in adapter.discover_instruments(
                CrawlContext()
            )
        ]

    instruments = asyncio.run(
        discover()
    )

    snapshot = write_universe_snapshot(
        args.output,
        [
            instrument.instrument_id
            for instrument in instruments
        ],
        metadata={
            "site_id": "naver_finance",
            "country": "KR",
            "source": NAVER_MARKET_SUM_URL,
            "generation_method": (
                "NaverFinanceAdapter.discover_instruments "
                "using NaverMarketSumClient.discover_all; "
                "KOSPI sosok=0 then KOSDAQ sosok=1; "
                "stable Naver page order"
            ),
        },
    )

    print(
        f"wrote {len(snapshot.instrument_ids)} instruments "
        f"to {args.output}"
    )

    return 0


if __name__ == "__main__":

    raise SystemExit(
        main()
    )
