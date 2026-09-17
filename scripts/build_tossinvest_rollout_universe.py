from __future__ import annotations

import argparse
import asyncio

from pathlib import Path

from crawl_framework.rollout_universe import write_universe_snapshot
from crawl_framework.sites.tossinvest import TOSS_SCREENER_URL, TossInvestInstrumentClient
from crawl_framework.transports.playwright import BrowserWorkerPool, PlaywrightTransportConfig


DEFAULT_OUTPUT = Path("config/universes/tossinvest_kr_rollout_universe.txt")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the current TossInvest KR universe")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--headful", action="store_true")
    parser.add_argument("--profile-root", default="state/tossinvest_universe_profile")
    return parser.parse_args()


async def collect(args: argparse.Namespace):
    config = PlaywrightTransportConfig(
        workers=1,
        headless=not args.headful,
        profile_root=Path(args.profile_root),
        recycle_after_scopes=10,
        launch_args=("--disable-blink-features=AutomationControlled",),
    )
    async with BrowserWorkerPool(config) as pool:
        return await TossInvestInstrumentClient(pool).discover()


def main() -> int:
    args = parse_args()
    instruments = asyncio.run(collect(args))
    snapshot = write_universe_snapshot(
        args.output,
        [item.instrument_id for item in instruments],
        metadata={
            "site_id": "tossinvest",
            "country": "KR",
            "source": TOSS_SCREENER_URL,
            "generation_method": (
                "TossInvestInstrumentClient real Playwright Screener discovery; "
                "domestic market; protected baseline filters; stable virtual-list order"
            ),
        },
    )
    print(f"wrote {len(snapshot.instrument_ids)} instruments to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
