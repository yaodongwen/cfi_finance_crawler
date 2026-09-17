#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from crawl_framework.sites.hkexnews import HKEXFilingsClient
from crawl_framework.transports.http import (
    HttpRetryConfig,
    HttpTransport,
    RequestsHttpRequester,
)


def read_snapshot(path: Path, limit: int, offset: int = 0) -> list[str]:
    values = [
        line.strip() for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]
    return values[offset:offset + limit]


async def run(path: Path, limit: int, date_from: str, offset: int = 0) -> dict:
    transport = HttpTransport(
        requester=RequestsHttpRequester(timeout_seconds=30, pool_size=1),
        retry_config=HttpRetryConfig(max_attempts=3, backoff_seconds=1.0),
        max_concurrency=1,
    )
    client = HKEXFilingsClient.from_transport(transport)
    date_to = datetime.now(ZoneInfo("Asia/Hong_Kong")).strftime("%Y%m%d")
    rows = []
    totals = {"annual": 0, "interim": 0, "quarterly": 0}
    for instrument in read_snapshot(path, limit, offset):
        code = instrument.split(":", 1)[-1]
        try:
            stock = await client.resolve_stock(code)
            if stock is None:
                rows.append({"instrument": instrument, "error": "not_found"})
                continue
            counts = {}
            for report_type in totals:
                reports = await client.search_reports(
                    stock.stock_id,
                    [report_type],
                    date_from=date_from,
                    date_to=date_to,
                )
                counts[report_type] = len(reports)
                totals[report_type] += len(reports)
            rows.append({"instrument": instrument, "counts": counts})
        except Exception as exc:
            rows.append({"instrument": instrument, "error": repr(exc)})
    return {"instruments": rows, "totals": totals, "pdf_downloaded": False}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--universe",
        type=Path,
        default=Path("config/universes/hkex_hk_rollout_universe.txt"),
    )
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--date-from", default="20240101")
    args = parser.parse_args()
    print(json.dumps(
        asyncio.run(run(args.universe, args.limit, args.date_from, args.offset)),
        indent=2,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
