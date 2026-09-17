#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo

from crawl_framework.core.plugin import CrawlScope
from crawl_framework.sites.hkexnews import HKEXFilingsClient, HKEXNewsPlugin
from crawl_framework.transports.http import (
    HttpRetryConfig,
    HttpTransport,
    RequestsHttpRequester,
)


async def run(instrument: str, date_from: str) -> dict:
    transport = HttpTransport(
        requester=RequestsHttpRequester(timeout_seconds=30, pool_size=1),
        retry_config=HttpRetryConfig(max_attempts=3, backoff_seconds=1.0),
        max_concurrency=1,
    )
    client = HKEXFilingsClient.from_transport(transport)
    code = instrument.split(":", 1)[-1]
    stock = await client.resolve_stock(code)
    if stock is None:
        return {"success": False, "instrument": instrument, "stock_found": False}
    date_to = datetime.now(ZoneInfo("Asia/Hong_Kong")).strftime("%Y%m%d")
    reports = await client.search_reports(
        stock.stock_id,
        ["annual"],
        date_from=date_from,
        date_to=date_to,
    )
    first = reports[0] if reports else None
    normalized = None
    if first is not None:
        raw = {
            **first.to_raw(),
            "hkex_stock_id": stock.stock_id,
            "hkex_stock_name": stock.name,
            "input_stock_name": stock.name,
        }
        normalized = HKEXNewsPlugin().normalize(
            "financial_report",
            raw,
            CrawlScope(
                scope_type="instrument",
                scope_id=instrument,
                source_key=stock.code,
            ),
        )
    return {
        "success": bool(reports),
        "instrument": instrument,
        "stock_found": True,
        "stock_id": stock.stock_id,
        "stock_code": stock.code,
        "stock_name": stock.name,
        "report_count": len(reports),
        "first_report": first.to_raw() if first else None,
        "normalized_record_uid": normalized.record_uid if normalized else None,
        "normalized_event_time": (
            normalized.event_time.isoformat()
            if normalized and normalized.event_time
            else None
        ),
        "pdf_downloaded": False,
        "durable_storage_mutated": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--instrument", default="XHKG:00005")
    parser.add_argument("--date-from", default="20240101")
    args = parser.parse_args()
    result = asyncio.run(run(args.instrument, args.date_from))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
