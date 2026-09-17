from __future__ import annotations

import argparse
import asyncio
import json

from crawl_framework.sites.kabutan import probe_kabutan_access
from crawl_framework.transports.http import (
    HttpRetryConfig,
    HttpTransport,
    RequestsHttpRequester,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe Kabutan anonymous Market News access once.",
    )
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    return parser.parse_args()


async def async_main(timeout_seconds: float) -> int:
    transport = HttpTransport(
        requester=RequestsHttpRequester(
            timeout_seconds=timeout_seconds,
            pool_size=1,
        ),
        retry_config=HttpRetryConfig(max_attempts=1),
        max_concurrency=1,
    )
    result = await probe_kabutan_access(transport)
    print(json.dumps(result.to_dict(), ensure_ascii=False, sort_keys=True))
    if result.access_state == "AVAILABLE":
        return 0
    if result.access_state == "WAF_BLOCKED":
        return 2
    return 1


def main() -> int:
    args = parse_args()
    if args.timeout_seconds <= 0:
        raise ValueError("--timeout-seconds must be positive")
    return asyncio.run(async_main(args.timeout_seconds))


if __name__ == "__main__":
    raise SystemExit(main())
