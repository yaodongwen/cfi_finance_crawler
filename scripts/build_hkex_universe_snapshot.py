#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from crawl_framework.sites.hkexnews.universe import build_hkex_universe_snapshot


DEFAULT_SOURCE = Path("../finance_report/hk/outputs/hk_stocks_market_cap.csv")
DEFAULT_OUTPUT = Path("config/universes/hkex_hk_rollout_universe.txt")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    count = build_hkex_universe_snapshot(args.source, args.output)
    print(json.dumps({"output": str(args.output), "count": count}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
