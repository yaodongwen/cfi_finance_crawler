from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from crawl_framework.sites.hkexnews.filings import canonical_hkex_instrument_id


def build_hkex_universe_snapshot(source: Path, output: Path) -> int:
    instruments: list[str] = []
    seen: set[str] = set()
    with source.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if "股票代码" not in (reader.fieldnames or []):
            raise ValueError("HKEX source CSV requires 股票代码 column")
        for row in reader:
            instrument_id = canonical_hkex_instrument_id(row.get("股票代码", ""))
            if instrument_id in seen:
                continue
            seen.add(instrument_id)
            instruments.append(instrument_id)

    metadata = {
        "count": len(instruments),
        "country": "HK",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generation_method": (
            "stable CSV row order; normalize 股票代码 to exact five-digit "
            "XHKG canonical IDs; first occurrence wins"
        ),
        "site_id": "hkexnews",
        "source": str(source),
        "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        "# crawl_framework rollout universe snapshot\n"
        f"# metadata: {json.dumps(metadata, ensure_ascii=False, sort_keys=True)}\n"
        + "\n".join(instruments)
        + "\n",
        encoding="utf-8",
    )
    return len(instruments)
