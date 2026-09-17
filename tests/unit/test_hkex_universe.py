import csv
from pathlib import Path

from crawl_framework.sites.hkexnews import build_hkex_universe_snapshot


def test_build_hkex_snapshot_is_stable_canonical_and_deduplicated(tmp_path):
    source = tmp_path / "stocks.csv"
    with source.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["股票代码", "股票名称"])
        writer.writeheader()
        writer.writerows(
            [
                {"股票代码": "5", "股票名称": "HSBC"},
                {"股票代码": "00005", "股票名称": "duplicate"},
                {"股票代码": "700", "股票名称": "Tencent"},
            ]
        )
    output = tmp_path / "universe.txt"

    assert build_hkex_universe_snapshot(source, output) == 2
    lines = [
        line for line in output.read_text(encoding="utf-8").splitlines()
        if not line.startswith("#")
    ]
    assert lines == ["XHKG:00005", "XHKG:00700"]
    assert '"count": 2' in output.read_text(encoding="utf-8")
    assert '"source_sha256"' in output.read_text(encoding="utf-8")


def test_committed_hkex_snapshot_is_canonical_and_unique():
    path = Path("config/universes/hkex_hk_rollout_universe.txt")
    values = [
        line.strip() for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]

    assert len(values) == 2798
    assert len(values) == len(set(values))
    assert all(
        value.startswith("XHKG:")
        and len(value) == 10
        and value.split(":", 1)[1].isdigit()
        for value in values
    )
