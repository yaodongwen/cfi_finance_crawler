from __future__ import annotations

import argparse
import asyncio
import json
import tempfile

from pathlib import Path

from crawl_framework.app_factory import AppConfig, AppFactory, AppPaths, SiteFactoryRegistry
from crawl_framework.cli.main import CLIOptions
from crawl_framework.core.concurrency import StageConcurrencyConfig
from crawl_framework.sites.tossinvest import TossInvestPlugin
from crawl_framework.transports.playwright import PlaywrightTransportConfig


class SmokeCursor:
    def __init__(self, connection):
        self.connection = connection

    def execute(self, sql, params=None):
        self.connection.executed.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class SmokeConnection:
    def __init__(self):
        self.executed = []

    def cursor(self):
        return SmokeCursor(self)

    def commit(self):
        pass


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--instrument", default="005930")
    parser.add_argument(
        "--dataset",
        choices=("forum_post", "news_article", "news_instrument"),
        default="forum_post",
    )
    parser.add_argument("--max-scroll-rounds", type=int, default=2)
    parser.add_argument("--detail-limit", type=int, default=1)
    return parser.parse_args()


async def run(args):
    root = Path(tempfile.mkdtemp(prefix="toss-j3-durable-"))
    registry = SiteFactoryRegistry()
    registry.register("tossinvest", TossInvestPlugin)
    factory = AppFactory(
        config=AppConfig(
            paths=AppPaths.from_root(root),
            buffer_min_rows=1,
            buffer_max_rows=100,
            stage_concurrency=StageConcurrencyConfig(
                crawl_workers=2,
                writer_workers=1,
                upload_workers=1,
                catalog_workers=1,
            ),
            browser_config=PlaywrightTransportConfig(
                workers=1,
                headless=True,
                profile_root=root / "profiles",
                launch_args=("--disable-blink-features=AutomationControlled",),
            ),
        ),
        site_registry=registry,
        connection_factory=SmokeConnection,
    )
    options = CLIOptions(
        site="tossinvest",

        datasets=(args.dataset,),
        flush_at_end=True,
        json_output=True,
        recovery_only=False,
        instruments=(args.instrument,),
    )
    bootstrap = factory.build(options)
    runtime = bootstrap.runtime
    if args.dataset == "forum_post":
        runtime.context.extra.update({
            "toss_forum_mode": "full",
            "toss_forum_max_scroll_rounds": args.max_scroll_rounds,
            "toss_forum_scroll_pause_ms": 300,
        })
    else:
        runtime.context.extra.update({
            "toss_news_baseline_complete": False,
            "toss_news_max_scroll_rounds": args.max_scroll_rounds,
            "toss_news_scroll_pause_ms": 300,
            "toss_news_detail_limit": args.detail_limit,
        })
    result = await bootstrap.run()

    pipeline = runtime.pipeline
    stats = pipeline.stats
    production = runtime.production_stats
    pending = pipeline.recovery_store.list_pending()
    checkpoint_files = list((root / "state" / "checkpoints").rglob("*.json"))
    checkpoint_state = {}
    if checkpoint_files:
        checkpoint_state = json.loads(checkpoint_files[0].read_text(encoding="utf-8")).get("state", {})
    output = {
        "success": result.success,
        "raw_count": runtime.stats.raw_records,
        "normalized_count": runtime.stats.normalized_records,
        "records_new": stats.records_new,
        "records_unchanged": stats.records_unchanged,
        "records_updated": stats.records_updated,
        "files_written": stats.files_written,
        "files_uploaded": stats.files_uploaded,
        "files_verified": stats.files_verified,
        "files_registered": stats.files_registered,
        "pipeline_errors": stats.errors,
        "remaining_pending": len(pending),
        "checkpoint_status": "complete" if checkpoint_files else "missing",
        "checkpoint_state": checkpoint_state,
        "crawl_workers": production.crawl_workers,
        "writer_workers": production.writer_workers,
        "upload_workers": production.upload_workers,
        "catalog_workers": production.catalog_workers,
        "max_record_queue_depth": production.max_record_queue_depth,
        "max_upload_queue_depth": production.max_upload_queue_depth,
        "max_catalog_queue_depth": production.max_catalog_queue_depth,
        "record_queue_size": runtime.record_queue_size,
        "upload_queue_size": runtime.upload_queue_size,
        "catalog_queue_size": runtime.catalog_queue_size,
        "smoke_root": str(root),
    }
    print(json.dumps(output, ensure_ascii=False, sort_keys=True))
    return 0 if all((output["success"], output["raw_count"] > 0,
                     output["remaining_pending"] == 0,
                     output["checkpoint_status"] == "complete")) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run(parse_args())))
