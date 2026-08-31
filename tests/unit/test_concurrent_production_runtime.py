from __future__ import annotations

import asyncio
import time

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from crawl_framework.core.concurrent_runtime import (
    ConcurrentProductionRuntime,
)
from crawl_framework.core.models import (
    CanonicalRecord,
)
from crawl_framework.core.pipeline import (
    BatchProcessResult,
)
from crawl_framework.core.plugin import (
    CrawlCheckpoint,
    CrawlContext,
    CrawlScope,
    SitePlugin,
)
from crawl_framework.storage.checkpoint import (
    FileCheckpointStore,
)
from crawl_framework.storage.uploader import (
    UploadResult,
)


class InstrumentedPlugin(SitePlugin):
    site_id = "demo"
    country = "KR"
    timezone = "Asia/Seoul"

    def __init__(self, events: list[tuple[str, str, float]]) -> None:
        self.events = events

    def datasets(self) -> tuple[str, ...]:
        return ("forum_post",)

    async def discover(self, dataset, context):
        del dataset, context
        for code in (
            "000001",
            "000002",
            "000003",
            "000004",
            "000005",
            "000006",
        ):
            yield CrawlScope(
                scope_type="instrument",
                source_key=code,
                scope_id=f"XKRX:{code}",
            )

    async def crawl(self, dataset, scope, checkpoint, context):
        del dataset, checkpoint, context
        for index in range(2):
            self.events.append(
                ("crawl", scope.source_key, time.monotonic())
            )
            await asyncio.sleep(0.03)
            yield {
                "id": f"{scope.source_key}-{index}",
            }

    def normalize(self, dataset, raw, scope):
        return CanonicalRecord(
            site_id=self.site_id,
            country=self.country,
            dataset=dataset,
            source_id=raw["id"],
            scope_type=scope.scope_type,
            scope_id=scope.scope_id,
            instrument_id=scope.scope_id,
            event_time=datetime(2026, 8, 28, tzinfo=timezone.utc),
            title=raw["id"],
            content="body",
            source_url=f"https://example.test/{raw['id']}",
        )

    def checkpoint_after_record(
        self,
        dataset,
        scope,
        raw,
        record,
        checkpoint,
        context,
    ):
        del dataset, scope, raw, record, context
        state = dict(checkpoint.state)
        state["seen"] = state.get("seen", 0) + 1
        return CrawlCheckpoint(state)


@dataclass(frozen=True, slots=True)
class FakeBatch:
    scope_tokens: frozenset[str]
    records: tuple[CanonicalRecord, ...]


class FakeBuffer:
    def __init__(self) -> None:
        self.records: list[CanonicalRecord] = []
        self.scope_tokens: set[str] = set()

    def add(self, record, *, scope_token=None):
        self.records.append(record)
        if scope_token is not None:
            self.scope_tokens.add(scope_token)
        return None

    def flush_scope(self, scope_token):
        matched = [
            record
            for record in self.records
            if record.source_id.startswith(scope_token.rsplit("|", 1)[-1])
        ]
        self.records = [
            record
            for record in self.records
            if record not in matched
        ]
        if not matched:
            return []
        return [
            FakeBatch(
                scope_tokens=frozenset({scope_token}),
                records=tuple(matched),
            )
        ]

    def flush_all(self):
        return []


class InstrumentedPipeline:
    def __init__(
        self,
        events: list[tuple[str, str, float]],
        *,
        upload_delay: float = 0.05,
        catalog_delay: float = 0.03,
    ) -> None:
        self.events = events
        self.buffer = FakeBuffer()
        self.upload_delay = upload_delay
        self.catalog_delay = catalog_delay
        self.stage_order: list[str] = []

    def submit_for_staged_runtime(self, record, *, scope_token=None):
        self.buffer.add(record, scope_token=scope_token)
        return object(), []

    def prepare_batch(self, batch):
        self.stage_order.append("write")
        self.events.append(("write", _batch_name(batch), time.monotonic()))
        return type(
            "Prepared",
            (),
            {
                "batch": batch,
                "parquet_info": object(),
                "manifest": object(),
            },
        )()

    def upload_prepared_batch(self, prepared):
        self.stage_order.append("upload")
        self.events.append(
            ("upload_start", _batch_name(prepared.batch), time.monotonic())
        )
        time.sleep(self.upload_delay)
        self.events.append(
            ("upload_done", _batch_name(prepared.batch), time.monotonic())
        )
        return type(
            "Uploaded",
            (),
            {
                "prepared": prepared,
                "upload_result": UploadResult(
                    local_path=Path("local.parquet"),
                    remote_path="/remote/local.parquet",
                    status="verified",
                    local_size=1,
                    remote_size=1,
                ),
                "manifest": object(),
            },
        )()

    def catalog_uploaded_batch(self, uploaded, **kwargs):
        del kwargs
        self.stage_order.append("catalog")
        self.events.append(
            ("catalog_start", _batch_name(uploaded.prepared.batch), time.monotonic())
        )
        time.sleep(self.catalog_delay)
        self.events.append(
            ("catalog_done", _batch_name(uploaded.prepared.batch), time.monotonic())
        )
        return BatchProcessResult(
            parquet_info=object(),
            upload_result=uploaded.upload_result,
            catalog_registered=True,
            seen_committed=True,
            checkpoint_committed=True,
            local_deleted=False,
        )


def _batch_name(batch) -> str:
    token = next(iter(batch.scope_tokens))
    return token.rsplit("|", 1)[-1]


@pytest.mark.asyncio
async def test_concurrent_runtime_overlaps_crawl_upload_and_catalog(tmp_path):
    events: list[tuple[str, str, float]] = []
    runtime = ConcurrentProductionRuntime(
        plugin=InstrumentedPlugin(events),
        pipeline=InstrumentedPipeline(events),
        checkpoint_store=FileCheckpointStore(tmp_path / "checkpoints"),
        context=CrawlContext(),
        crawl_workers=2,
        writer_workers=1,
        upload_workers=2,
        catalog_workers=2,
        record_queue_size=10,
        upload_queue_size=10,
        catalog_queue_size=10,
    )

    result = await runtime.run(datasets=("forum_post",))

    stats = result["production_stats"]
    assert stats.records_crawled == 12
    assert stats.uploads_started == 6
    assert stats.catalog_jobs_completed == 6
    assert stats.crawl_upload_overlap
    assert stats.upload_catalog_overlap


@pytest.mark.asyncio
async def test_concurrent_runtime_backpressure_is_observable(tmp_path):
    events: list[tuple[str, str, float]] = []
    runtime = ConcurrentProductionRuntime(
        plugin=InstrumentedPlugin(events),
        pipeline=InstrumentedPipeline(events, upload_delay=0.1),
        checkpoint_store=FileCheckpointStore(tmp_path / "checkpoints"),
        context=CrawlContext(),
        crawl_workers=2,
        writer_workers=1,
        upload_workers=1,
        catalog_workers=1,
        record_queue_size=1,
        upload_queue_size=1,
        catalog_queue_size=1,
    )

    result = await runtime.run(datasets=("forum_post",))

    stats = result["production_stats"]
    assert stats.max_record_queue_depth <= 1
    assert stats.max_upload_queue_depth <= 1
    assert (
        stats.record_queue_put_waits
        + stats.upload_queue_put_waits
        + stats.catalog_queue_put_waits
        > 0
    )


@pytest.mark.asyncio
async def test_concurrent_runtime_preserves_batch_durable_order(tmp_path):
    events: list[tuple[str, str, float]] = []
    pipeline = InstrumentedPipeline(events, upload_delay=0.0, catalog_delay=0.0)
    runtime = ConcurrentProductionRuntime(
        plugin=InstrumentedPlugin(events),
        pipeline=pipeline,
        checkpoint_store=FileCheckpointStore(tmp_path / "checkpoints"),
        context=CrawlContext(),
        crawl_workers=1,
        writer_workers=1,
        upload_workers=1,
        catalog_workers=1,
        record_queue_size=10,
        upload_queue_size=10,
        catalog_queue_size=10,
    )

    await runtime.run(datasets=("forum_post",))

    by_batch = {}
    for event, name, _ in events:
        by_batch.setdefault(name, []).append(event)

    for observed in by_batch.values():
        assert observed.index("write") < observed.index("upload_start")
        assert observed.index("upload_done") < observed.index("catalog_start")
        assert observed.index("catalog_start") < observed.index("catalog_done")
