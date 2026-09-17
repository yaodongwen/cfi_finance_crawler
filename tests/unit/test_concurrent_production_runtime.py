from __future__ import annotations

import asyncio
import time

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from crawl_framework.core.concurrent_runtime import (
    ConcurrentRuntimeError,
    ConcurrentProductionRuntime,
    ProgressSnapshot,
)
from crawl_framework.core.bootstrap import CrawlBootstrap
from crawl_framework.core.concurrency import (
    DatasetResourceBudget,
    GlobalStageBudget,
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
from crawl_framework.core.resume import (
    CheckpointResumeEvidenceProvider,
    ResumePlanner,
)
from crawl_framework.core.shutdown import ShutdownController
from crawl_framework.storage.checkpoint import (
    CheckpointKey,
    FileCheckpointStore,
)
from crawl_framework.storage.recovery_orchestrator import StartupRecoveryResult
from crawl_framework.storage.uploader import (
    UploadResult,
)
from crawl_framework.observability.progress import ProgressAggregator


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


class ResumeAwarePlugin(InstrumentedPlugin):
    def __init__(self, events):
        super().__init__(events)
        self.crawl_calls = []
        self.plan_calls = []

    async def crawl(self, dataset, scope, checkpoint, context):
        self.crawl_calls.append(scope.source_key)
        async for raw in super().crawl(dataset, scope, checkpoint, context):
            yield raw

    def is_checkpoint_durable_complete(
        self,
        dataset,
        scope,
        checkpoint,
        context,
    ):
        del dataset, context
        self.plan_calls.append(scope.source_key)
        return checkpoint.state.get("scope_complete") is True


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

    def has_scope(self, scope_token):
        suffix = scope_token.rsplit("|", 1)[-1]
        return any(
            record.source_id.startswith(suffix)
            for record in self.records
        )

    def flush_all(self):
        if not self.records:
            return []
        batch = FakeBatch(
            scope_tokens=frozenset(self.scope_tokens),
            records=tuple(self.records),
        )
        self.records = []
        self.scope_tokens = set()
        return [batch]


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
        self.fail_upload = False

    def submit_for_staged_runtime(self, record, *, scope_token=None):
        self.buffer.add(record, scope_token=scope_token)
        return type(
            "Decision",
            (),
            {
                "decision": "new",
                "buffered": True,
            },
        )(), []

    def prepare_batch(self, batch):
        self.stage_order.append("write")
        self.events.append(("write", _batch_name(batch), time.monotonic()))
        parquet_info = type(
            "ParquetInfo",
            (),
            {
                "file_size": len(batch.records),
            },
        )()
        return type(
            "Prepared",
            (),
            {
                "batch": batch,
                "parquet_info": parquet_info,
                "manifest": object(),
            },
        )()

    def upload_prepared_batch(self, prepared):
        self.stage_order.append("upload")
        self.events.append(
            ("upload_start", _batch_name(prepared.batch), time.monotonic())
        )
        if self.fail_upload:
            raise RuntimeError(
                "upload boom"
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
    return ",".join(
        sorted(
            token.rsplit("|", 1)[-1]
            for token in batch.scope_tokens
        )
    )


def resume_planner(plugin, checkpoint_store, context):
    return ResumePlanner(
        evidence_provider=CheckpointResumeEvidenceProvider(
            checkpoint_store=checkpoint_store,
            is_durable_complete=lambda scope, checkpoint: (
                plugin.is_checkpoint_durable_complete(
                    scope.dataset,
                    scope.scope,
                    checkpoint,
                    context,
                )
            ),
        )
    )


def complete_key(source_key):
    return CheckpointKey(
        site_id="demo",
        dataset="forum_post",
        scope_type="instrument",
        source_key=source_key,
    )


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
async def test_resume_plan_skips_durable_scope_before_crawl(tmp_path):
    events = []
    plugin = ResumeAwarePlugin(events)
    checkpoints = FileCheckpointStore(tmp_path / "checkpoints")
    checkpoints.save(
        complete_key("000001"),
        CrawlCheckpoint(state={"scope_complete": True}),
    )
    context = CrawlContext()
    runtime = ConcurrentProductionRuntime(
        plugin=plugin,
        pipeline=InstrumentedPipeline(events, upload_delay=0, catalog_delay=0),
        checkpoint_store=checkpoints,
        context=context,
        crawl_workers=2,
        writer_workers=1,
        upload_workers=1,
        catalog_workers=1,
        record_queue_size=10,
        upload_queue_size=10,
        catalog_queue_size=10,
        resume_planner=resume_planner(plugin, checkpoints, context),
    )

    result = await runtime.run(datasets=("forum_post",))

    assert "000001" not in plugin.crawl_calls
    assert plugin.plan_calls == [
        "000001", "000002", "000003", "000004", "000005", "000006"
    ]
    assert result["resume_plans"]["forum_post"]["durable_complete_scopes"] == 1
    assert result["resume_plans"]["forum_post"]["incomplete_scopes"] == 5
    assert len(result["runtime"]) == 6
    assert runtime.stats.scopes_started == 5
    assert runtime.stats.scopes_finished == 6


@pytest.mark.asyncio
async def test_runtime_populates_resume_seeded_progress_aggregator(tmp_path):
    events = []
    plugin = ResumeAwarePlugin(events)
    checkpoints = FileCheckpointStore(tmp_path / "checkpoints")
    checkpoints.save(
        complete_key("000001"),
        CrawlCheckpoint(state={"scope_complete": True}),
    )
    context = CrawlContext()
    aggregator = ProgressAggregator(profile="test_profile")
    runtime = ConcurrentProductionRuntime(
        plugin=plugin,
        pipeline=InstrumentedPipeline(events, upload_delay=0, catalog_delay=0),
        checkpoint_store=checkpoints,
        context=context,
        crawl_workers=2,
        writer_workers=1,
        upload_workers=1,
        catalog_workers=1,
        record_queue_size=10,
        upload_queue_size=10,
        catalog_queue_size=10,
        resume_planner=resume_planner(plugin, checkpoints, context),
        progress_aggregator=aggregator,
    )

    result = await runtime.run(datasets=("forum_post",))
    progress = result["progress"]
    dataset_progress = progress["sites"][0]["datasets"][0]

    assert progress["profile"] == "test_profile"
    assert progress["completed_scopes"] == 6
    assert progress["percent"] == 100.0
    assert dataset_progress["skipped_due_to_checkpoint"] == 1
    assert dataset_progress["records"]["crawled"] == 10
    assert dataset_progress["records"]["new"] == 10
    assert dataset_progress["storage"]["files_written"] == 5
    assert dataset_progress["queues"]["record_current"] == 0


@pytest.mark.asyncio
async def test_bootstrap_builds_resume_plan_from_post_recovery_state(tmp_path):
    events = []
    plugin = ResumeAwarePlugin(events)
    checkpoints = FileCheckpointStore(tmp_path / "checkpoints")
    context = CrawlContext()
    runtime = ConcurrentProductionRuntime(
        plugin=plugin,
        pipeline=InstrumentedPipeline(events, upload_delay=0, catalog_delay=0),
        checkpoint_store=checkpoints,
        context=context,
        crawl_workers=2,
        writer_workers=1,
        upload_workers=1,
        catalog_workers=1,
        record_queue_size=10,
        upload_queue_size=10,
        catalog_queue_size=10,
        resume_planner=resume_planner(plugin, checkpoints, context),
    )

    class Recovery:
        def run(self):
            events.append(("recovery", "all", time.monotonic()))
            for source_key in (
                "000001", "000002", "000003", "000004", "000005", "000006"
            ):
                checkpoints.save(
                    complete_key(source_key),
                    CrawlCheckpoint(state={"scope_complete": True}),
                )
            return StartupRecoveryResult(
                scanned=1,
                attempted=1,
                recovered=1,
                skipped=0,
                failed=0,
                terminal_failed=0,
                remaining_pending=0,
                can_continue=True,
            )

    result = await CrawlBootstrap(
        recovery_orchestrator=Recovery(),
        runtime=runtime,
    ).run(datasets=("forum_post",))

    assert result.success is True
    assert plugin.crawl_calls == []
    assert runtime.stats.scopes_started == 0
    assert result.runtime["resume_plans"]["forum_post"][
        "durable_complete_scopes"
    ] == 6
    assert [event[0] for event in events] == ["recovery"]


@pytest.mark.asyncio
async def test_concurrent_runtime_uses_dataset_crawl_worker_budget(tmp_path):
    events: list[tuple[str, str, float]] = []
    runtime = ConcurrentProductionRuntime(
        plugin=InstrumentedPlugin(events),
        pipeline=InstrumentedPipeline(events),
        checkpoint_store=FileCheckpointStore(tmp_path / "checkpoints"),
        context=CrawlContext(
            extra={
                "dataset_budgets": {
                    "forum_post": DatasetResourceBudget(
                        crawl_workers=3,
                    ),
                },
            },
        ),
        crawl_workers=1,
        writer_workers=1,
        upload_workers=1,
        catalog_workers=1,
        record_queue_size=10,
        upload_queue_size=10,
        catalog_queue_size=10,
    )

    result = await runtime.run(
        datasets=(
            "forum_post",
        )
    )

    assert result[
        "production_stats"
    ].crawl_workers == 3


@pytest.mark.asyncio
async def test_concurrent_runtime_reports_live_progress(tmp_path):
    events: list[tuple[str, str, float]] = []
    snapshots: list[ProgressSnapshot] = []
    runtime = ConcurrentProductionRuntime(
        plugin=InstrumentedPlugin(events),
        pipeline=InstrumentedPipeline(events, upload_delay=0.02),
        checkpoint_store=FileCheckpointStore(tmp_path / "checkpoints"),
        context=CrawlContext(),
        crawl_workers=2,
        writer_workers=1,
        upload_workers=1,
        catalog_workers=1,
        record_queue_size=10,
        upload_queue_size=10,
        catalog_queue_size=10,
        progress_reporter=snapshots.append,
        progress_interval_seconds=0.01,
    )

    await runtime.run(
        datasets=(
            "forum_post",
        )
    )

    assert snapshots
    assert snapshots[0].dataset == "forum_post"
    assert isinstance(
        snapshots[0].record_queue_depth,
        int,
    )


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


@pytest.mark.asyncio
async def test_concurrent_runtime_can_coalesce_scope_flushes_without_early_checkpoint(
    tmp_path,
):
    events: list[tuple[str, str, float]] = []
    pipeline = InstrumentedPipeline(events, upload_delay=0.0, catalog_delay=0.0)
    checkpoint_store = FileCheckpointStore(
        tmp_path / "checkpoints"
    )
    runtime = ConcurrentProductionRuntime(
        plugin=InstrumentedPlugin(events),
        pipeline=pipeline,
        checkpoint_store=checkpoint_store,
        context=CrawlContext(),
        crawl_workers=2,
        writer_workers=1,
        upload_workers=1,
        catalog_workers=1,
        record_queue_size=20,
        upload_queue_size=20,
        catalog_queue_size=20,
        flush_scope_on_scope_done=False,
    )

    result = await runtime.run(
        datasets=("forum_post",)
    )

    names = [
        name
        for event, name, _ in events
        if event == "write"
    ]
    assert names == [
        "000001,000002,000003,000004,000005,000006"
    ]
    assert runtime.production_stats.files_written == 1
    assert runtime.production_stats.uploads_completed == 1
    assert runtime.production_stats.catalog_jobs_completed == 1
    assert len(result["runtime"]) == 6
    assert runtime.stats.checkpoints_saved == 6


@pytest.mark.asyncio
async def test_coalesced_runtime_flushes_a_bounded_scope_window(tmp_path):
    events: list[tuple[str, str, float]] = []
    runtime = ConcurrentProductionRuntime(
        plugin=InstrumentedPlugin(events),
        pipeline=InstrumentedPipeline(events, upload_delay=0.0, catalog_delay=0.0),
        checkpoint_store=FileCheckpointStore(tmp_path / "checkpoints"),
        context=CrawlContext(),
        crawl_workers=2,
        writer_workers=1,
        upload_workers=1,
        catalog_workers=1,
        record_queue_size=20,
        upload_queue_size=20,
        catalog_queue_size=20,
        flush_scope_on_scope_done=False,
        coalesce_flush_scope_count=3,
    )

    result = await runtime.run(datasets=("forum_post",))

    assert runtime.production_stats.files_written == 2
    assert runtime.production_stats.uploads_completed == 2
    assert runtime.production_stats.catalog_jobs_completed == 2
    assert len(result["runtime"]) == 6
    assert runtime.stats.checkpoints_saved == 6


@pytest.mark.asyncio
async def test_concurrent_runtime_runs_post_dataset_hook_after_drain(
    tmp_path,
):

    events: list[tuple[str, str, float]] = []
    hook_calls = []

    def post_dataset_hook(dataset):

        hook_calls.append(
            (
                dataset,
                list(events),
            )
        )

        return {
            "dataset": dataset,
            "replacement_files": 1,
        }

    runtime = ConcurrentProductionRuntime(
        plugin=InstrumentedPlugin(events),
        pipeline=InstrumentedPipeline(events, upload_delay=0.0, catalog_delay=0.0),
        checkpoint_store=FileCheckpointStore(tmp_path / "checkpoints"),
        context=CrawlContext(),
        crawl_workers=2,
        writer_workers=1,
        upload_workers=1,
        catalog_workers=1,
        record_queue_size=20,
        upload_queue_size=20,
        catalog_queue_size=20,
        flush_scope_on_scope_done=False,
        post_dataset_hook=post_dataset_hook,
    )

    result = await runtime.run(
        datasets=("forum_post",)
    )

    assert hook_calls
    assert hook_calls[0][0] == "forum_post"

    observed = [
        event
        for event, _, _ in hook_calls[0][1]
    ]

    assert (
        "catalog_done"
        in observed
    )

    assert result["post_dataset"] == [
        {
            "dataset": "forum_post",
            "replacement_files": 1,
        }
    ]


@pytest.mark.asyncio
async def test_concurrent_runtime_collects_worker_failure_and_cancels_siblings(
    tmp_path,
):
    events: list[tuple[str, str, float]] = []
    pipeline = InstrumentedPipeline(events)
    pipeline.fail_upload = True
    runtime = ConcurrentProductionRuntime(
        plugin=InstrumentedPlugin(events),
        pipeline=pipeline,
        checkpoint_store=FileCheckpointStore(tmp_path / "checkpoints"),
        context=CrawlContext(),
        crawl_workers=2,
        writer_workers=1,
        upload_workers=2,
        catalog_workers=2,
        record_queue_size=2,
        upload_queue_size=2,
        catalog_queue_size=2,
    )

    with pytest.raises(
        ConcurrentRuntimeError,
        match="upload boom",
    ):

        await runtime.run(
            datasets=(
                "forum_post",
            )
        )

    assert (
        runtime.stats.errors
        == 1
    )


@pytest.mark.asyncio
async def test_concurrent_runtime_cancellation_awaits_stage_workers(tmp_path):
    started = asyncio.Event()
    stopped = asyncio.Event()

    class BlockingPlugin(InstrumentedPlugin):
        async def crawl(self, dataset, scope, checkpoint, context):
            del dataset, scope, checkpoint, context
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()
            if False:
                yield {}

    events: list[tuple[str, str, float]] = []
    runtime = ConcurrentProductionRuntime(
        plugin=BlockingPlugin(events),
        pipeline=InstrumentedPipeline(events),
        checkpoint_store=FileCheckpointStore(tmp_path / "checkpoints"),
        context=CrawlContext(),
        crawl_workers=1,
        writer_workers=1,
        upload_workers=1,
        catalog_workers=1,
        record_queue_size=2,
        upload_queue_size=2,
        catalog_queue_size=2,
    )

    task = asyncio.create_task(runtime.run(datasets=("forum_post",)))
    await asyncio.wait_for(started.wait(), timeout=1)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert stopped.is_set()


@pytest.mark.asyncio
async def test_concurrent_runtime_always_closes_progress_renderer(tmp_path):
    events = []
    closed = []
    runtime = ConcurrentProductionRuntime(
        plugin=InstrumentedPlugin(events),
        pipeline=InstrumentedPipeline(events, upload_delay=0, catalog_delay=0),
        checkpoint_store=FileCheckpointStore(tmp_path / "checkpoints"),
        context=CrawlContext(),
        crawl_workers=1,
        writer_workers=1,
        upload_workers=1,
        catalog_workers=1,
        record_queue_size=10,
        upload_queue_size=10,
        catalog_queue_size=10,
        progress_finalizer=lambda: closed.append(True),
    )

    await runtime.run(datasets=("forum_post",))

    assert closed == [True]


@pytest.mark.asyncio
async def test_first_shutdown_drains_durable_work_without_completing_scope(
    tmp_path,
):
    first_record_consumed = asyncio.Event()
    release_crawl = asyncio.Event()

    class GracefulPlugin(InstrumentedPlugin):
        async def discover(self, dataset, context):
            del dataset, context
            for code in ("000001", "000002"):
                yield CrawlScope(
                    scope_type="instrument",
                    source_key=code,
                    scope_id=f"XKRX:{code}",
                )

        async def crawl(self, dataset, scope, checkpoint, context):
            del dataset, checkpoint, context
            yield {"id": f"{scope.source_key}-0"}
            first_record_consumed.set()
            await release_crawl.wait()
            yield {"id": f"{scope.source_key}-1"}

        def checkpoint_after_scope(
            self, dataset, scope, checkpoint, context
        ):
            del dataset, scope, context
            state = dict(checkpoint.state)
            state["scope_complete"] = True
            return CrawlCheckpoint(state)

    events = []
    controller = ShutdownController()
    checkpoint_store = FileCheckpointStore(tmp_path / "checkpoints")
    runtime = ConcurrentProductionRuntime(
        plugin=GracefulPlugin(events),
        pipeline=InstrumentedPipeline(events, upload_delay=0, catalog_delay=0),
        checkpoint_store=checkpoint_store,
        context=CrawlContext(),
        crawl_workers=1,
        writer_workers=1,
        upload_workers=1,
        catalog_workers=1,
        record_queue_size=2,
        upload_queue_size=2,
        catalog_queue_size=2,
        shutdown_controller=controller,
    )

    task = asyncio.create_task(runtime.run(datasets=("forum_post",)))
    await asyncio.wait_for(first_record_consumed.wait(), timeout=1)
    controller.request_shutdown()
    release_crawl.set()
    result = await asyncio.wait_for(task, timeout=2)

    assert result["interrupted"] is True
    assert result["shutdown_state"] == "draining"
    assert runtime.production_stats.files_written == 1
    assert runtime.production_stats.uploads_completed == 1
    assert runtime.production_stats.catalog_jobs_completed == 1
    assert [item.scope_id for item in result["runtime"]] == ["XKRX:000001"]
    checkpoint = checkpoint_store.load(
        CheckpointKey(
            site_id="demo",
            dataset="forum_post",
            scope_type="instrument",
            source_key="000001",
        )
    )
    assert checkpoint.state == {"seen": 1}
    assert "scope_complete" not in checkpoint.state


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ("discover", "crawl"))
async def test_shutdown_transport_teardown_is_interrupted_not_worker_failure(
    tmp_path,
    failure_stage,
):
    started = asyncio.Event()
    release = asyncio.Event()

    class DisconnectingPlugin(InstrumentedPlugin):
        async def discover(self, dataset, context):
            if failure_stage == "discover":
                started.set()
                await release.wait()
                raise RuntimeError("transport connection closed")
            async for item in super().discover(dataset, context):
                yield item

        async def crawl(self, dataset, scope, checkpoint, context):
            if failure_stage == "crawl":
                started.set()
                await release.wait()
                raise RuntimeError("transport connection closed")
            async for item in super().crawl(dataset, scope, checkpoint, context):
                yield item
            if False:
                yield None

    controller = ShutdownController()
    events = []
    runtime = ConcurrentProductionRuntime(
        plugin=DisconnectingPlugin(events),
        pipeline=InstrumentedPipeline(events, upload_delay=0, catalog_delay=0),
        checkpoint_store=FileCheckpointStore(tmp_path / "checkpoints"),
        context=CrawlContext(),
        crawl_workers=1,
        writer_workers=1,
        upload_workers=1,
        catalog_workers=1,
        record_queue_size=2,
        upload_queue_size=2,
        catalog_queue_size=2,
        shutdown_controller=controller,
    )

    task = asyncio.create_task(runtime.run(datasets=("forum_post",)))
    await asyncio.wait_for(started.wait(), timeout=1)
    controller.request_shutdown()
    release.set()
    result = await asyncio.wait_for(task, timeout=2)

    assert result["interrupted"] is True
    assert result["shutdown_state"] == "draining"
    assert runtime.stats.errors == 0
    assert runtime.production_stats.files_written == 0
    assert runtime.production_stats.uploads_completed == 0
    assert runtime.production_stats.catalog_jobs_completed == 0


@pytest.mark.asyncio
async def test_second_shutdown_cancels_and_awaits_blocked_workers(tmp_path):
    started = asyncio.Event()
    stopped = asyncio.Event()
    finalized = []
    loop_errors = []
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(
        lambda _loop, context: loop_errors.append(context)
    )

    class BlockingPlugin(InstrumentedPlugin):
        async def crawl(self, dataset, scope, checkpoint, context):
            del dataset, scope, checkpoint, context
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()
            if False:
                yield {}

    controller = ShutdownController()
    events = []
    runtime = ConcurrentProductionRuntime(
        plugin=BlockingPlugin(events),
        pipeline=InstrumentedPipeline(events),
        checkpoint_store=FileCheckpointStore(tmp_path / "checkpoints"),
        context=CrawlContext(),
        crawl_workers=1,
        writer_workers=1,
        upload_workers=1,
        catalog_workers=1,
        record_queue_size=2,
        upload_queue_size=2,
        catalog_queue_size=2,
        shutdown_controller=controller,
        progress_finalizer=lambda: finalized.append(True),
    )

    try:
        task = asyncio.create_task(runtime.run(datasets=("forum_post",)))
        await asyncio.wait_for(started.wait(), timeout=1)
        controller.request_shutdown()
        controller.request_shutdown()
        result = await asyncio.wait_for(task, timeout=2)
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(previous_handler)

    assert result["interrupted"] is True
    assert result["shutdown_state"] == "aborting"
    assert result["shutdown_requests"] == 2
    assert stopped.is_set()
    assert finalized == [True]
    assert runtime.stats.errors == 0
    assert loop_errors == []


@pytest.mark.asyncio
async def test_two_production_runtimes_share_global_stage_limits(tmp_path):
    budget = GlobalStageBudget(
        writer_workers=1,
        upload_workers=1,
        catalog_workers=1,
    )

    def build_runtime(name):
        events = []
        return ConcurrentProductionRuntime(
            plugin=InstrumentedPlugin(events),
            pipeline=InstrumentedPipeline(
                events,
                upload_delay=0.02,
                catalog_delay=0.02,
            ),
            checkpoint_store=FileCheckpointStore(tmp_path / name),
            context=CrawlContext(),
            crawl_workers=2,
            writer_workers=2,
            upload_workers=2,
            catalog_workers=2,
            record_queue_size=4,
            upload_queue_size=4,
            catalog_queue_size=4,
            stage_budget=budget,
        )

    left = build_runtime("left")
    right = build_runtime("right")
    await asyncio.gather(
        left.run(datasets=("forum_post",)),
        right.run(datasets=("forum_post",)),
    )

    snapshot = budget.snapshot()
    assert snapshot.writer.max_active == 1
    assert snapshot.upload.max_active == 1
    assert snapshot.catalog.max_active == 1
    assert snapshot.upload.waits > 0
