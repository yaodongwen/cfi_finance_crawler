from __future__ import annotations

import asyncio

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from crawl_framework.core.concurrent_runtime import ConcurrentProductionRuntime
from crawl_framework.core.models import CanonicalRecord
from crawl_framework.core.pipeline import BatchProcessResult, PipelineRecordDecision
from crawl_framework.core.plugin import CrawlCheckpoint, CrawlContext, CrawlScope, SitePlugin
from crawl_framework.core.resume import (
    CheckpointResumeEvidenceProvider,
    ResumePlan,
    ResumePlanner,
    ResumeScope,
    ResumeStatus,
    ScopeResumeEvidence,
)
from crawl_framework.core.shutdown import ShutdownController
from crawl_framework.observability.progress import ProgressAggregator, ProgressStatus
from crawl_framework.storage.checkpoint import CheckpointKey, FileCheckpointStore
from crawl_framework.storage.uploader import UploadResult


@dataclass
class MatrixEvidenceProvider:
    evidence: dict[str, ScopeResumeEvidence]

    def inspect(self, scope: ResumeScope) -> ScopeResumeEvidence:
        return self.evidence.get(scope.scope_token, ScopeResumeEvidence())


@dataclass(frozen=True, slots=True)
class MatrixBatch:
    scope_tokens: frozenset[str]
    records: tuple[CanonicalRecord, ...]


class MatrixBuffer:
    def __init__(self) -> None:
        self.pending: dict[str, list[CanonicalRecord]] = {}

    def add(self, record, *, scope_token=None):
        assert scope_token is not None
        self.pending.setdefault(scope_token, []).append(record)
        return None

    def has_scope(self, scope_token):
        return bool(self.pending.get(scope_token))

    def flush_scope(self, scope_token):
        records = tuple(self.pending.pop(scope_token, ()))
        if not records:
            return []
        return [MatrixBatch(frozenset({scope_token}), records)]

    def flush_all(self):
        batches = [
            MatrixBatch(frozenset({scope_token}), tuple(records))
            for scope_token, records in self.pending.items()
            if records
        ]
        self.pending.clear()
        return batches


class MatrixPipeline:
    def __init__(self, *, decision: str = "new") -> None:
        self.buffer = MatrixBuffer()
        self.decision = decision
        self.submits = 0
        self.writes = 0
        self.uploads = 0
        self.catalog_jobs = 0

    def submit_for_staged_runtime(self, record, *, scope_token=None):
        self.submits += 1
        buffered = self.decision != "unchanged"
        if buffered:
            self.buffer.add(record, scope_token=scope_token)
        return (
            PipelineRecordDecision(
                record_uid=record.record_uid,
                version_hash=record.version_hash,
                decision=self.decision,
                buffered=buffered,
            ),
            [],
        )

    def prepare_batch(self, batch):
        self.writes += 1
        return SimpleNamespace(
            batch=batch,
            parquet_info=SimpleNamespace(file_size=len(batch.records)),
            manifest=object(),
        )

    def upload_prepared_batch(self, prepared):
        self.uploads += 1
        upload = UploadResult(
            local_path=Path("matrix.parquet"),
            remote_path="/remote/matrix.parquet",
            status="verified",
            local_size=1,
            remote_size=1,
        )
        return SimpleNamespace(
            prepared=prepared,
            upload_result=upload,
            manifest=object(),
        )

    def catalog_uploaded_batch(self, uploaded, **kwargs):
        del kwargs
        self.catalog_jobs += 1
        return BatchProcessResult(
            parquet_info=uploaded.prepared.parquet_info,
            upload_result=uploaded.upload_result,
            catalog_registered=True,
            seen_committed=True,
            checkpoint_committed=True,
            local_deleted=False,
        )


class MatrixPlugin(SitePlugin):
    site_id = "matrix"
    country = "KR"
    timezone = "Asia/Seoul"

    def __init__(
        self,
        source_keys: tuple[str, ...],
        *,
        dataset: str = "forum_post",
        zero_records: bool = False,
    ) -> None:
        self.source_keys = source_keys
        self.dataset = dataset
        self.zero_records = zero_records
        self.crawl_calls: list[str] = []

    def datasets(self):
        return (self.dataset,)

    async def discover(self, dataset, context):
        del dataset, context
        for source_key in self.source_keys:
            yield scope(source_key)

    async def crawl(self, dataset, crawl_scope, checkpoint, context):
        del dataset, checkpoint, context
        self.crawl_calls.append(crawl_scope.source_key)
        if not self.zero_records:
            yield {"source_id": crawl_scope.source_key}

    def normalize(self, dataset, raw, crawl_scope):
        return CanonicalRecord(
            site_id=self.site_id,
            country=self.country,
            dataset=dataset,
            source_id=raw["source_id"],
            scope_type=crawl_scope.scope_type,
            scope_id=crawl_scope.scope_id,
            instrument_id=crawl_scope.scope_id,
            event_time=datetime(2026, 9, 17, tzinfo=timezone.utc),
            title=raw["source_id"],
        )

    def checkpoint_after_scope(self, dataset, crawl_scope, checkpoint, context):
        del dataset, crawl_scope, context
        state = dict(checkpoint.state)
        state["scope_complete"] = True
        return CrawlCheckpoint(state=state)

    def is_checkpoint_durable_complete(
        self, dataset, crawl_scope, checkpoint, context
    ):
        del dataset, crawl_scope, context
        return checkpoint.state.get("scope_complete") is True


def scope(source_key: str) -> CrawlScope:
    return CrawlScope(
        scope_type="instrument",
        scope_id=f"XTEST:{source_key}",
        source_key=source_key,
    )


def resume_scope(source_key: str, *, dataset: str = "forum_post") -> ResumeScope:
    return ResumeScope(
        site_id="matrix",
        dataset=dataset,
        scope=scope(source_key),
    )


def evidence_for(status: ResumeStatus) -> ScopeResumeEvidence:
    return ScopeResumeEvidence(
        checkpoint_exists=status is ResumeStatus.DURABLE_COMPLETE,
        checkpoint_state=(
            {"scope_complete": True}
            if status is ResumeStatus.DURABLE_COMPLETE
            else {}
        ),
        durable_complete=status is ResumeStatus.DURABLE_COMPLETE,
        recovery_pending=status is ResumeStatus.RECOVERY_PENDING,
        blocked_reason="waf_human_verification"
        if status is ResumeStatus.BLOCKED
        else None,
        failed_retryable=status is ResumeStatus.FAILED_RETRYABLE,
        failed_terminal=status is ResumeStatus.FAILED_TERMINAL,
    )


def planner_for(statuses: dict[str, ResumeStatus]) -> ResumePlanner:
    return ResumePlanner(
        evidence_provider=MatrixEvidenceProvider(
            {
                resume_scope(source_key).scope_token: evidence_for(status)
                for source_key, status in statuses.items()
            }
        )
    )


def build_runtime(
    tmp_path,
    plugin,
    pipeline,
    *,
    planner=None,
    aggregator=None,
    controller=None,
    post_dataset_hook=None,
    checkpoint_store=None,
):
    return ConcurrentProductionRuntime(
        plugin=plugin,
        pipeline=pipeline,
        checkpoint_store=(
            checkpoint_store
            or FileCheckpointStore(tmp_path / "checkpoints")
        ),
        context=CrawlContext(),
        crawl_workers=1,
        writer_workers=1,
        upload_workers=1,
        catalog_workers=1,
        record_queue_size=2,
        upload_queue_size=2,
        catalog_queue_size=2,
        resume_planner=planner,
        progress_aggregator=aggregator,
        shutdown_controller=controller,
        post_dataset_hook=post_dataset_hook,
    )


@pytest.mark.parametrize(
    ("durable", "expected_percent", "expected_remaining"),
    ((0, 0.0, 4), (2, 50.0, 2), (4, 100.0, 0)),
    ids=("cold", "partial", "complete"),
)
def test_cold_partial_complete_resume_progress_matrix(
    durable,
    expected_percent,
    expected_remaining,
):
    scopes = tuple(resume_scope(str(index)) for index in range(4))
    planner = planner_for({
        str(index): (
            ResumeStatus.DURABLE_COMPLETE
            if index < durable
            else ResumeStatus.INCOMPLETE
        )
        for index in range(4)
    })
    plan = planner.build(scopes)
    progress = ProgressAggregator(profile="matrix")
    progress.seed_resume_plan(plan)
    snapshot = progress.snapshot()

    assert plan.durable_complete_scopes == durable
    assert len(plan.crawl_scopes) == expected_remaining
    assert snapshot.completed_scopes == durable
    assert snapshot.remaining_scopes == expected_remaining
    assert snapshot.percent == expected_percent
    assert 0.0 <= snapshot.percent <= 100.0


@pytest.mark.asyncio
async def test_all_resume_states_drive_real_runtime_work_matrix(tmp_path):
    statuses = {
        "complete": ResumeStatus.DURABLE_COMPLETE,
        "pending": ResumeStatus.RECOVERY_PENDING,
        "incomplete": ResumeStatus.INCOMPLETE,
        "blocked": ResumeStatus.BLOCKED,
        "retryable": ResumeStatus.FAILED_RETRYABLE,
        "terminal": ResumeStatus.FAILED_TERMINAL,
    }
    plugin = MatrixPlugin(tuple(statuses))
    pipeline = MatrixPipeline()
    aggregator = ProgressAggregator(profile="matrix")
    runtime = build_runtime(
        tmp_path,
        plugin,
        pipeline,
        planner=planner_for(statuses),
        aggregator=aggregator,
    )

    result = await runtime.run(datasets=("forum_post",))
    progress = result["progress"]["sites"][0]["datasets"][0]

    assert plugin.crawl_calls == ["incomplete", "retryable"]
    assert pipeline.submits == 2
    assert pipeline.writes == pipeline.uploads == pipeline.catalog_jobs == 2
    plan = result["resume_plans"]["forum_post"]
    assert {
        key: value
        for key, value in plan.items()
        if key != "items"
    } == {
        "total_scopes": 6,
        "durable_complete_scopes": 1,
        "recovery_pending_scopes": 1,
        "incomplete_scopes": 1,
        "blocked_scopes": 1,
        "failed_retryable_scopes": 1,
        "failed_terminal_scopes": 1,
    }
    assert progress["completed_scopes"] == 3
    assert progress["skipped_due_to_checkpoint"] == 1
    assert progress["recovery_pending_scopes"] == 1
    assert progress["blocked_scopes"] == 1
    assert progress["failed_scopes"] == 2
    assert progress["status"] == ProgressStatus.FAILED.value


@pytest.mark.asyncio
async def test_completed_profile_rerun_has_zero_new_durable_work(tmp_path):
    keys = ("A", "B", "C")
    plugin = MatrixPlugin(keys)
    pipeline = MatrixPipeline()
    checkpoint_store = FileCheckpointStore(tmp_path / "checkpoints")
    for key in keys:
        checkpoint_store.save(
            CheckpointKey("matrix", "forum_post", "instrument", key),
            CrawlCheckpoint(state={"scope_complete": True}),
        )
    planner = ResumePlanner(
        evidence_provider=CheckpointResumeEvidenceProvider(
            checkpoint_store=checkpoint_store,
            is_durable_complete=lambda planned, checkpoint: (
                plugin.is_checkpoint_durable_complete(
                    planned.dataset,
                    planned.scope,
                    checkpoint,
                    CrawlContext(),
                )
            ),
        )
    )
    runtime = build_runtime(
        tmp_path,
        plugin,
        pipeline,
        planner=planner,
        aggregator=ProgressAggregator(profile="matrix"),
        checkpoint_store=checkpoint_store,
    )

    result = await runtime.run(datasets=("forum_post",))

    assert plugin.crawl_calls == []
    assert pipeline.submits == 0
    assert pipeline.writes == pipeline.uploads == pipeline.catalog_jobs == 0
    assert result["production_stats"].records_crawled == 0
    assert result["production_stats"].files_written == 0
    assert result["production_stats"].uploads_completed == 0
    assert result["production_stats"].catalog_jobs_completed == 0
    assert result["progress"]["percent"] == 100.0


@pytest.mark.asyncio
async def test_zero_record_scope_completes_without_file_work(tmp_path):
    plugin = MatrixPlugin(("empty",), zero_records=True)
    pipeline = MatrixPipeline()
    runtime = build_runtime(
        tmp_path,
        plugin,
        pipeline,
        planner=planner_for({"empty": ResumeStatus.INCOMPLETE}),
        aggregator=ProgressAggregator(profile="matrix"),
    )

    result = await runtime.run(datasets=("forum_post",))
    checkpoint = runtime.checkpoint_store.load(
        CheckpointKey("matrix", "forum_post", "instrument", "empty")
    )

    assert plugin.crawl_calls == ["empty"]
    assert checkpoint.state == {"scope_complete": True}
    assert pipeline.writes == pipeline.uploads == pipeline.catalog_jobs == 0
    assert result["progress"]["completed_scopes"] == 1
    assert result["progress"]["percent"] == 100.0


@pytest.mark.asyncio
async def test_unchanged_replay_makes_no_new_file_upload_or_catalog_job(tmp_path):
    plugin = MatrixPlugin(("same",))
    pipeline = MatrixPipeline(decision="unchanged")
    runtime = build_runtime(
        tmp_path,
        plugin,
        pipeline,
        planner=planner_for({"same": ResumeStatus.INCOMPLETE}),
        aggregator=ProgressAggregator(profile="matrix"),
    )

    result = await runtime.run(datasets=("forum_post",))
    dataset = result["progress"]["sites"][0]["datasets"][0]

    assert pipeline.submits == 1
    assert pipeline.writes == pipeline.uploads == pipeline.catalog_jobs == 0
    assert dataset["records"]["unchanged"] == 1
    assert dataset["storage"]["files_written"] == 0
    assert dataset["storage"]["files_uploaded"] == 0
    assert dataset["storage"]["files_cataloged"] == 0
    assert dataset["completed_scopes"] == 1


@pytest.mark.asyncio
async def test_attachment_dataset_and_compaction_metrics_share_progress_model(tmp_path):
    plugin = MatrixPlugin(("pdf",), dataset="attachment", zero_records=True)
    pipeline = MatrixPipeline()
    compaction = SimpleNamespace(
        replacement_files=1,
        source_files_superseded=4,
    )
    runtime = build_runtime(
        tmp_path,
        plugin,
        pipeline,
        planner=ResumePlanner(
            evidence_provider=MatrixEvidenceProvider({})
        ),
        aggregator=ProgressAggregator(profile="matrix"),
        post_dataset_hook=lambda dataset: compaction,
    )

    result = await runtime.run(datasets=("attachment",))
    dataset = result["progress"]["sites"][0]["datasets"][0]

    assert dataset["dataset"] == "attachment"
    assert dataset["storage"]["files_compacted"] == 1
    assert dataset["storage"]["replacement_files"] == 1
    assert dataset["storage"]["source_files_superseded"] == 4


@pytest.mark.asyncio
async def test_interrupted_scope_keeps_resume_progress_incomplete(tmp_path):
    started = asyncio.Event()
    release = asyncio.Event()

    class InterruptPlugin(MatrixPlugin):
        async def crawl(self, dataset, crawl_scope, checkpoint, context):
            del dataset, checkpoint, context
            self.crawl_calls.append(crawl_scope.source_key)
            yield {"source_id": crawl_scope.source_key}
            started.set()
            await release.wait()

    plugin = InterruptPlugin(("interrupt",))
    pipeline = MatrixPipeline()
    controller = ShutdownController()
    runtime = build_runtime(
        tmp_path,
        plugin,
        pipeline,
        planner=planner_for({"interrupt": ResumeStatus.INCOMPLETE}),
        aggregator=ProgressAggregator(profile="matrix"),
        controller=controller,
    )

    task = asyncio.create_task(runtime.run(datasets=("forum_post",)))
    await asyncio.wait_for(started.wait(), timeout=1)
    controller.request_shutdown()
    release.set()
    result = await asyncio.wait_for(task, timeout=2)

    assert result["interrupted"] is True
    assert result["progress"]["completed_scopes"] == 0
    assert result["progress"]["remaining_scopes"] == 1
    checkpoint = runtime.checkpoint_store.load(
        CheckpointKey("matrix", "forum_post", "instrument", "interrupt")
    )
    assert checkpoint.state.get("scope_complete") is not True
