from __future__ import annotations

import asyncio
import time

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Callable

from crawl_framework.core.concurrency import (
    DatasetResourceBudget,
    GlobalStageBudget,
)
from crawl_framework.core.pipeline import (
    BatchProcessResult,
    PreparedBatch,
    StoragePipeline,
    UploadedBatch,
)
from crawl_framework.core.plugin import (
    CrawlCheckpoint,
    CrawlContext,
    CrawlScope,
    SitePlugin,
)
from crawl_framework.core.runtime import (
    RuntimeStats,
    ScopePipelineStats,
    ScopeRunResult,
    checkpoint_key_for_scope,
    make_scope_token,
)
from crawl_framework.core.resume import (
    ResumePlan,
    ResumePlanItem,
    ResumePlanner,
    ResumeScope,
    ResumeStatus,
)
from crawl_framework.core.shutdown import ShutdownController
from crawl_framework.storage.buffer import (
    FlushBatch,
)
from crawl_framework.storage.checkpoint import (
    CheckpointKey,
    CheckpointStore,
)
from crawl_framework.observability.progress import ProgressAggregator


class ConcurrentRuntimeError(
    RuntimeError
):
    """
    One or more concurrent production workers failed.
    """


class _ForcedShutdown(BaseException):
    """Internal control flow used after a second shutdown request."""


@dataclass(slots=True)
class ProductionPipelineStats:
    crawl_workers: int
    writer_workers: int
    upload_workers: int
    catalog_workers: int
    records_crawled: int = 0
    files_written: int = 0
    uploads_started: int = 0
    uploads_completed: int = 0
    catalog_jobs_started: int = 0
    catalog_jobs_completed: int = 0
    max_record_queue_depth: int = 0
    max_upload_queue_depth: int = 0
    max_catalog_queue_depth: int = 0
    crawl_busy_time: float = 0.0
    writer_busy_time: float = 0.0
    upload_busy_time: float = 0.0
    catalog_busy_time: float = 0.0
    crawl_intervals: list[tuple[float, float]] = field(default_factory=list)
    write_intervals: list[tuple[float, float]] = field(default_factory=list)
    upload_intervals: list[tuple[float, float]] = field(default_factory=list)
    catalog_intervals: list[tuple[float, float]] = field(default_factory=list)
    record_queue_put_waits: int = 0
    upload_queue_put_waits: int = 0
    catalog_queue_put_waits: int = 0

    @property
    def crawl_upload_overlap(self) -> bool:
        return _has_overlap(self.crawl_intervals, self.upload_intervals)

    @property
    def upload_catalog_overlap(self) -> bool:
        return _has_overlap(self.upload_intervals, self.catalog_intervals)

    @property
    def crawl_catalog_overlap(self) -> bool:
        return _has_overlap(self.crawl_intervals, self.catalog_intervals)


@dataclass(
    frozen=True,
    slots=True,
)
class ProgressSnapshot:
    dataset: str
    scopes_discovered: int
    scopes_started: int
    scopes_finished: int
    records_crawled: int
    files_written: int
    uploads_completed: int
    catalog_jobs_completed: int
    record_queue_depth: int
    upload_queue_depth: int
    catalog_queue_depth: int
    pending_scopes: int
    crawl_busy_time: float
    upload_busy_time: float
    catalog_busy_time: float


ProgressReporter = Callable[
    [
        ProgressSnapshot,
    ],
    None,
]


@dataclass(slots=True)
class _ScopeState:
    dataset: str
    scope: CrawlScope
    scope_token: str
    checkpoint_key: CheckpointKey
    checkpoint: CrawlCheckpoint
    candidate_checkpoint: CrawlCheckpoint
    raw_count: int = 0
    normalized_count: int = 0
    skipped_count: int = 0
    pending_batches: int = 0
    crawl_done: bool = False
    interrupted: bool = False
    result: ScopeRunResult | None = None


@dataclass(frozen=True, slots=True)
class _RecordItem:
    state: _ScopeState
    raw: Any


@dataclass(frozen=True, slots=True)
class _FlushScopeItem:
    state: _ScopeState
    checkpoint_scope: bool = True


@dataclass(frozen=True, slots=True)
class _UploadJob:
    batch: FlushBatch


@dataclass(frozen=True, slots=True)
class _CatalogJob:
    uploaded: UploadedBatch


class ConcurrentProductionRuntime:
    """
    Bounded staged production runtime.

    Scope discovery/crawling, local durable writing, upload/verify, and
    Catalog/SeenStore/checkpoint work run as separate worker pools connected by
    bounded queues. The per-batch durable order is still enforced by
    StoragePipeline stage methods.
    """

    def __init__(
        self,
        *,
        plugin: SitePlugin,
        pipeline: StoragePipeline,
        checkpoint_store: CheckpointStore,
        context: CrawlContext,
        crawl_workers: int,
        writer_workers: int,
        upload_workers: int,
        catalog_workers: int,
        record_queue_size: int,
        upload_queue_size: int,
        catalog_queue_size: int,
        progress_reporter: ProgressReporter | None = None,
        progress_interval_seconds: float | None = None,
        flush_scope_on_scope_done: bool = True,
        coalesce_flush_scope_count: int = 500,
        post_dataset_hook: Callable[[str], Any] | None = None,
        resume_planner: ResumePlanner | None = None,
        progress_aggregator: ProgressAggregator | None = None,
        progress_finalizer: Callable[[], None] | None = None,
        shutdown_controller: ShutdownController | None = None,
        stage_budget: GlobalStageBudget | None = None,
    ) -> None:
        self.plugin = plugin
        self.pipeline = pipeline
        self.checkpoint_store = checkpoint_store
        self.context = context
        self.crawl_workers = max(1, int(crawl_workers))
        self.writer_workers = max(1, int(writer_workers))
        self.upload_workers = max(1, int(upload_workers))
        self.catalog_workers = max(1, int(catalog_workers))
        self.record_queue_size = max(1, int(record_queue_size))
        self.upload_queue_size = max(1, int(upload_queue_size))
        self.catalog_queue_size = max(1, int(catalog_queue_size))
        self.progress_reporter = progress_reporter
        self.progress_interval_seconds = (
            None
            if progress_interval_seconds is None
            else max(
                0.0,
                float(progress_interval_seconds),
            )
        )
        self.flush_scope_on_scope_done = bool(
            flush_scope_on_scope_done
        )
        self.coalesce_flush_scope_count = max(
            1,
            int(coalesce_flush_scope_count),
        )
        self.post_dataset_hook = post_dataset_hook
        self.resume_planner = resume_planner
        self.progress_aggregator = progress_aggregator
        self.progress_finalizer = progress_finalizer
        self.shutdown_controller = shutdown_controller or ShutdownController()
        self.stage_budget = stage_budget
        self.stats = RuntimeStats()
        self.production_stats = ProductionPipelineStats(
            crawl_workers=self.crawl_workers,
            writer_workers=self.writer_workers,
            upload_workers=self.upload_workers,
            catalog_workers=self.catalog_workers,
        )
        self.post_dataset_results: list[Any] = []
        self.resume_plans: dict[str, ResumePlan] = {}
        self._states: dict[str, _ScopeState] = {}
        self._results: list[ScopeRunResult] = []
        self._interrupted = False
        self._forced_shutdown = False

    async def run(
        self,
        *,
        datasets: list[str] | tuple[str, ...] | None = None,
        flush_at_end: bool = True,
    ) -> dict[str, Any]:
        selected = (
            list(self.plugin.validate_datasets())
            if datasets is None
            else [str(dataset).strip() for dataset in datasets]
        )

        all_results: list[ScopeRunResult] = []

        try:
            try:
                for dataset in selected:
                    if self.shutdown_controller.stop_requested:
                        self._interrupted = True
                        break
                    all_results.extend(
                        await self.run_dataset(dataset)
                    )
                    if self.shutdown_controller.stop_requested:
                        self._interrupted = True
                        break
                    if self.post_dataset_hook is not None:
                        async with self._maintenance_slot():
                            self.post_dataset_results.append(
                                await asyncio.to_thread(
                                    self.post_dataset_hook,
                                    dataset,
                                )
                            )
                        self._record_compaction_progress(
                            dataset,
                            self.post_dataset_results[-1],
                        )
            finally:
                if flush_at_end and not self.shutdown_controller.abort_requested:
                    for batch in self.pipeline.buffer.flush_all():
                        async with self._stage_slot("writer"):
                            prepared = await asyncio.to_thread(
                                self.pipeline.prepare_batch,
                                batch,
                            )
                        async with self._stage_slot("upload"):
                            uploaded = await asyncio.to_thread(
                                self.pipeline.upload_prepared_batch,
                                prepared,
                            )
                        async with self._stage_slot("catalog"):
                            await asyncio.to_thread(
                                self.pipeline.catalog_uploaded_batch,
                                uploaded,
                            )

            if (
                self.progress_reporter is not None
                and self.progress_aggregator is not None
            ):
                self.progress_reporter(None)

            return {
                "runtime": all_results,
                "production_stats": self.production_stats,
                "post_dataset": self.post_dataset_results,
                "resume_plans": {
                    dataset: plan.to_dict()
                    for dataset, plan in self.resume_plans.items()
                },
                "progress": (
                    self.progress_aggregator.snapshot().to_dict()
                    if self.progress_aggregator is not None
                    else None
                ),
                "interrupted": self._interrupted,
                "shutdown_state": self.shutdown_controller.state.value,
                "shutdown_requests": self.shutdown_controller.requests,
                "call_graph": (
                    "cli.main -> app_factory -> CrawlBootstrap -> "
                    "ConcurrentProductionRuntime -> crawl_queue -> "
                    "record_queue -> writer workers -> upload_queue -> "
                    "upload workers -> catalog_queue -> catalog workers -> "
                    "SeenStore/checkpoint"
                ),
            }
        finally:
            if self.progress_finalizer is not None:
                self.progress_finalizer()

    async def run_dataset(
        self,
        dataset: str,
    ) -> list[ScopeRunResult]:
        if not self.plugin.supports_dataset(dataset):
            raise ValueError(
                f"{self.plugin.site_id}: unsupported dataset {dataset!r}"
            )

        self.stats.datasets_started += 1
        crawl_workers = self._crawl_workers_for_dataset(
            dataset
        )
        self.production_stats.crawl_workers = (
            crawl_workers
        )
        scope_queue: asyncio.Queue[
            CrawlScope | ResumePlanItem | object
        ] = asyncio.Queue()
        record_queue: asyncio.Queue[_RecordItem | _FlushScopeItem | object] = (
            asyncio.Queue(maxsize=self.record_queue_size)
        )
        upload_queue: asyncio.Queue[_UploadJob | object] = asyncio.Queue(
            maxsize=self.upload_queue_size
        )
        catalog_queue: asyncio.Queue[_CatalogJob | object] = asyncio.Queue(
            maxsize=self.catalog_queue_size
        )
        sentinel = object()
        writer_lock = asyncio.Lock()
        failure_event = asyncio.Event()
        worker_errors: list[BaseException] = []
        worker_errors_lock = asyncio.Lock()
        coalesced_scopes_since_flush = 0

        async def record_worker_error(
            exc: BaseException,
        ) -> None:
            async with worker_errors_lock:
                worker_errors.append(
                    exc
                )
            failure_event.set()

        async def checked_put(
            queue,
            item,
            attr: str,
        ) -> None:
            if failure_event.is_set():
                raise ConcurrentRuntimeError(
                    "concurrent production runtime "
                    "is stopping after worker failure"
                )
            await put_bounded(
                queue,
                item,
                attr,
            )

        async def put_bounded(queue, item, attr: str) -> None:
            if queue.full():
                setattr(
                    self.production_stats,
                    attr,
                    getattr(self.production_stats, attr) + 1,
                )
            await queue.put(item)

        def remember_depths() -> None:
            self.production_stats.max_record_queue_depth = max(
                self.production_stats.max_record_queue_depth,
                record_queue.qsize(),
            )
            self.production_stats.max_upload_queue_depth = max(
                self.production_stats.max_upload_queue_depth,
                upload_queue.qsize(),
            )
            self.production_stats.max_catalog_queue_depth = max(
                self.production_stats.max_catalog_queue_depth,
                catalog_queue.qsize(),
            )
            if self.progress_aggregator is not None:
                self.progress_aggregator.observe_queues(
                    self.plugin.site_id,
                    dataset,
                    record_current=record_queue.qsize(),
                    upload_current=upload_queue.qsize(),
                    catalog_current=catalog_queue.qsize(),
                )

        def make_progress_snapshot() -> ProgressSnapshot:
            return ProgressSnapshot(
                dataset=dataset,
                scopes_discovered=self.stats.scopes_discovered,
                scopes_started=self.stats.scopes_started,
                scopes_finished=self.stats.scopes_finished,
                records_crawled=(
                    self.production_stats.records_crawled
                ),
                files_written=(
                    self.production_stats.files_written
                ),
                uploads_completed=(
                    self.production_stats.uploads_completed
                ),
                catalog_jobs_completed=(
                    self.production_stats.catalog_jobs_completed
                ),
                record_queue_depth=record_queue.qsize(),
                upload_queue_depth=upload_queue.qsize(),
                catalog_queue_depth=catalog_queue.qsize(),
                pending_scopes=len(
                    [
                        state
                        for state in self._states.values()
                        if state.result is None
                    ]
                ),
                crawl_busy_time=(
                    self.production_stats.crawl_busy_time
                ),
                upload_busy_time=(
                    self.production_stats.upload_busy_time
                ),
                catalog_busy_time=(
                    self.production_stats.catalog_busy_time
                ),
            )

        async def progress_worker() -> None:
            if (
                self.progress_reporter is None
                or self.progress_interval_seconds is None
            ):

                return

            interval = self.progress_interval_seconds

            while not failure_event.is_set():

                await asyncio.sleep(
                    interval
                )

                self.progress_reporter(
                    make_progress_snapshot()
                )

        async def discover() -> None:
            try:
                discovered: list[CrawlScope] = []
                async for scope in self.plugin.discover(dataset, self.context):
                    if (
                        failure_event.is_set()
                        or self.shutdown_controller.stop_requested
                    ):
                        self._interrupted = self.shutdown_controller.stop_requested
                        return
                    self.stats.scopes_discovered += 1
                    discovered.append(scope)

                if self.resume_planner is None:
                    for scope in discovered:
                        if self.shutdown_controller.stop_requested:
                            self._interrupted = True
                            return
                        await scope_queue.put(scope)
                    return

                plan = self.resume_planner.build(
                    ResumeScope(
                        site_id=self.plugin.site_id,
                        dataset=dataset,
                        scope=scope,
                    )
                    for scope in discovered
                )
                self.resume_plans[dataset] = plan
                if self.progress_aggregator is not None:
                    self.progress_aggregator.seed_resume_plan(plan)
                self.stats.checkpoints_loaded += plan.total_scopes

                for item in plan.items:
                    if self.shutdown_controller.stop_requested:
                        self._interrupted = True
                        return
                    if item.should_crawl:
                        await scope_queue.put(item)
                    elif item.status is ResumeStatus.DURABLE_COMPLETE:
                        self._record_durable_resume_skip(item)
            except Exception:
                if self.shutdown_controller.stop_requested:
                    self._interrupted = True
                    return
                raise
            finally:
                for _ in range(crawl_workers):
                    await scope_queue.put(sentinel)

        async def crawl_worker() -> None:
            try:
                while True:
                    scope = await scope_queue.get()
                    try:
                        if scope is sentinel:
                            return
                        if failure_event.is_set():
                            return
                        if self.shutdown_controller.stop_requested:
                            self._interrupted = True
                            return
                        planned_checkpoint = None
                        if isinstance(scope, ResumePlanItem):
                            planned_checkpoint = CrawlCheckpoint(
                                state=dict(scope.evidence.checkpoint_state)
                            )
                            scope = scope.scope.scope
                        assert isinstance(scope, CrawlScope)
                        state = self._begin_scope(
                            dataset,
                            scope,
                            checkpoint=planned_checkpoint,
                        )
                        start = time.monotonic()
                        scope_interrupted = False
                        try:
                            async for raw in self.plugin.crawl(
                                dataset,
                                scope,
                                state.checkpoint,
                                self.context,
                            ):
                                if failure_event.is_set():
                                    return
                                if self.shutdown_controller.stop_requested:
                                    self._interrupted = True
                                    scope_interrupted = True
                                    break
                                self.stats.raw_records += 1
                                self.production_stats.records_crawled += 1
                                state.raw_count += 1
                                if self.progress_aggregator is not None:
                                    self.progress_aggregator.record_activity(
                                        self.plugin.site_id,
                                        dataset,
                                        crawled=1,
                                    )
                                await checked_put(
                                    record_queue,
                                    _RecordItem(state=state, raw=raw),
                                    "record_queue_put_waits",
                                )
                                remember_depths()
                            if self.shutdown_controller.stop_requested:
                                self._interrupted = True
                                scope_interrupted = True
                        finally:
                            end = time.monotonic()
                            self.production_stats.crawl_busy_time += end - start
                            self.production_stats.crawl_intervals.append(
                                (start, end)
                            )
                            if self.progress_aggregator is not None:
                                self.progress_aggregator.busy_time(
                                    self.plugin.site_id,
                                    dataset,
                                    crawl=end - start,
                                )
                        await checked_put(
                            record_queue,
                            _FlushScopeItem(
                                state=state,
                                checkpoint_scope=not scope_interrupted,
                            ),
                            "record_queue_put_waits",
                        )
                        remember_depths()
                    finally:
                        scope_queue.task_done()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self.shutdown_controller.stop_requested:
                    self._interrupted = True
                    return
                await record_worker_error(exc)
                raise

        async def writer_worker() -> None:
            nonlocal coalesced_scopes_since_flush
            try:
                while True:
                    item = await record_queue.get()
                    try:
                        if item is sentinel:
                            return
                        if failure_event.is_set():
                            return
                        async with writer_lock:
                            start = time.monotonic()
                            batches: list[FlushBatch] = []
                            if isinstance(item, _RecordItem):
                                record = self.plugin.normalize_and_validate(
                                    dataset,
                                    item.raw,
                                    item.state.scope,
                                )
                                if record is None:
                                    self.stats.skipped_records += 1
                                    item.state.skipped_count += 1
                                else:
                                    self.stats.normalized_records += 1
                                    item.state.normalized_count += 1
                                    decision, batches = (
                                        self.pipeline.submit_for_staged_runtime(
                                            record,
                                            scope_token=item.state.scope_token,
                                        )
                                    )
                                    if self.progress_aggregator is not None:
                                        self.progress_aggregator.record_activity(
                                            self.plugin.site_id,
                                            dataset,
                                            normalized=1,
                                            **{decision.decision: 1},
                                        )
                                        if decision.buffered:
                                            self.progress_aggregator.storage_activity(
                                                self.plugin.site_id,
                                                dataset,
                                                buffered_rows=1,
                                            )
                                    hook = getattr(
                                        self.plugin,
                                        "checkpoint_after_record",
                                        None,
                                    )
                                    if callable(hook):
                                        item.state.candidate_checkpoint = hook(
                                            dataset,
                                            item.state.scope,
                                            item.raw,
                                            record,
                                            item.state.candidate_checkpoint,
                                            self.context,
                                        )
                            elif isinstance(item, _FlushScopeItem):
                                item.state.interrupted = not item.checkpoint_scope
                                scope_hook = getattr(
                                    self.plugin,
                                    "checkpoint_after_scope",
                                    None,
                                )
                                if callable(scope_hook) and item.checkpoint_scope:
                                    item.state.candidate_checkpoint = scope_hook(
                                        dataset,
                                        item.state.scope,
                                        item.state.candidate_checkpoint,
                                        self.context,
                                    )
                                if self.flush_scope_on_scope_done:
                                    batches = self.pipeline.buffer.flush_scope(
                                        item.state.scope_token
                                    )
                                else:
                                    coalesced_scopes_since_flush += 1
                                    if (
                                        coalesced_scopes_since_flush
                                        >= self.coalesce_flush_scope_count
                                    ):
                                        batches = self.pipeline.buffer.flush_all()
                                        coalesced_scopes_since_flush = 0
                                item.state.crawl_done = True
                            else:
                                raise TypeError(type(item).__name__)

                            for batch in batches:
                                self._track_batch(batch)
                                await checked_put(
                                    upload_queue,
                                    _UploadJob(batch=batch),
                                    "upload_queue_put_waits",
                                )
                                remember_depths()
                            end = time.monotonic()
                            self.production_stats.writer_busy_time += end - start
                            self.production_stats.write_intervals.append(
                                (start, end)
                            )
                            if self.progress_aggregator is not None:
                                self.progress_aggregator.busy_time(
                                    self.plugin.site_id,
                                    dataset,
                                    write=end - start,
                                )

                        if isinstance(item, _FlushScopeItem):
                            self._complete_scope_if_ready(item.state)
                    finally:
                        record_queue.task_done()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await record_worker_error(exc)
                raise

        async def upload_worker() -> None:
            try:
                while True:
                    item = await upload_queue.get()
                    try:
                        if item is sentinel:
                            return
                        if failure_event.is_set():
                            return
                        assert isinstance(item, _UploadJob)
                        start = time.monotonic()
                        async with self._stage_slot("writer"):
                            prepared: PreparedBatch = await asyncio.to_thread(
                                self.pipeline.prepare_batch,
                                item.batch,
                            )
                        self.production_stats.files_written += 1
                        self.production_stats.uploads_started += 1
                        if self.progress_aggregator is not None:
                            self.progress_aggregator.storage_activity(
                                self.plugin.site_id,
                                dataset,
                                files_prepared=1,
                                files_written=1,
                            )
                        async with self._stage_slot("upload"):
                            uploaded: UploadedBatch = await asyncio.to_thread(
                                self.pipeline.upload_prepared_batch,
                                prepared,
                            )
                        self.production_stats.uploads_completed += 1
                        if self.progress_aggregator is not None:
                            self.progress_aggregator.storage_activity(
                                self.plugin.site_id,
                                dataset,
                                files_uploaded=1,
                                files_verified=1,
                                uploaded_bytes=(
                                    uploaded.prepared.parquet_info.file_size
                                ),
                            )
                        end = time.monotonic()
                        self.production_stats.upload_busy_time += end - start
                        self.production_stats.upload_intervals.append(
                            (start, end)
                        )
                        if self.progress_aggregator is not None:
                            self.progress_aggregator.busy_time(
                                self.plugin.site_id,
                                dataset,
                                upload=end - start,
                            )
                        await checked_put(
                            catalog_queue,
                            _CatalogJob(uploaded=uploaded),
                            "catalog_queue_put_waits",
                        )
                        remember_depths()
                    finally:
                        upload_queue.task_done()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await record_worker_error(exc)
                raise

        async def catalog_worker() -> None:
            try:
                while True:
                    item = await catalog_queue.get()
                    try:
                        if item is sentinel:
                            return
                        if failure_event.is_set():
                            return
                        assert isinstance(item, _CatalogJob)
                        self.production_stats.catalog_jobs_started += 1
                        start = time.monotonic()
                        async with self._stage_slot("catalog"):
                            result: BatchProcessResult = await asyncio.to_thread(
                                self.pipeline.catalog_uploaded_batch,
                                item.uploaded,
                            )
                        end = time.monotonic()
                        self.production_stats.catalog_busy_time += end - start
                        self.production_stats.catalog_intervals.append(
                            (start, end)
                        )
                        self.production_stats.catalog_jobs_completed += 1
                        if self.progress_aggregator is not None:
                            self.progress_aggregator.storage_activity(
                                self.plugin.site_id,
                                dataset,
                                files_cataloged=1,
                                active_files=1,
                            )
                            self.progress_aggregator.busy_time(
                                self.plugin.site_id,
                                dataset,
                                catalog=end - start,
                            )
                        self._complete_batch(
                            item.uploaded.prepared.batch,
                            result,
                        )
                    finally:
                        catalog_queue.task_done()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await record_worker_error(exc)
                raise

        tasks = [
            asyncio.create_task(discover()),
            *[
                asyncio.create_task(crawl_worker())
                for _ in range(crawl_workers)
            ],
            *[
                asyncio.create_task(writer_worker())
                for _ in range(self.writer_workers)
            ],
            *[
                asyncio.create_task(upload_worker())
                for _ in range(self.upload_workers)
            ],
            *[
                asyncio.create_task(catalog_worker())
                for _ in range(self.catalog_workers)
            ],
        ]
        progress_task = asyncio.create_task(
            progress_worker()
        )

        async def fail_fast_tasks(
            stage_tasks,
        ) -> None:
            joined = asyncio.gather(
                *stage_tasks
            )
            failure_waiter = asyncio.create_task(
                failure_event.wait()
            )
            abort_waiter = asyncio.create_task(
                self.shutdown_controller.wait_for_abort()
            )
            try:
                done, pending = await asyncio.wait(
                    {
                        joined,
                        failure_waiter,
                        abort_waiter,
                    },
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

                for task in done:
                    await task
            except BaseException:
                joined.cancel()
                failure_waiter.cancel()
                abort_waiter.cancel()
                await asyncio.gather(
                    joined,
                    failure_waiter,
                    abort_waiter,
                    return_exceptions=True,
                )
                raise

            if self.shutdown_controller.abort_requested:
                joined.cancel()
                await asyncio.gather(joined, return_exceptions=True)
                raise _ForcedShutdown

            if failure_event.is_set():
                joined.cancel()
                await asyncio.gather(
                    joined,
                    return_exceptions=True,
                )
                raise _make_concurrent_runtime_error(
                    worker_errors
                )

        try:
            await fail_fast_tasks(
                [
                    tasks[0],
                    *tasks[1:1 + crawl_workers],
                ]
            )
            for _ in range(self.writer_workers):
                await record_queue.put(sentinel)
            await fail_fast_tasks(
                [
                    *tasks[
                        1 + crawl_workers:
                        1 + crawl_workers + self.writer_workers
                    ]
                ]
            )
            for batch in self.pipeline.buffer.flush_all():
                self._track_batch(batch)
                await checked_put(
                    upload_queue,
                    _UploadJob(batch=batch),
                    "upload_queue_put_waits",
                )
                remember_depths()
            for _ in range(self.upload_workers):
                await upload_queue.put(sentinel)
            await fail_fast_tasks(
                [
                    *tasks[
                        1 + crawl_workers + self.writer_workers:
                        1 + crawl_workers + self.writer_workers + self.upload_workers
                    ]
                ]
            )
            for _ in range(self.catalog_workers):
                await catalog_queue.put(sentinel)
            await fail_fast_tasks(
                [
                    *tasks[
                        1 + crawl_workers + self.writer_workers + self.upload_workers:
                    ]
                ]
            )
        except BaseException as exc:
            if not isinstance(exc, _ForcedShutdown):
                self.stats.errors += 1
            for task in tasks:
                task.cancel()
            progress_task.cancel()
            await asyncio.gather(
                *tasks,
                progress_task,
                return_exceptions=True,
            )
            if isinstance(exc, _ForcedShutdown):
                self._interrupted = True
                self._forced_shutdown = True
            elif worker_errors:
                raise _make_concurrent_runtime_error(
                    worker_errors
                )
            else:
                raise

        progress_task.cancel()
        await asyncio.gather(
            progress_task,
            return_exceptions=True,
        )

        if self.progress_aggregator is not None:
            self.progress_aggregator.observe_queues(
                self.plugin.site_id,
                dataset,
                record_current=record_queue.qsize(),
                upload_current=upload_queue.qsize(),
                catalog_current=catalog_queue.qsize(),
            )

        if not self._forced_shutdown:
            self.stats.datasets_finished += 1
        results = list(self._results)
        self._results.clear()
        self._states.clear()
        return results

    def _crawl_workers_for_dataset(
        self,
        dataset: str,
    ) -> int:
        extra = getattr(
            self.context,
            "extra",
            {},
        )

        if not isinstance(
            extra,
            dict,
        ):

            return self.crawl_workers

        budgets = extra.get(
            "dataset_budgets"
        )

        if not isinstance(
            budgets,
            dict,
        ):

            return self.crawl_workers

        budget = budgets.get(
            dataset
        )

        value = None

        if isinstance(
            budget,
            DatasetResourceBudget,
        ):

            value = budget.crawl_workers

        elif isinstance(
            budget,
            dict,
        ):

            value = budget.get(
                "crawl_workers"
            )

        if value is None:

            return self.crawl_workers

        value = int(
            value
        )

        if value < 1:

            raise ValueError(
                "dataset crawl_workers must be >= 1"
            )

        return value

    def _stage_slot(self, stage: str):
        if self.stage_budget is None:
            return _unlimited_stage_slot()
        return getattr(self.stage_budget, f"{stage}_slot")()

    def _maintenance_slot(self):
        if self.stage_budget is None:
            return _unlimited_stage_slot()
        return self.stage_budget.maintenance_slot()

    def _begin_scope(
        self,
        dataset: str,
        scope: CrawlScope,
        *,
        checkpoint: CrawlCheckpoint | None = None,
    ) -> _ScopeState:
        self.stats.scopes_started += 1
        if self.progress_aggregator is not None:
            self.progress_aggregator.scope_started(
                self.plugin.site_id,
                dataset,
            )
        scope_token = make_scope_token(
            site_id=self.plugin.site_id,
            dataset=dataset,
            scope=scope,
        )
        checkpoint_key = checkpoint_key_for_scope(
            site_id=self.plugin.site_id,
            dataset=dataset,
            scope=scope,
        )
        if checkpoint is None:
            checkpoint = self.checkpoint_store.load(checkpoint_key)
            self.stats.checkpoints_loaded += 1
        state = _ScopeState(
            dataset=dataset,
            scope=scope,
            scope_token=scope_token,
            checkpoint_key=checkpoint_key,
            checkpoint=checkpoint,
            candidate_checkpoint=checkpoint,
        )
        self._states[scope_token] = state
        return state

    def _record_durable_resume_skip(
        self,
        item: ResumePlanItem,
    ) -> None:
        scope = item.scope.scope
        self.stats.scopes_finished += 1
        self._results.append(
            ScopeRunResult(
                dataset=item.scope.dataset,
                scope_type=scope.scope_type,
                scope_id=scope.scope_id,
                source_key=scope.source_key,
                scope_token=item.scope.scope_token,
                raw_count=0,
                normalized_count=0,
                skipped_count=0,
                checkpoint_state=dict(item.evidence.checkpoint_state),
                pipeline=ScopePipelineStats(),
            )
        )

    def _track_batch(self, batch: FlushBatch) -> None:
        for scope_token in batch.scope_tokens:
            state = self._states.get(scope_token)
            if state is not None:
                state.pending_batches += 1

    def _complete_batch(
        self,
        batch: FlushBatch,
        result: BatchProcessResult,
    ) -> None:
        del result
        for scope_token in batch.scope_tokens:
            state = self._states.get(scope_token)
            if state is None:
                continue
            state.pending_batches -= 1
            self._complete_scope_if_ready(state)

    def _complete_scope_if_ready(
        self,
        state: _ScopeState,
    ) -> None:
        if state.result is not None:
            return
        if not state.crawl_done:
            return
        if state.pending_batches > 0:
            return
        has_scope = getattr(
            self.pipeline.buffer,
            "has_scope",
            None,
        )
        if callable(has_scope) and has_scope(
            state.scope_token
        ):
            return

        if state.candidate_checkpoint is not None:
            self.checkpoint_store.save(
                state.checkpoint_key,
                state.candidate_checkpoint,
            )
            self.stats.checkpoints_saved += 1

        checkpoint_state: dict[str, Any] = {}
        if (
            state.candidate_checkpoint is not None
            and isinstance(state.candidate_checkpoint.state, dict)
        ):
            checkpoint_state = dict(state.candidate_checkpoint.state)

        state.result = ScopeRunResult(
            dataset=state.dataset,
            scope_type=state.scope.scope_type,
            scope_id=state.scope.scope_id,
            source_key=state.scope.source_key,
            scope_token=state.scope_token,
            raw_count=state.raw_count,
            normalized_count=state.normalized_count,
            skipped_count=state.skipped_count,
            checkpoint_state=checkpoint_state,
            pipeline=ScopePipelineStats(),
        )
        self.stats.scopes_finished += 1
        if self.progress_aggregator is not None:
            if state.interrupted:
                self.progress_aggregator.scope_interrupted(
                    self.plugin.site_id,
                    state.dataset,
                )
            else:
                self.progress_aggregator.scope_completed(
                    self.plugin.site_id,
                    state.dataset,
                )
        self._results.append(state.result)

    def _record_compaction_progress(self, dataset: str, result: Any) -> None:
        if self.progress_aggregator is None or result is None:
            return
        replacement_files = int(getattr(result, "replacement_files", 0) or 0)
        superseded = getattr(result, "source_files_superseded", None)
        if superseded is None:
            superseded = getattr(result, "source_files", 0)
            if not isinstance(superseded, int):
                try:
                    superseded = len(superseded)
                except TypeError:
                    superseded = 0
        self.progress_aggregator.storage_activity(
            self.plugin.site_id,
            dataset,
            files_compacted=replacement_files,
            replacement_files=replacement_files,
            source_files_superseded=int(superseded or 0),
        )


def _has_overlap(
    left: list[tuple[float, float]],
    right: list[tuple[float, float]],
) -> bool:
    for left_start, left_end in left:
        for right_start, right_end in right:
            if left_start < right_end and right_start < left_end:
                return True
    return False


@asynccontextmanager
async def _unlimited_stage_slot():
    yield


def _make_concurrent_runtime_error(
    errors: list[BaseException],
) -> ConcurrentRuntimeError:
    if not errors:
        return ConcurrentRuntimeError(
            "concurrent production runtime failed"
        )

    first = errors[0]

    message = (
        "concurrent production runtime failed: "
        f"{type(first).__name__}: {first}"
    )

    if len(errors) > 1:

        message += (
            f" ({len(errors)} worker errors)"
        )

    return ConcurrentRuntimeError(
        message
    )
