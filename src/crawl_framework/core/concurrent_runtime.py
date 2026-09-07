from __future__ import annotations

import asyncio
import time

from dataclasses import dataclass, field
from typing import Any, Callable

from crawl_framework.core.concurrency import (
    DatasetResourceBudget,
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
from crawl_framework.storage.buffer import (
    FlushBatch,
)
from crawl_framework.storage.checkpoint import (
    CheckpointKey,
    CheckpointStore,
)


class ConcurrentRuntimeError(
    RuntimeError
):
    """
    One or more concurrent production workers failed.
    """


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
    result: ScopeRunResult | None = None


@dataclass(frozen=True, slots=True)
class _RecordItem:
    state: _ScopeState
    raw: Any


@dataclass(frozen=True, slots=True)
class _FlushScopeItem:
    state: _ScopeState


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
        post_dataset_hook: Callable[[str], Any] | None = None,
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
        self.post_dataset_hook = post_dataset_hook
        self.stats = RuntimeStats()
        self.production_stats = ProductionPipelineStats(
            crawl_workers=self.crawl_workers,
            writer_workers=self.writer_workers,
            upload_workers=self.upload_workers,
            catalog_workers=self.catalog_workers,
        )
        self.post_dataset_results: list[Any] = []
        self._states: dict[str, _ScopeState] = {}
        self._results: list[ScopeRunResult] = []

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
            for dataset in selected:
                all_results.extend(
                    await self.run_dataset(dataset)
                )
                if self.post_dataset_hook is not None:
                    self.post_dataset_results.append(
                        await asyncio.to_thread(
                            self.post_dataset_hook,
                            dataset,
                        )
                    )
        finally:
            if flush_at_end:
                for batch in self.pipeline.buffer.flush_all():
                    prepared = await asyncio.to_thread(
                        self.pipeline.prepare_batch,
                        batch,
                    )
                    uploaded = await asyncio.to_thread(
                        self.pipeline.upload_prepared_batch,
                        prepared,
                    )
                    await asyncio.to_thread(
                        self.pipeline.catalog_uploaded_batch,
                        uploaded,
                    )

        return {
            "runtime": all_results,
            "production_stats": self.production_stats,
            "post_dataset": self.post_dataset_results,
            "call_graph": (
                "cli.main -> app_factory -> CrawlBootstrap -> "
                "ConcurrentProductionRuntime -> crawl_queue -> "
                "record_queue -> writer workers -> upload_queue -> "
                "upload workers -> catalog_queue -> catalog workers -> "
                "SeenStore/checkpoint"
            ),
        }

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
        scope_queue: asyncio.Queue[CrawlScope | object] = asyncio.Queue()
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
                async for scope in self.plugin.discover(dataset, self.context):
                    if failure_event.is_set():
                        return
                    self.stats.scopes_discovered += 1
                    await scope_queue.put(scope)
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
                        assert isinstance(scope, CrawlScope)
                        state = self._begin_scope(dataset, scope)
                        start = time.monotonic()
                        try:
                            async for raw in self.plugin.crawl(
                                dataset,
                                scope,
                                state.checkpoint,
                                self.context,
                            ):
                                if failure_event.is_set():
                                    return
                                self.stats.raw_records += 1
                                self.production_stats.records_crawled += 1
                                state.raw_count += 1
                                await checked_put(
                                    record_queue,
                                    _RecordItem(state=state, raw=raw),
                                    "record_queue_put_waits",
                                )
                                remember_depths()
                        finally:
                            end = time.monotonic()
                            self.production_stats.crawl_busy_time += end - start
                            self.production_stats.crawl_intervals.append(
                                (start, end)
                            )
                        await checked_put(
                            record_queue,
                            _FlushScopeItem(state=state),
                            "record_queue_put_waits",
                        )
                        remember_depths()
                    finally:
                        scope_queue.task_done()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await record_worker_error(exc)
                raise

        async def writer_worker() -> None:
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
                                    _, batches = (
                                        self.pipeline.submit_for_staged_runtime(
                                            record,
                                            scope_token=item.state.scope_token,
                                        )
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
                                if self.flush_scope_on_scope_done:
                                    batches = self.pipeline.buffer.flush_scope(
                                        item.state.scope_token
                                    )
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
                        prepared: PreparedBatch = await asyncio.to_thread(
                            self.pipeline.prepare_batch,
                            item.batch,
                        )
                        self.production_stats.files_written += 1
                        self.production_stats.uploads_started += 1
                        uploaded: UploadedBatch = await asyncio.to_thread(
                            self.pipeline.upload_prepared_batch,
                            prepared,
                        )
                        self.production_stats.uploads_completed += 1
                        end = time.monotonic()
                        self.production_stats.upload_busy_time += end - start
                        self.production_stats.upload_intervals.append(
                            (start, end)
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
            done, pending = await asyncio.wait(
                {
                    joined,
                    failure_waiter,
                },
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in pending:
                task.cancel()

            for task in done:
                await task

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
        except Exception:
            self.stats.errors += 1
            for task in tasks:
                task.cancel()
            progress_task.cancel()
            await asyncio.gather(
                *tasks,
                progress_task,
                return_exceptions=True,
            )
            if worker_errors:
                raise _make_concurrent_runtime_error(
                    worker_errors
                )
            raise

        progress_task.cancel()
        await asyncio.gather(
            progress_task,
            return_exceptions=True,
        )

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

    def _begin_scope(
        self,
        dataset: str,
        scope: CrawlScope,
    ) -> _ScopeState:
        self.stats.scopes_started += 1
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
        self._results.append(state.result)


def _has_overlap(
    left: list[tuple[float, float]],
    right: list[tuple[float, float]],
) -> bool:
    for left_start, left_end in left:
        for right_start, right_end in right:
            if left_start < right_end and right_start < left_end:
                return True
    return False


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
