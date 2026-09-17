from __future__ import annotations

import threading
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any

from crawl_framework.core.resume import ResumePlan, ResumeStatus


class ProgressStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    BLOCKED = "BLOCKED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class RecordProgress:
    crawled: int = 0
    normalized: int = 0
    new: int = 0
    unchanged: int = 0
    updated: int = 0
    failed: int = 0


@dataclass(frozen=True, slots=True)
class StorageProgress:
    buffered_rows: int = 0
    files_prepared: int = 0
    files_written: int = 0
    files_uploaded: int = 0
    files_verified: int = 0
    files_cataloged: int = 0
    active_files: int = 0
    files_compacted: int = 0
    replacement_files: int = 0
    source_files_superseded: int = 0
    uploaded_bytes: int = 0


@dataclass(frozen=True, slots=True)
class QueueProgress:
    record_current: int = 0
    record_max: int = 0
    upload_current: int = 0
    upload_max: int = 0
    catalog_current: int = 0
    catalog_max: int = 0


@dataclass(frozen=True, slots=True)
class RecoveryProgress:
    pending: int = 0
    attempted: int = 0
    recovered: int = 0
    retryable_failed: int = 0
    terminal_failed: int = 0


@dataclass(frozen=True, slots=True)
class PerformanceProgress:
    records_per_second: float = 0.0
    files_per_minute: float = 0.0
    upload_mb_per_second: float = 0.0
    crawl_busy_time: float = 0.0
    write_busy_time: float = 0.0
    upload_busy_time: float = 0.0
    catalog_busy_time: float = 0.0


@dataclass(frozen=True, slots=True)
class DatasetProgressSnapshot:
    site_id: str
    dataset: str
    status: ProgressStatus
    total_scopes: int
    completed_scopes: int
    remaining_scopes: int
    percent: float
    running_scopes: int
    skipped_scopes: int
    skipped_due_to_checkpoint: int
    blocked_scopes: int
    failed_scopes: int
    recovery_pending_scopes: int
    records: RecordProgress
    storage: StorageProgress
    queues: QueueProgress
    recovery: RecoveryProgress
    performance: PerformanceProgress


@dataclass(frozen=True, slots=True)
class SiteProgressSnapshot:
    site_id: str
    status: ProgressStatus
    total_scopes: int
    completed_scopes: int
    remaining_scopes: int
    percent: float
    datasets: tuple[DatasetProgressSnapshot, ...]


@dataclass(frozen=True, slots=True)
class PlatformProgressSnapshot:
    profile: str | None
    elapsed_seconds: float
    status: ProgressStatus
    total_scopes: int
    completed_scopes: int
    remaining_scopes: int
    percent: float
    estimated_remaining_scopes: int
    eta_seconds: float | None
    sites: tuple[SiteProgressSnapshot, ...]
    recovery: RecoveryProgress

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["status"] = self.status.value
        for site in payload["sites"]:
            site["status"] = site["status"].value
            for dataset in site["datasets"]:
                dataset["status"] = dataset["status"].value
        return payload


@dataclass(slots=True)
class _DatasetState:
    site_id: str
    dataset: str
    total_scopes: int = 0
    durable_complete: int = 0
    runtime_completed: int = 0
    running_scopes: int = 0
    blocked_scopes: int = 0
    failed_scopes: int = 0
    recovery_pending_scopes: int = 0
    session_completed: int = 0
    records: dict[str, int] = field(default_factory=dict)
    storage: dict[str, int] = field(default_factory=dict)
    queues: dict[str, int] = field(default_factory=dict)
    recovery: dict[str, int] = field(default_factory=dict)
    busy: dict[str, float] = field(default_factory=dict)


class ProgressAggregator:
    """Thread-safe, renderer-independent production progress model."""

    def __init__(
        self,
        *,
        profile: str | None = None,
        clock=time.monotonic,
    ) -> None:
        self.profile = profile
        self._clock = clock
        self._started_at = float(clock())
        self._states: dict[tuple[str, str], _DatasetState] = {}
        self._global_recovery = {
            "pending": 0,
            "attempted": 0,
            "recovered": 0,
            "retryable_failed": 0,
            "terminal_failed": 0,
        }
        self._lock = threading.RLock()

    def seed_resume_plan(self, plan: ResumePlan) -> None:
        grouped: dict[tuple[str, str], list] = {}
        for item in plan.items:
            key = (item.scope.site_id, item.scope.dataset)
            grouped.setdefault(key, []).append(item)

        with self._lock:
            for key, items in grouped.items():
                state = self._state(*key)
                state.total_scopes = len(items)
                state.durable_complete = sum(
                    item.status is ResumeStatus.DURABLE_COMPLETE
                    for item in items
                )
                state.recovery_pending_scopes = sum(
                    item.status is ResumeStatus.RECOVERY_PENDING
                    for item in items
                )
                state.blocked_scopes = sum(
                    item.status is ResumeStatus.BLOCKED
                    for item in items
                )
                state.failed_scopes = sum(
                    item.status
                    in {
                        ResumeStatus.FAILED_RETRYABLE,
                        ResumeStatus.FAILED_TERMINAL,
                    }
                    for item in items
                )

    def scope_started(self, site_id: str, dataset: str, count: int = 1) -> None:
        with self._lock:
            state = self._state(site_id, dataset)
            state.running_scopes += self._positive(count)

    def scope_completed(self, site_id: str, dataset: str, count: int = 1) -> None:
        value = self._positive(count)
        with self._lock:
            state = self._state(site_id, dataset)
            state.running_scopes = max(0, state.running_scopes - value)
            capacity = max(0, state.total_scopes - state.durable_complete)
            accepted = min(value, max(0, capacity - state.runtime_completed))
            state.runtime_completed += accepted
            state.session_completed += accepted

    def scope_failed(self, site_id: str, dataset: str, count: int = 1) -> None:
        value = self._positive(count)
        with self._lock:
            state = self._state(site_id, dataset)
            state.running_scopes = max(0, state.running_scopes - value)
            state.failed_scopes = min(
                state.total_scopes,
                state.failed_scopes + value,
            )

    def scope_interrupted(
        self,
        site_id: str,
        dataset: str,
        count: int = 1,
    ) -> None:
        """Return an interrupted in-flight scope to the remaining population."""

        value = self._positive(count)
        with self._lock:
            state = self._state(site_id, dataset)
            state.running_scopes = max(0, state.running_scopes - value)

    def record_activity(self, site_id: str, dataset: str, **values: int) -> None:
        self._add_metrics(
            site_id,
            dataset,
            "records",
            values,
            {"crawled", "normalized", "new", "unchanged", "updated", "failed"},
        )

    def storage_activity(self, site_id: str, dataset: str, **values: int) -> None:
        self._add_metrics(
            site_id,
            dataset,
            "storage",
            values,
            {
                "buffered_rows",
                "files_prepared",
                "files_written",
                "files_uploaded",
                "files_verified",
                "files_cataloged",
                "active_files",
                "files_compacted",
                "replacement_files",
                "source_files_superseded",
                "uploaded_bytes",
            },
        )

    def observe_queues(
        self,
        site_id: str,
        dataset: str,
        *,
        record_current: int,
        upload_current: int,
        catalog_current: int,
    ) -> None:
        with self._lock:
            queues = self._state(site_id, dataset).queues
            for name, value in (
                ("record", record_current),
                ("upload", upload_current),
                ("catalog", catalog_current),
            ):
                current = max(0, int(value))
                queues[f"{name}_current"] = current
                queues[f"{name}_max"] = max(
                    queues.get(f"{name}_max", 0),
                    current,
                )

    def busy_time(
        self,
        site_id: str,
        dataset: str,
        **values: float,
    ) -> None:
        allowed = {"crawl", "write", "upload", "catalog"}
        unknown = set(values) - allowed
        if unknown:
            raise ValueError(f"unknown busy-time metrics: {sorted(unknown)}")
        with self._lock:
            busy = self._state(site_id, dataset).busy
            for name, value in values.items():
                busy[name] = busy.get(name, 0.0) + max(0.0, float(value))

    def set_startup_recovery(
        self,
        *,
        pending: int,
        attempted: int,
        recovered: int,
        retryable_failed: int,
        terminal_failed: int,
    ) -> None:
        with self._lock:
            self._global_recovery = {
                "pending": max(0, int(pending)),
                "attempted": max(0, int(attempted)),
                "recovered": max(0, int(recovered)),
                "retryable_failed": max(0, int(retryable_failed)),
                "terminal_failed": max(0, int(terminal_failed)),
            }

    def snapshot(self) -> PlatformProgressSnapshot:
        with self._lock:
            elapsed = max(0.0, float(self._clock()) - self._started_at)
            datasets = tuple(
                self._dataset_snapshot(state, elapsed)
                for _, state in sorted(self._states.items())
            )
            sites = self._site_snapshots(datasets)
            total = sum(site.total_scopes for site in sites)
            completed = sum(site.completed_scopes for site in sites)
            remaining = max(0, total - completed)
            session_completed = sum(
                state.session_completed for state in self._states.values()
            )
            eta = None
            if session_completed > 0 and elapsed > 0 and remaining > 0:
                eta = remaining / (session_completed / elapsed)
            status = self._aggregate_status(
                tuple(site.status for site in sites),
                total=total,
                completed=completed,
            )
            return PlatformProgressSnapshot(
                profile=self.profile,
                elapsed_seconds=elapsed,
                status=status,
                total_scopes=total,
                completed_scopes=completed,
                remaining_scopes=remaining,
                percent=self._percent(completed, total),
                estimated_remaining_scopes=remaining,
                eta_seconds=eta,
                sites=sites,
                recovery=RecoveryProgress(**self._global_recovery),
            )

    def _dataset_snapshot(
        self,
        state: _DatasetState,
        elapsed: float,
    ) -> DatasetProgressSnapshot:
        completed = min(
            state.total_scopes,
            state.durable_complete + state.runtime_completed,
        )
        remaining = max(0, state.total_scopes - completed)
        records = RecordProgress(**{
            name: state.records.get(name, 0)
            for name in RecordProgress.__dataclass_fields__
        })
        storage = StorageProgress(**{
            name: state.storage.get(name, 0)
            for name in StorageProgress.__dataclass_fields__
        })
        queues = QueueProgress(**{
            name: state.queues.get(name, 0)
            for name in QueueProgress.__dataclass_fields__
        })
        recovery = RecoveryProgress(**{
            name: state.recovery.get(name, 0)
            for name in RecoveryProgress.__dataclass_fields__
        })
        performance = PerformanceProgress(
            records_per_second=records.crawled / elapsed if elapsed > 0 else 0.0,
            files_per_minute=(
                storage.files_written * 60.0 / elapsed if elapsed > 0 else 0.0
            ),
            upload_mb_per_second=(
                storage.uploaded_bytes / (1024 * 1024) / elapsed
                if elapsed > 0
                else 0.0
            ),
            crawl_busy_time=state.busy.get("crawl", 0.0),
            write_busy_time=state.busy.get("write", 0.0),
            upload_busy_time=state.busy.get("upload", 0.0),
            catalog_busy_time=state.busy.get("catalog", 0.0),
        )
        if state.failed_scopes:
            status = ProgressStatus.FAILED
        elif state.blocked_scopes and not state.running_scopes:
            status = ProgressStatus.BLOCKED
        elif completed >= state.total_scopes and state.total_scopes:
            status = ProgressStatus.COMPLETE
        elif state.running_scopes or state.runtime_completed:
            status = ProgressStatus.RUNNING
        else:
            status = ProgressStatus.PENDING
        return DatasetProgressSnapshot(
            site_id=state.site_id,
            dataset=state.dataset,
            status=status,
            total_scopes=state.total_scopes,
            completed_scopes=completed,
            remaining_scopes=remaining,
            percent=self._percent(completed, state.total_scopes),
            running_scopes=state.running_scopes,
            skipped_scopes=state.durable_complete,
            skipped_due_to_checkpoint=state.durable_complete,
            blocked_scopes=state.blocked_scopes,
            failed_scopes=state.failed_scopes,
            recovery_pending_scopes=state.recovery_pending_scopes,
            records=records,
            storage=storage,
            queues=queues,
            recovery=recovery,
            performance=performance,
        )

    def _site_snapshots(
        self,
        datasets: tuple[DatasetProgressSnapshot, ...],
    ) -> tuple[SiteProgressSnapshot, ...]:
        grouped: dict[str, list[DatasetProgressSnapshot]] = {}
        for dataset in datasets:
            grouped.setdefault(dataset.site_id, []).append(dataset)
        result = []
        for site_id, values in sorted(grouped.items()):
            total = sum(item.total_scopes for item in values)
            completed = sum(item.completed_scopes for item in values)
            result.append(
                SiteProgressSnapshot(
                    site_id=site_id,
                    status=self._aggregate_status(
                        tuple(item.status for item in values),
                        total=total,
                        completed=completed,
                    ),
                    total_scopes=total,
                    completed_scopes=completed,
                    remaining_scopes=max(0, total - completed),
                    percent=self._percent(completed, total),
                    datasets=tuple(values),
                )
            )
        return tuple(result)

    def _state(self, site_id: str, dataset: str) -> _DatasetState:
        key = (str(site_id).strip(), str(dataset).strip())
        if not all(key):
            raise ValueError("site_id and dataset cannot be empty")
        if key not in self._states:
            self._states[key] = _DatasetState(*key)
        return self._states[key]

    def _add_metrics(
        self,
        site_id: str,
        dataset: str,
        target: str,
        values: dict[str, int],
        allowed: set[str],
    ) -> None:
        unknown = set(values) - allowed
        if unknown:
            raise ValueError(f"unknown {target} metrics: {sorted(unknown)}")
        with self._lock:
            metrics = getattr(self._state(site_id, dataset), target)
            for name, value in values.items():
                metrics[name] = metrics.get(name, 0) + self._positive(value)

    @staticmethod
    def _positive(value: int) -> int:
        value = int(value)
        if value < 0:
            raise ValueError("progress increments cannot be negative")
        return value

    @staticmethod
    def _percent(completed: int, total: int) -> float:
        if total <= 0:
            return 0.0
        return min(100.0, max(0.0, completed * 100.0 / total))

    @staticmethod
    def _aggregate_status(
        statuses: tuple[ProgressStatus, ...],
        *,
        total: int,
        completed: int,
    ) -> ProgressStatus:
        if ProgressStatus.FAILED in statuses:
            return ProgressStatus.FAILED
        if (
            ProgressStatus.BLOCKED in statuses
            and all(
                item in {ProgressStatus.BLOCKED, ProgressStatus.COMPLETE}
                for item in statuses
            )
        ):
            return ProgressStatus.BLOCKED
        if total > 0 and completed >= total:
            return ProgressStatus.COMPLETE
        if ProgressStatus.RUNNING in statuses:
            return ProgressStatus.RUNNING
        return ProgressStatus.PENDING
