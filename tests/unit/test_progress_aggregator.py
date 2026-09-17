from __future__ import annotations

import json

import pytest

from crawl_framework.core.plugin import CrawlScope
from crawl_framework.core.resume import (
    ResumePlan,
    ResumePlanItem,
    ResumeScope,
    ResumeStatus,
    ScopeResumeEvidence,
)
from crawl_framework.observability.progress import (
    ProgressAggregator,
    ProgressStatus,
)


class Clock:
    def __init__(self) -> None:
        self.value = 100.0

    def __call__(self) -> float:
        return self.value


def item(
    site: str,
    dataset: str,
    source_key: str,
    status: ResumeStatus,
) -> ResumePlanItem:
    scope = ResumeScope(
        site_id=site,
        dataset=dataset,
        scope=CrawlScope(
            scope_type="instrument",
            source_key=source_key,
            scope_id=f"X:{source_key}",
        ),
    )
    evidence = ScopeResumeEvidence(
        durable_complete=status is ResumeStatus.DURABLE_COMPLETE,
        recovery_pending=status is ResumeStatus.RECOVERY_PENDING,
        blocked_reason="remote_waf" if status is ResumeStatus.BLOCKED else None,
        failed_retryable=status is ResumeStatus.FAILED_RETRYABLE,
        failed_terminal=status is ResumeStatus.FAILED_TERMINAL,
    )
    return ResumePlanItem(scope=scope, status=status, evidence=evidence)


def plan(*items: ResumePlanItem) -> ResumePlan:
    return ResumePlan(items=tuple(items))


def dataset(snapshot, site_id: str, dataset_name: str):
    return next(
        entry
        for site in snapshot.sites
        if site.site_id == site_id
        for entry in site.datasets
        if entry.dataset == dataset_name
    )


def test_resume_plan_seeds_restart_progress_instead_of_zero() -> None:
    aggregator = ProgressAggregator(profile="global_full")
    aggregator.seed_resume_plan(
        plan(
            *[
                item(
                    "naver_finance",
                    "forum_post",
                    str(index),
                    (
                        ResumeStatus.DURABLE_COMPLETE
                        if index < 5
                        else ResumeStatus.INCOMPLETE
                    ),
                )
                for index in range(10)
            ]
        )
    )

    snapshot = aggregator.snapshot()
    progress = dataset(snapshot, "naver_finance", "forum_post")

    assert progress.completed_scopes == 5
    assert progress.skipped_due_to_checkpoint == 5
    assert progress.remaining_scopes == 5
    assert progress.percent == 50.0
    assert snapshot.percent == 50.0


def test_progress_is_capped_and_never_negative() -> None:
    aggregator = ProgressAggregator()
    aggregator.seed_resume_plan(
        plan(
            item("site", "news_article", "1", ResumeStatus.DURABLE_COMPLETE),
            item("site", "news_article", "2", ResumeStatus.INCOMPLETE),
        )
    )
    aggregator.scope_started("site", "news_article")
    aggregator.scope_completed("site", "news_article", count=50)
    aggregator.scope_completed("site", "news_article")

    progress = dataset(aggregator.snapshot(), "site", "news_article")
    assert progress.completed_scopes == 2
    assert progress.running_scopes == 0
    assert progress.remaining_scopes == 0
    assert progress.percent == 100.0
    assert progress.status is ProgressStatus.COMPLETE

    with pytest.raises(ValueError, match="cannot be negative"):
        aggregator.scope_started("site", "news_article", count=-1)


def test_all_resume_states_are_visible_in_dataset_counts() -> None:
    statuses = tuple(ResumeStatus)
    aggregator = ProgressAggregator()
    aggregator.seed_resume_plan(
        plan(
            *[
                item("site", "dataset", str(index), status)
                for index, status in enumerate(statuses)
            ]
        )
    )

    progress = dataset(aggregator.snapshot(), "site", "dataset")
    assert progress.total_scopes == 6
    assert progress.completed_scopes == 1
    assert progress.recovery_pending_scopes == 1
    assert progress.blocked_scopes == 1
    assert progress.failed_scopes == 2
    assert progress.status is ProgressStatus.FAILED
    assert 0.0 <= progress.percent <= 100.0


def test_platform_site_dataset_hierarchy_and_blocked_status() -> None:
    aggregator = ProgressAggregator(profile="global_incremental")
    aggregator.seed_resume_plan(
        plan(
            item("naver", "forum_post", "1", ResumeStatus.DURABLE_COMPLETE),
            item("naver", "news_article", "1", ResumeStatus.DURABLE_COMPLETE),
            item("kabutan", "news_article", "month", ResumeStatus.BLOCKED),
        )
    )

    snapshot = aggregator.snapshot()
    statuses = {site.site_id: site.status for site in snapshot.sites}
    assert statuses == {
        "kabutan": ProgressStatus.BLOCKED,
        "naver": ProgressStatus.COMPLETE,
    }
    assert snapshot.status is ProgressStatus.BLOCKED
    assert snapshot.total_scopes == 3
    assert snapshot.completed_scopes == 2


def test_record_storage_queue_recovery_and_performance_metrics() -> None:
    clock = Clock()
    aggregator = ProgressAggregator(profile="test", clock=clock)
    aggregator.seed_resume_plan(
        plan(item("site", "dataset", "1", ResumeStatus.INCOMPLETE))
    )
    aggregator.scope_started("site", "dataset")
    aggregator.record_activity(
        "site",
        "dataset",
        crawled=20,
        normalized=19,
        new=10,
        unchanged=8,
        updated=1,
        failed=1,
    )
    aggregator.storage_activity(
        "site",
        "dataset",
        buffered_rows=11,
        files_prepared=2,
        files_written=2,
        files_uploaded=2,
        files_verified=2,
        files_cataloged=2,
        active_files=2,
        files_compacted=1,
        replacement_files=1,
        source_files_superseded=3,
        uploaded_bytes=20 * 1024 * 1024,
    )
    aggregator.observe_queues(
        "site", "dataset", record_current=2, upload_current=3, catalog_current=1
    )
    aggregator.observe_queues(
        "site", "dataset", record_current=1, upload_current=1, catalog_current=0
    )
    aggregator.busy_time(
        "site", "dataset", crawl=4, write=2, upload=3, catalog=1
    )
    aggregator.set_startup_recovery(
        pending=0,
        attempted=2,
        recovered=2,
        retryable_failed=0,
        terminal_failed=0,
    )
    clock.value += 10
    aggregator.scope_completed("site", "dataset")

    snapshot = aggregator.snapshot()
    progress = dataset(snapshot, "site", "dataset")
    assert progress.records.crawled == 20
    assert progress.records.normalized == 19
    assert progress.storage.files_cataloged == 2
    assert progress.storage.source_files_superseded == 3
    assert progress.queues.record_current == 1
    assert progress.queues.record_max == 2
    assert progress.queues.upload_max == 3
    assert progress.performance.records_per_second == 2.0
    assert progress.performance.files_per_minute == 12.0
    assert progress.performance.upload_mb_per_second == 2.0
    assert progress.performance.crawl_busy_time == 4
    assert snapshot.recovery.recovered == 2
    assert snapshot.eta_seconds is None


def test_eta_requires_session_completion_and_elapsed_time() -> None:
    clock = Clock()
    aggregator = ProgressAggregator(clock=clock)
    aggregator.seed_resume_plan(
        plan(
            *[
                item("site", "dataset", str(index), ResumeStatus.INCOMPLETE)
                for index in range(4)
            ]
        )
    )
    clock.value += 10
    assert aggregator.snapshot().eta_seconds is None

    aggregator.scope_started("site", "dataset")
    aggregator.scope_completed("site", "dataset")
    assert aggregator.snapshot().eta_seconds == 30.0


def test_snapshot_to_dict_is_json_serializable() -> None:
    aggregator = ProgressAggregator(profile="global_full")
    aggregator.seed_resume_plan(
        plan(item("site", "dataset", "1", ResumeStatus.INCOMPLETE))
    )

    payload = aggregator.snapshot().to_dict()

    assert payload["profile"] == "global_full"
    assert payload["sites"][0]["datasets"][0]["status"] == "PENDING"
    json.dumps(payload)
