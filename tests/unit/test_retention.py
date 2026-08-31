from __future__ import annotations

from datetime import (
    datetime,
    timedelta,
    timezone,
)

from crawl_framework.storage.retention import (
    RetentionCandidate,
    RetentionPolicy,
    evaluate_retention,
)


UTC = timezone.utc


def complete_candidate(
    *,
    created_at,
    kind="parquet",
    lifecycle_status="active",
    failed=False,
):

    return RetentionCandidate(
        path="file.parquet",
        kind=kind,
        created_at=created_at,
        uploaded=True,
        verified=True,
        catalog_registered=True,
        seen_committed=True,
        checkpoint_committed=True,
        failed=failed,
        lifecycle_status=lifecycle_status,
    )


def test_retention_keeps_when_durability_barrier_incomplete():

    now = datetime(
        2026,
        8,
        27,
        tzinfo=UTC,
    )

    candidate = RetentionCandidate(
        path="unsafe.parquet",
        kind="parquet",
        created_at=(
            now
            -
            timedelta(
                days=100
            )
        ),
        uploaded=True,
        verified=True,
        catalog_registered=True,
        seen_committed=True,
        checkpoint_committed=False,
    )

    result = evaluate_retention(
        candidate,
        policy=RetentionPolicy(
            dry_run=False
        ),
        now=now,
    )

    assert result.decision == "keep"
    assert result.allowed is False
    assert result.reason == "durability barrier incomplete"


def test_retention_keeps_failures_by_default():

    now = datetime(
        2026,
        8,
        27,
        tzinfo=UTC,
    )

    result = evaluate_retention(
        complete_candidate(
            created_at=(
                now
                -
                timedelta(
                    days=100
                )
            ),
            failed=True,
        ),
        policy=RetentionPolicy(
            dry_run=False
        ),
        now=now,
    )

    assert result.decision == "keep"
    assert result.reason == "failed artifact retained for inspection"


def test_retention_keeps_recent_manifests():

    now = datetime(
        2026,
        8,
        27,
        tzinfo=UTC,
    )

    result = evaluate_retention(
        complete_candidate(
            created_at=(
                now
                -
                timedelta(
                    days=5
                )
            ),
            kind="manifest",
        ),
        policy=RetentionPolicy(
            manifest_retention_days=30,
            dry_run=False,
        ),
        now=now,
    )

    assert result.decision == "keep"
    assert result.allowed is False


def test_retention_archives_superseded_after_rollback_window():

    now = datetime(
        2026,
        8,
        27,
        tzinfo=UTC,
    )

    result = evaluate_retention(
        complete_candidate(
            created_at=(
                now
                -
                timedelta(
                    days=60
                )
            ),
            lifecycle_status="superseded",
        ),
        policy=RetentionPolicy(
            superseded_rollback_days=30,
            archive_before_delete=True,
            dry_run=False,
        ),
        now=now,
    )

    assert result.decision == "archive"
    assert result.allowed is True


def test_retention_dry_run_never_allows_delete():

    now = datetime(
        2026,
        8,
        27,
        tzinfo=UTC,
    )

    result = evaluate_retention(
        complete_candidate(
            created_at=(
                now
                -
                timedelta(
                    days=100
                )
            ),
        ),
        policy=RetentionPolicy(
            dry_run=True
        ),
        now=now,
    )

    assert result.decision == "delete"
    assert result.allowed is False
    assert result.reason == "dry-run deletion candidate"
