from __future__ import annotations

from dataclasses import dataclass
from datetime import (
    datetime,
    timedelta,
    timezone,
)
from typing import Literal


RetentionDecision = Literal[
    "keep",
    "delete",
    "archive",
]


@dataclass(
    frozen=True,
    slots=True,
)
class RetentionPolicy:
    """
    Explicit retention knobs for storage maintenance.
    """

    dry_run: bool = True

    keep_failures: bool = True

    manifest_retention_days: int = 30

    superseded_rollback_days: int = 30

    archive_before_delete: bool = True


@dataclass(
    frozen=True,
    slots=True,
)
class RetentionCandidate:
    path: str

    kind: str

    created_at: datetime

    uploaded: bool = False

    verified: bool = False

    catalog_registered: bool = False

    seen_committed: bool = False

    checkpoint_committed: bool = False

    failed: bool = False

    lifecycle_status: str = "active"


@dataclass(
    frozen=True,
    slots=True,
)
class RetentionEvaluation:
    candidate: RetentionCandidate

    decision: RetentionDecision

    allowed: bool

    reason: str


def _age_days(
    *,
    now: datetime,
    created_at: datetime,
) -> int:

    if created_at.tzinfo is None:

        created_at = created_at.replace(
            tzinfo=timezone.utc
        )

    if now.tzinfo is None:

        now = now.replace(
            tzinfo=timezone.utc
        )

    return int(
        (
            now
            -
            created_at
        ).total_seconds()
        //
        86_400
    )


def _durability_complete(
    candidate: RetentionCandidate,
) -> bool:

    return (
        candidate.uploaded
        and
        candidate.verified
        and
        candidate.catalog_registered
        and
        candidate.seen_committed
        and
        candidate.checkpoint_committed
    )


def evaluate_retention(
    candidate: RetentionCandidate,
    *,
    policy: RetentionPolicy,
    now: datetime | None = None,
) -> RetentionEvaluation:
    """
    Pure retention decision. It never mutates or deletes files.
    """

    if now is None:

        now = datetime.now(
            timezone.utc
        )

    if (
        candidate.failed
        and
        policy.keep_failures
    ):

        return RetentionEvaluation(
            candidate=candidate,
            decision="keep",
            allowed=False,
            reason="failed artifact retained for inspection",
        )

    if not _durability_complete(
        candidate
    ):

        return RetentionEvaluation(
            candidate=candidate,
            decision="keep",
            allowed=False,
            reason="durability barrier incomplete",
        )

    age_days = _age_days(
        now=now,
        created_at=candidate.created_at,
    )

    if candidate.kind in {
        "manifest",
        "sidecar",
    }:

        if age_days < policy.manifest_retention_days:

            return RetentionEvaluation(
                candidate=candidate,
                decision="keep",
                allowed=False,
                reason="manifest retention window not elapsed",
            )

    if candidate.lifecycle_status == "superseded":

        if age_days < policy.superseded_rollback_days:

            return RetentionEvaluation(
                candidate=candidate,
                decision="keep",
                allowed=False,
                reason="superseded rollback window not elapsed",
            )

        if policy.archive_before_delete:

            return RetentionEvaluation(
                candidate=candidate,
                decision="archive",
                allowed=not policy.dry_run,
                reason="archive superseded artifact before deletion",
            )

    return RetentionEvaluation(
        candidate=candidate,
        decision="delete",
        allowed=not policy.dry_run,
        reason=(
            "dry-run deletion candidate"
            if policy.dry_run
            else
            "retention policy permits deletion"
        ),
    )
