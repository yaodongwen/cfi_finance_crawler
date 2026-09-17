from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Iterable, Mapping, Protocol

from crawl_framework.core.plugin import CrawlCheckpoint, CrawlScope
from crawl_framework.storage.checkpoint import CheckpointKey, CheckpointStore
from crawl_framework.storage.recovery import RecoveryManifest, RecoveryStore


class ResumeStatus(str, Enum):
    DURABLE_COMPLETE = "DURABLE_COMPLETE"
    RECOVERY_PENDING = "RECOVERY_PENDING"
    INCOMPLETE = "INCOMPLETE"
    BLOCKED = "BLOCKED"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    FAILED_TERMINAL = "FAILED_TERMINAL"


class RecoveryNextAction(str, Enum):
    UPLOAD_AND_VERIFY = "UPLOAD_AND_VERIFY"
    VERIFY_REMOTE = "VERIFY_REMOTE"
    REGISTER_CATALOG = "REGISTER_CATALOG"
    COMMIT_SEEN = "COMMIT_SEEN"
    ADVANCE_CHECKPOINT_MARKER = "ADVANCE_CHECKPOINT_MARKER"
    CLEAN_LOCAL = "CLEAN_LOCAL"
    RETRY = "RETRY"
    STOP_TERMINAL = "STOP_TERMINAL"
    NONE = "NONE"


@dataclass(frozen=True, slots=True)
class ResumeScope:
    site_id: str
    dataset: str
    scope: CrawlScope

    def __post_init__(self) -> None:
        site_id = str(self.site_id).strip()
        dataset = str(self.dataset).strip()
        if not site_id:
            raise ValueError("site_id cannot be empty")
        if not dataset:
            raise ValueError("dataset cannot be empty")
        object.__setattr__(self, "site_id", site_id)
        object.__setattr__(self, "dataset", dataset)

    @property
    def checkpoint_key(self) -> CheckpointKey:
        return CheckpointKey(
            site_id=self.site_id,
            dataset=self.dataset,
            scope_type=self.scope.scope_type,
            source_key=self.scope.source_key,
        )

    @property
    def scope_token(self) -> str:
        return "|".join(
            (
                self.site_id,
                self.dataset,
                self.scope.scope_type,
                self.scope.source_key,
            )
        )


@dataclass(frozen=True, slots=True)
class ScopeResumeEvidence:
    """Read-only durable evidence for one planned scope."""

    checkpoint_exists: bool = False
    checkpoint_state: Mapping[str, Any] = field(default_factory=dict)
    durable_complete: bool = False
    recovery_pending: bool = False
    failed_retryable: bool = False
    failed_terminal: bool = False
    blocked_reason: str | None = None
    detail: str | None = None
    recovery_manifest_ids: tuple[str, ...] = ()
    recovery_actions: tuple[RecoveryNextAction, ...] = ()

    def __post_init__(self) -> None:
        failure_count = int(self.failed_retryable) + int(self.failed_terminal)
        if failure_count > 1:
            raise ValueError(
                "resume evidence cannot be both retryable and terminal failed"
            )
        object.__setattr__(self, "checkpoint_state", dict(self.checkpoint_state))
        object.__setattr__(
            self,
            "recovery_manifest_ids",
            tuple(self.recovery_manifest_ids),
        )
        object.__setattr__(
            self,
            "recovery_actions",
            tuple(self.recovery_actions),
        )
        if self.blocked_reason is not None:
            reason = str(self.blocked_reason).strip()
            object.__setattr__(self, "blocked_reason", reason or None)


class ResumeEvidenceProvider(Protocol):
    def inspect(self, scope: ResumeScope) -> ScopeResumeEvidence:
        """Return evidence without mutating checkpoint or recovery state."""


CompletionPredicate = Callable[[ResumeScope, CrawlCheckpoint], bool]
RecoveryInspector = Callable[[ResumeScope], ScopeResumeEvidence | None]
BlockedInspector = Callable[[ResumeScope], str | None]


class CheckpointResumeEvidenceProvider:
    """Adapt existing checkpoint stores to the generic planning interface.

    Checkpoint state remains site-owned, so durable completion is deliberately
    supplied as a predicate. Recovery and blocked-state inspectors are optional
    read-only adapters; absent evidence is never guessed.
    """

    def __init__(
        self,
        *,
        checkpoint_store: CheckpointStore,
        is_durable_complete: CompletionPredicate,
        inspect_recovery: RecoveryInspector | None = None,
        inspect_blocked: BlockedInspector | None = None,
    ) -> None:
        self.checkpoint_store = checkpoint_store
        self.is_durable_complete = is_durable_complete
        self.inspect_recovery = inspect_recovery
        self.inspect_blocked = inspect_blocked

    def inspect(self, scope: ResumeScope) -> ScopeResumeEvidence:
        key = scope.checkpoint_key
        checkpoint_exists = self.checkpoint_store.exists(key)
        checkpoint = self.checkpoint_store.load(key)
        recovery = (
            self.inspect_recovery(scope)
            if self.inspect_recovery is not None
            else None
        )
        blocked_reason = (
            self.inspect_blocked(scope)
            if self.inspect_blocked is not None
            else None
        )

        return ScopeResumeEvidence(
            checkpoint_exists=checkpoint_exists,
            checkpoint_state=checkpoint.state,
            durable_complete=bool(self.is_durable_complete(scope, checkpoint)),
            recovery_pending=(recovery.recovery_pending if recovery else False),
            failed_retryable=(recovery.failed_retryable if recovery else False),
            failed_terminal=(recovery.failed_terminal if recovery else False),
            blocked_reason=blocked_reason or (
                recovery.blocked_reason if recovery else None
            ),
            detail=recovery.detail if recovery else None,
            recovery_manifest_ids=(
                recovery.recovery_manifest_ids if recovery else ()
            ),
            recovery_actions=recovery.recovery_actions if recovery else (),
        )


def recovery_next_action(manifest: RecoveryManifest) -> RecoveryNextAction:
    if manifest.stage == "failed":
        return (
            RecoveryNextAction.STOP_TERMINAL
            if manifest.retryable is False
            else RecoveryNextAction.RETRY
        )
    return {
        "local": RecoveryNextAction.UPLOAD_AND_VERIFY,
        "uploaded": RecoveryNextAction.VERIFY_REMOTE,
        "verified": RecoveryNextAction.REGISTER_CATALOG,
        "catalog_registered": RecoveryNextAction.COMMIT_SEEN,
        "seen_committed": RecoveryNextAction.ADVANCE_CHECKPOINT_MARKER,
        "checkpoint_committed": RecoveryNextAction.CLEAN_LOCAL,
        "cleanable": RecoveryNextAction.CLEAN_LOCAL,
        "deleted": RecoveryNextAction.NONE,
    }[manifest.stage]


class RecoveryStoreResumeInspector:
    """Resolve scope recovery evidence only from persisted scope membership."""

    def __init__(self, store: RecoveryStore) -> None:
        self.store = store

    def __call__(self, scope: ResumeScope) -> ScopeResumeEvidence | None:
        manifests = self.store.list_for_scope(scope.scope_token)
        if not manifests:
            return None

        actions = tuple(recovery_next_action(item) for item in manifests)
        failed = tuple(item for item in manifests if item.stage == "failed")
        failed_terminal = any(item.retryable is False for item in failed)
        failed_retryable = bool(failed) and not failed_terminal
        recovery_pending = any(item.stage != "failed" for item in manifests)

        return ScopeResumeEvidence(
            recovery_pending=recovery_pending,
            failed_retryable=failed_retryable,
            failed_terminal=failed_terminal,
            detail=", ".join(
                f"{item.manifest_id}:{action.value}"
                for item, action in zip(manifests, actions)
            ),
            recovery_manifest_ids=tuple(item.manifest_id for item in manifests),
            recovery_actions=actions,
        )


@dataclass(frozen=True, slots=True)
class ResumePlanItem:
    scope: ResumeScope
    status: ResumeStatus
    evidence: ScopeResumeEvidence

    @property
    def should_crawl(self) -> bool:
        return self.status in {
            ResumeStatus.INCOMPLETE,
            ResumeStatus.FAILED_RETRYABLE,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "site_id": self.scope.site_id,
            "dataset": self.scope.dataset,
            "scope_type": self.scope.scope.scope_type,
            "scope_id": self.scope.scope.scope_id,
            "source_key": self.scope.scope.source_key,
            "scope_token": self.scope.scope_token,
            "status": self.status.value,
            "should_crawl": self.should_crawl,
            "checkpoint_exists": self.evidence.checkpoint_exists,
            "blocked_reason": self.evidence.blocked_reason,
            "detail": self.evidence.detail,
            "recovery_manifest_ids": list(
                self.evidence.recovery_manifest_ids
            ),
            "recovery_actions": [
                action.value
                for action in self.evidence.recovery_actions
            ],
        }


@dataclass(frozen=True, slots=True)
class ResumePlan:
    items: tuple[ResumePlanItem, ...]

    @property
    def total_scopes(self) -> int:
        return len(self.items)

    def count(self, status: ResumeStatus) -> int:
        return sum(item.status is status for item in self.items)

    @property
    def durable_complete_scopes(self) -> int:
        return self.count(ResumeStatus.DURABLE_COMPLETE)

    @property
    def recovery_pending_scopes(self) -> int:
        return self.count(ResumeStatus.RECOVERY_PENDING)

    @property
    def incomplete_scopes(self) -> int:
        return self.count(ResumeStatus.INCOMPLETE)

    @property
    def blocked_scopes(self) -> int:
        return self.count(ResumeStatus.BLOCKED)

    @property
    def failed_retryable_scopes(self) -> int:
        return self.count(ResumeStatus.FAILED_RETRYABLE)

    @property
    def failed_terminal_scopes(self) -> int:
        return self.count(ResumeStatus.FAILED_TERMINAL)

    @property
    def crawl_scopes(self) -> tuple[ResumeScope, ...]:
        return tuple(item.scope for item in self.items if item.should_crawl)

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_scopes": self.total_scopes,
            "durable_complete_scopes": self.durable_complete_scopes,
            "recovery_pending_scopes": self.recovery_pending_scopes,
            "incomplete_scopes": self.incomplete_scopes,
            "blocked_scopes": self.blocked_scopes,
            "failed_retryable_scopes": self.failed_retryable_scopes,
            "failed_terminal_scopes": self.failed_terminal_scopes,
            "items": [item.to_dict() for item in self.items],
        }


class ResumePlanner:
    """Build a deterministic, read-only plan from explicit durable evidence."""

    def __init__(self, *, evidence_provider: ResumeEvidenceProvider) -> None:
        self.evidence_provider = evidence_provider

    def build(self, scopes: Iterable[ResumeScope]) -> ResumePlan:
        items: list[ResumePlanItem] = []
        seen_tokens: set[str] = set()

        for scope in scopes:
            if scope.scope_token in seen_tokens:
                raise ValueError(f"duplicate resume scope: {scope.scope_token}")
            seen_tokens.add(scope.scope_token)
            evidence = self.evidence_provider.inspect(scope)
            items.append(
                ResumePlanItem(
                    scope=scope,
                    status=self.classify(evidence),
                    evidence=evidence,
                )
            )

        return ResumePlan(items=tuple(items))

    @staticmethod
    def classify(evidence: ScopeResumeEvidence) -> ResumeStatus:
        # Operational access control wins over all executable states. Failure
        # evidence then wins over pending/complete so it cannot be hidden.
        if evidence.blocked_reason:
            return ResumeStatus.BLOCKED
        if evidence.failed_terminal:
            return ResumeStatus.FAILED_TERMINAL
        if evidence.failed_retryable:
            return ResumeStatus.FAILED_RETRYABLE
        if evidence.recovery_pending:
            return ResumeStatus.RECOVERY_PENDING
        if evidence.durable_complete:
            return ResumeStatus.DURABLE_COMPLETE
        return ResumeStatus.INCOMPLETE
