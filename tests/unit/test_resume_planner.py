from __future__ import annotations

from dataclasses import dataclass

import pytest

from crawl_framework.core.plugin import CrawlCheckpoint, CrawlScope
from crawl_framework.core.resume import (
    CheckpointResumeEvidenceProvider,
    RecoveryNextAction,
    RecoveryStoreResumeInspector,
    ResumePlanner,
    ResumeScope,
    ResumeStatus,
    ScopeResumeEvidence,
    recovery_next_action,
)
from crawl_framework.storage.recovery import RecoveryManifest, RecoveryStore


def planned(source_key: str, *, dataset: str = "forum_post") -> ResumeScope:
    return ResumeScope(
        site_id="example",
        dataset=dataset,
        scope=CrawlScope(
            scope_type="instrument",
            scope_id=f"XTEST:{source_key}",
            source_key=source_key,
        ),
    )


@dataclass
class EvidenceProvider:
    evidence: dict[str, ScopeResumeEvidence]

    def inspect(self, scope: ResumeScope) -> ScopeResumeEvidence:
        return self.evidence.get(scope.scope_token, ScopeResumeEvidence())


def test_planner_classifies_all_states_and_counts() -> None:
    scopes = tuple(planned(str(index)) for index in range(6))
    evidence = {
        scopes[0].scope_token: ScopeResumeEvidence(durable_complete=True),
        scopes[1].scope_token: ScopeResumeEvidence(recovery_pending=True),
        scopes[2].scope_token: ScopeResumeEvidence(),
        scopes[3].scope_token: ScopeResumeEvidence(blocked_reason="remote WAF"),
        scopes[4].scope_token: ScopeResumeEvidence(failed_retryable=True),
        scopes[5].scope_token: ScopeResumeEvidence(failed_terminal=True),
    }

    plan = ResumePlanner(evidence_provider=EvidenceProvider(evidence)).build(scopes)

    assert [item.status for item in plan.items] == list(ResumeStatus)
    assert plan.total_scopes == 6
    assert plan.durable_complete_scopes == 1
    assert plan.recovery_pending_scopes == 1
    assert plan.incomplete_scopes == 1
    assert plan.blocked_scopes == 1
    assert plan.failed_retryable_scopes == 1
    assert plan.failed_terminal_scopes == 1
    assert plan.crawl_scopes == (scopes[2], scopes[4])


def test_planner_preserves_scope_order_and_serializes_stably() -> None:
    scopes = (planned("B"), planned("A"))
    plan = ResumePlanner(evidence_provider=EvidenceProvider({})).build(scopes)

    payload = plan.to_dict()
    assert [item["source_key"] for item in payload["items"]] == ["B", "A"]
    assert payload["total_scopes"] == 2
    assert payload["incomplete_scopes"] == 2
    assert payload["items"][0]["scope_token"] == (
        "example|forum_post|instrument|B"
    )


def test_planner_rejects_duplicate_scope_tokens() -> None:
    scope = planned("same")
    planner = ResumePlanner(evidence_provider=EvidenceProvider({}))

    with pytest.raises(ValueError, match="duplicate resume scope"):
        planner.build((scope, scope))


@pytest.mark.parametrize(
    ("evidence", "expected"),
    (
        (
            ScopeResumeEvidence(
                blocked_reason="blocked",
                failed_terminal=True,
                recovery_pending=True,
                durable_complete=True,
            ),
            ResumeStatus.BLOCKED,
        ),
        (
            ScopeResumeEvidence(
                failed_terminal=True,
                recovery_pending=True,
                durable_complete=True,
            ),
            ResumeStatus.FAILED_TERMINAL,
        ),
        (
            ScopeResumeEvidence(
                failed_retryable=True,
                recovery_pending=True,
                durable_complete=True,
            ),
            ResumeStatus.FAILED_RETRYABLE,
        ),
        (
            ScopeResumeEvidence(recovery_pending=True, durable_complete=True),
            ResumeStatus.RECOVERY_PENDING,
        ),
    ),
)
def test_classification_priority_is_deterministic(
    evidence: ScopeResumeEvidence,
    expected: ResumeStatus,
) -> None:
    assert ResumePlanner.classify(evidence) is expected


def test_conflicting_failure_evidence_is_rejected() -> None:
    with pytest.raises(ValueError, match="both retryable and terminal"):
        ScopeResumeEvidence(failed_retryable=True, failed_terminal=True)


class TrackingCheckpointStore:
    def __init__(self, checkpoints: dict) -> None:
        self.checkpoints = checkpoints
        self.loads = []
        self.exists_calls = []
        self.saves = []
        self.deletes = []

    def exists(self, key) -> bool:
        self.exists_calls.append(key)
        return key in self.checkpoints

    def load(self, key) -> CrawlCheckpoint:
        self.loads.append(key)
        return self.checkpoints.get(key, CrawlCheckpoint())

    def save(self, key, checkpoint) -> None:
        self.saves.append((key, checkpoint))

    def delete(self, key) -> bool:
        self.deletes.append(key)
        return False


def test_checkpoint_provider_is_read_only_and_uses_site_policy() -> None:
    complete = planned("complete")
    incomplete = planned("incomplete")
    store = TrackingCheckpointStore(
        {
            complete.checkpoint_key: CrawlCheckpoint(
                state={"scope_complete": True, "request_signature": "same"}
            )
        }
    )
    provider = CheckpointResumeEvidenceProvider(
        checkpoint_store=store,
        is_durable_complete=lambda scope, checkpoint: (
            checkpoint.state.get("scope_complete") is True
            and checkpoint.state.get("request_signature") == "same"
        ),
    )

    plan = ResumePlanner(evidence_provider=provider).build((complete, incomplete))

    assert [item.status for item in plan.items] == [
        ResumeStatus.DURABLE_COMPLETE,
        ResumeStatus.INCOMPLETE,
    ]
    assert store.loads == [complete.checkpoint_key, incomplete.checkpoint_key]
    assert store.saves == []
    assert store.deletes == []


def test_checkpoint_provider_combines_recovery_and_blocked_evidence() -> None:
    pending = planned("pending")
    blocked = planned("blocked")
    store = TrackingCheckpointStore({})
    provider = CheckpointResumeEvidenceProvider(
        checkpoint_store=store,
        is_durable_complete=lambda scope, checkpoint: False,
        inspect_recovery=lambda scope: (
            ScopeResumeEvidence(recovery_pending=True, detail="local parquet")
            if scope == pending
            else None
        ),
        inspect_blocked=lambda scope: "waf_human_verification"
        if scope == blocked
        else None,
    )

    plan = ResumePlanner(evidence_provider=provider).build((pending, blocked))

    assert [item.status for item in plan.items] == [
        ResumeStatus.RECOVERY_PENDING,
        ResumeStatus.BLOCKED,
    ]
    assert plan.items[0].evidence.detail == "local parquet"
    assert plan.items[1].evidence.blocked_reason == "waf_human_verification"


def manifest(
    manifest_id: str,
    scope_token: str,
    *,
    stage="local",
    retryable=None,
) -> RecoveryManifest:
    return RecoveryManifest(
        manifest_id=manifest_id,
        local_path=f"/tmp/{manifest_id}.parquet",
        relative_path=f"site=example/{manifest_id}.parquet",
        site_id="example",
        country="KR",
        dataset="forum_post",
        sha256="a" * 64,
        row_count=1,
        file_size=1,
        scope_tokens=(scope_token,),
        stage=stage,
        retryable=retryable,
    )


@pytest.mark.parametrize(
    ("stage", "expected"),
    (
        ("local", RecoveryNextAction.UPLOAD_AND_VERIFY),
        ("uploaded", RecoveryNextAction.VERIFY_REMOTE),
        ("verified", RecoveryNextAction.REGISTER_CATALOG),
        ("catalog_registered", RecoveryNextAction.COMMIT_SEEN),
        ("seen_committed", RecoveryNextAction.ADVANCE_CHECKPOINT_MARKER),
        ("checkpoint_committed", RecoveryNextAction.CLEAN_LOCAL),
        ("cleanable", RecoveryNextAction.CLEAN_LOCAL),
        ("deleted", RecoveryNextAction.NONE),
    ),
)
def test_recovery_stage_maps_to_minimum_next_action(stage, expected) -> None:
    assert recovery_next_action(
        manifest(f"manifest-{stage}", planned("A").scope_token, stage=stage)
    ) is expected


def test_recovery_inspector_uses_explicit_scope_membership_only(tmp_path) -> None:
    target = planned("target")
    unrelated = planned("unrelated")
    store = RecoveryStore(tmp_path / "recovery")
    store.save(manifest("target-uploaded", target.scope_token, stage="uploaded"))
    store.save(manifest("other-local", unrelated.scope_token, stage="local"))
    store.save(
        RecoveryManifest(
            manifest_id="legacy-unscoped",
            local_path="/tmp/legacy.parquet",
            relative_path="site=example/legacy.parquet",
            site_id="example",
            country="KR",
            dataset="forum_post",
            sha256="b" * 64,
            row_count=1,
            file_size=1,
        )
    )

    evidence = RecoveryStoreResumeInspector(store)(target)

    assert evidence is not None
    assert evidence.recovery_pending is True
    assert evidence.recovery_manifest_ids == ("target-uploaded",)
    assert evidence.recovery_actions == (RecoveryNextAction.VERIFY_REMOTE,)


def test_recovery_inspector_classifies_terminal_and_retryable_failures(
    tmp_path,
) -> None:
    retryable = planned("retryable")
    terminal = planned("terminal")
    store = RecoveryStore(tmp_path / "recovery")
    store.save(
        manifest(
            "retryable",
            retryable.scope_token,
            stage="failed",
            retryable=True,
        )
    )
    store.save(
        manifest(
            "terminal",
            terminal.scope_token,
            stage="failed",
            retryable=False,
        )
    )
    inspector = RecoveryStoreResumeInspector(store)

    assert ResumePlanner.classify(inspector(retryable)) is (
        ResumeStatus.FAILED_RETRYABLE
    )
    assert ResumePlanner.classify(inspector(terminal)) is (
        ResumeStatus.FAILED_TERMINAL
    )
