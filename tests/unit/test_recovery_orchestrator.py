from dataclasses import (
    replace,
)

from crawl_framework.storage.recovery import (
    RecoveryManifest,
    RecoveryResult,
    RecoveryStore,
)
from crawl_framework.storage.recovery_orchestrator import (
    RecoveryOrchestrator,
    StartupRecoveryPolicy,
)


# ============================================================
# Fake manager
# ============================================================


class FakeRecoveryManager:

    def __init__(
        self,
        store,
        results=None,
    ):

        self.store = store

        self.results = (
            results
            or {}
        )

        self.calls = []


    def recover_one(
        self,
        manifest,
    ):

        self.calls.append(
            manifest.manifest_id
        )

        result = self.results.get(
            manifest.manifest_id
        )

        if result is not None:

            return result

        return RecoveryResult(
            manifest_id=(
                manifest.manifest_id
            ),
            original_stage=(
                manifest.stage
            ),
            final_stage=(
                manifest.stage
            ),
            action="skipped",
            success=True,
            message="fake skipped",
        )


# ============================================================
# Helpers
# ============================================================


def make_manifest(
    manifest_id,
    *,
    stage="local",
    retryable=None,
    retry_reason=None,
):

    return RecoveryManifest(
        manifest_id=manifest_id,
        local_path=(
            f"/tmp/{manifest_id}.parquet"
        ),
        relative_path=(
            "site=demo/"
            "country=KR/"
            "dataset=forum_post/"
            "year=2026/"
            "month=08/"
            "day=24/"
            "bucket=00/"
            f"{manifest_id}.parquet"
        ),
        site_id="demo",
        country="KR",
        dataset="forum_post",
        sha256="a" * 64,
        row_count=1,
        file_size=100,
        stage=stage,
        retryable=retryable,
        retry_reason=retry_reason,
    )


# ============================================================
# Tests
# ============================================================


def test_empty_store_can_continue(
    tmp_path,
):

    store = RecoveryStore(
        tmp_path
        / "recovery"
    )

    manager = FakeRecoveryManager(
        store
    )

    orchestrator = (
        RecoveryOrchestrator(
            manager=manager
        )
    )

    result = orchestrator.run()

    assert (
        result.scanned
        == 0
    )

    assert (
        result.attempted
        == 0
    )

    assert (
        result.can_continue
        is True
    )


def test_pending_manifest_is_attempted(
    tmp_path,
):

    store = RecoveryStore(
        tmp_path
        / "recovery"
    )

    manifest = make_manifest(
        "pending-1"
    )

    store.save(
        manifest
    )

    manager = FakeRecoveryManager(
        store
    )

    orchestrator = (
        RecoveryOrchestrator(
            manager=manager
        )
    )

    result = orchestrator.run()

    assert (
        result.scanned
        == 1
    )

    assert (
        result.attempted
        == 1
    )

    assert manager.calls == [
        "pending-1"
    ]


def test_terminal_failure_not_attempted(
    tmp_path,
):

    store = RecoveryStore(
        tmp_path
        / "recovery"
    )

    manifest = make_manifest(
        "terminal-1",
        stage="failed",
        retryable=False,
        retry_reason=(
            "non_retryable_error"
        ),
    )

    store.save(
        manifest
    )

    manager = FakeRecoveryManager(
        store
    )

    orchestrator = (
        RecoveryOrchestrator(
            manager=manager
        )
    )

    result = orchestrator.run()

    assert (
        result.scanned
        == 1
    )

    assert (
        result.attempted
        == 0
    )

    assert (
        result.terminal_failed
        == 1
    )

    assert (
        result.skipped
        == 1
    )

    assert (
        manager.calls
        == []
    )

    # 默认 terminal failure
    # 不阻止整个 crawler。
    assert (
        result.can_continue
        is True
    )


def test_retryable_failed_is_attempted(
    tmp_path,
):

    store = RecoveryStore(
        tmp_path
        / "recovery"
    )

    manifest = make_manifest(
        "retryable-1",
        stage="failed",
        retryable=True,
        retry_reason=(
            "retryable_error"
        ),
    )

    store.save(
        manifest
    )

    manager = FakeRecoveryManager(
        store
    )

    orchestrator = (
        RecoveryOrchestrator(
            manager=manager
        )
    )

    result = orchestrator.run()

    assert (
        result.attempted
        == 1
    )

    assert manager.calls == [
        "retryable-1"
    ]


def test_successful_recovery_counted(
    tmp_path,
):

    store = RecoveryStore(
        tmp_path
        / "recovery"
    )

    manifest = make_manifest(
        "success-1"
    )

    store.save(
        manifest
    )

    manager = FakeRecoveryManager(
        store,
        results={
            "success-1": RecoveryResult(
                manifest_id="success-1",
                original_stage="local",
                final_stage="deleted",
                action="cleaned",
                success=True,
                message="done",
            )
        },
    )

    orchestrator = (
        RecoveryOrchestrator(
            manager=manager
        )
    )

    result = orchestrator.run()

    assert (
        result.recovered
        == 1
    )

    assert (
        result.failed
        == 0
    )


def test_failed_recovery_blocks_startup_by_default(
    tmp_path,
):

    store = RecoveryStore(
        tmp_path
        / "recovery"
    )

    manifest = make_manifest(
        "failure-1"
    )

    store.save(
        manifest
    )

    manager = FakeRecoveryManager(
        store,
        results={
            "failure-1": RecoveryResult(
                manifest_id="failure-1",
                original_stage="local",
                final_stage="failed",
                action="failed",
                success=False,
                message="connection refused",
            )
        },
    )

    orchestrator = (
        RecoveryOrchestrator(
            manager=manager
        )
    )

    result = orchestrator.run()

    assert (
        result.failed
        == 1
    )

    assert (
        result.can_continue
        is False
    )


def test_policy_can_allow_failed_recovery(
    tmp_path,
):

    store = RecoveryStore(
        tmp_path
        / "recovery"
    )

    manifest = make_manifest(
        "failure-allow"
    )

    store.save(
        manifest
    )

    manager = FakeRecoveryManager(
        store,
        results={
            "failure-allow": RecoveryResult(
                manifest_id=(
                    "failure-allow"
                ),
                original_stage="local",
                final_stage="failed",
                action="failed",
                success=False,
                message="temporary failure",
            )
        },
    )

    orchestrator = (
        RecoveryOrchestrator(
            manager=manager,
            policy=(
                StartupRecoveryPolicy(
                    block_on_failed=False
                )
            ),
        )
    )

    result = orchestrator.run()

    assert (
        result.failed
        == 1
    )

    assert (
        result.can_continue
        is True
    )


def test_policy_can_block_terminal_failure(
    tmp_path,
):

    store = RecoveryStore(
        tmp_path
        / "recovery"
    )

    manifest = make_manifest(
        "terminal-block",
        stage="failed",
        retryable=False,
        retry_reason=(
            "non_retryable_error"
        ),
    )

    store.save(
        manifest
    )

    manager = FakeRecoveryManager(
        store
    )

    orchestrator = (
        RecoveryOrchestrator(
            manager=manager,
            policy=(
                StartupRecoveryPolicy(
                    block_on_terminal_failed=True
                )
            ),
        )
    )

    result = orchestrator.run()

    assert (
        result.terminal_failed
        == 1
    )

    assert (
        result.can_continue
        is False
    )


def test_deleted_manifest_not_scanned(
    tmp_path,
):

    store = RecoveryStore(
        tmp_path
        / "recovery"
    )

    store.save(
        make_manifest(
            "deleted-1",
            stage="deleted",
        )
    )

    manager = FakeRecoveryManager(
        store
    )

    result = (
        RecoveryOrchestrator(
            manager=manager
        )
        .run()
    )

    assert (
        result.scanned
        == 0
    )

    assert (
        result.attempted
        == 0
    )


def test_result_contains_per_manifest_results(
    tmp_path,
):

    store = RecoveryStore(
        tmp_path
        / "recovery"
    )

    store.save(
        make_manifest(
            "a"
        )
    )

    store.save(
        make_manifest(
            "b"
        )
    )

    manager = FakeRecoveryManager(
        store
    )

    result = (
        RecoveryOrchestrator(
            manager=manager
        )
        .run()
    )

    assert (
        len(result.results)
        == 2
    )