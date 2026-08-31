from __future__ import annotations

from dataclasses import replace
from datetime import (
    datetime,
    timezone,
)

import pytest

from crawl_framework.core.models import (
    CanonicalRecord,
)
from crawl_framework.storage.buffer import (
    FlushBatch,
)
from crawl_framework.storage.cleaner import (
    Cleaner,
    CleanupResult,
)
from crawl_framework.storage.parquet_writer import (
    ParquetWriter,
)
from crawl_framework.storage.partition import (
    Partitioner,
)
from crawl_framework.storage.postgres import (
    PostgresCatalog,
)
from crawl_framework.storage.record_index import (
    RecordIndexStore,
)
from crawl_framework.storage.recovery import (
    RecoveryManager,
    RecoveryManifest,
    RecoveryStage,
    RecoveryStore,
)
from crawl_framework.storage.seen_store import (
    SQLiteSeenStore,
)
from crawl_framework.storage.uploader import (
    LocalUploader,
)

UTC = timezone.utc


class FakeCursor:

    def __init__(
        self,
        log,
    ):

        self.log = log

    def execute(
        self,
        sql,
        params=None,
    ):

        self.log.append(
            (
                sql,
                params,
            )
        )

    def __enter__(
        self,
    ):

        return self

    def __exit__(
        self,
        exc_type,
        exc,
        tb,
    ):

        pass


class FakeConnection:

    def __init__(
        self,
    ):

        self.executed = []
        self.commit_count = 0

    def cursor(
        self,
    ):

        return FakeCursor(
            self.executed
        )

    def commit(
        self,
    ):

        self.commit_count += 1


def manifest_from_info(
    info,
    *,
    stage="local",
):

    return RecoveryManifest(
        manifest_id=(
            info.relative_path
            .as_posix()
        ),
        local_path=(
            info.file_path
            .as_posix()
        ),
        relative_path=(
            info.relative_path
            .as_posix()
        ),
        site_id=(
            info.partition.site_id
        ),
        country=(
            info.partition.country
        ),
        dataset=(
            info.partition.dataset
        ),
        sha256=(
            info.sha256
        ),
        row_count=(
            info.row_count
        ),
        file_size=(
            info.file_size
        ),
        stage=stage,
    )


class FailAfterStage:

    def __init__(
        self,
        stage: RecoveryStage,
    ):

        self.stage = stage
        self.triggered = False

    def after_stage(
        self,
        manifest,
        stage,
    ) -> None:

        if (
            stage == self.stage
            and
            not self.triggered
        ):

            self.triggered = True

            raise RuntimeError(
                f"temporary failure after {stage}"
            )


def make_record(
    source_id="fault-1",
):

    return CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        source_id=source_id,
        scope_type="instrument",
        scope_id="XKRX:005930",
        instrument_id="XKRX:005930",
        event_time=datetime(
            2026,
            8,
            24,
            1,
            0,
            tzinfo=UTC,
        ),
        title="fault recovery",
    )


def make_info_and_manifest(
    tmp_path,
    *,
    stage="local",
    with_sidecar=True,
):

    record = make_record()
    partitioner = Partitioner()
    batch = FlushBatch(
        key=partitioner.partition_for(
            record
        ),
        records=(
            record,
        ),
        estimated_bytes=1000,
    )

    writer = ParquetWriter(
        tmp_path
        /
        "warehouse"
    )

    info = writer.write_batch(
        batch
    )

    manifest = manifest_from_info(
        info,
        stage=stage,
    )

    if with_sidecar:

        index_info = (
            RecordIndexStore()
            .write_for_parquet(
                info.file_path,
                batch.records,
            )
        )

        manifest = replace(
            manifest,
            record_index_path=(
                index_info
                .file_path
                .as_posix()
            ),
        )

    return (
        info,
        manifest,
        record,
    )


def make_manager(
    tmp_path,
    *,
    fault_injector=None,
):

    store = RecoveryStore(
        tmp_path
        /
        "state"
        /
        "recovery"
    )

    uploader = LocalUploader(
        tmp_path
        /
        "remote",
        verify_size=True,
        verify_sha256=True,
    )

    connection = FakeConnection()

    seen_store = SQLiteSeenStore(
        tmp_path
        /
        "state"
        /
        "seen.sqlite3"
    )

    manager = RecoveryManager(
        store=store,
        uploader=uploader,
        catalog=PostgresCatalog(
            connection
        ),
        cleaner=Cleaner(),
        seen_store=seen_store,
        fault_injector=fault_injector,
    )

    return (
        manager,
        store,
        connection,
        seen_store,
    )


@pytest.mark.parametrize(
    "stage",
    [
        "uploaded",
        "verified",
        "catalog_registered",
        "seen_committed",
        "checkpoint_committed",
        "cleanable",
    ],
)
def test_recovery_fault_injection_records_resume_stage(
    tmp_path,
    stage,
):

    fault = FailAfterStage(
        stage
    )

    manager, store, connection, seen_store = make_manager(
        tmp_path,
        fault_injector=fault,
    )

    info, manifest, record = make_info_and_manifest(
        tmp_path,
        stage="local",
    )

    store.save(
        manifest
    )

    result = manager.recover_one(
        manifest
    )

    assert result.success is False
    assert result.final_stage == "failed"

    failed = store.load(
        manifest.manifest_id
    )

    assert failed is not None
    assert failed.stage == "failed"
    assert failed.failed_from_stage == stage
    assert failed.retryable is True

    retry_manager, retry_store, retry_connection, retry_seen = make_manager(
        tmp_path
    )

    retry_result = retry_manager.recover_one(
        failed
    )

    assert retry_result.success is True
    assert retry_result.final_stage == "deleted"

    final = retry_store.load(
        manifest.manifest_id
    )

    assert final is not None
    assert final.stage == "deleted"

    assert not info.file_path.exists()

    seen = retry_seen.inspect(
        record.record_uid,
        record.version_hash,
    )

    assert seen.decision == "unchanged"


class CrashAfterDeletingParquetCleaner:

    def clean(
        self,
        info,
        context,
    ):

        info.file_path.unlink()

        return CleanupResult(
            file_path=info.file_path,
            decision="delete",
            deleted=False,
            reason="crash after parquet deletion",
            record_index_deleted=False,
        )


def test_recovery_detects_partial_cleanup_crash_boundary(
    tmp_path,
):

    info, manifest, record = make_info_and_manifest(
        tmp_path,
        stage="cleanable",
        with_sidecar=False,
    )

    store = RecoveryStore(
        tmp_path
        /
        "state"
        /
        "recovery"
    )

    store.save(
        manifest
    )

    manager = RecoveryManager(
        store=store,
        uploader=LocalUploader(
            tmp_path
            /
            "remote"
        ),
        catalog=PostgresCatalog(
            FakeConnection()
        ),
        cleaner=CrashAfterDeletingParquetCleaner(),
    )

    result = manager.recover_one(
        manifest
    )

    assert result.success is True
    assert result.final_stage == "deleted"

    loaded = store.load(
        manifest.manifest_id
    )

    assert loaded is not None
    assert loaded.stage == "deleted"
