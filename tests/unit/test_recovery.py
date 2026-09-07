import json

from datetime import (
    date,
    datetime,
    timezone,
)

import pytest

from crawl_framework.core.models import (
    CanonicalRecord,
)
from crawl_framework.storage.retry_policy import (
    RetryPolicy,
)
from crawl_framework.storage.buffer import (
    FlushBatch,
)
from crawl_framework.storage.cleaner import (
    Cleaner,
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
from crawl_framework.storage.recovery import (
    RecoveryManager,
    RecoveryManifest,
    RecoveryStore,
    manifest_to_parquet_info,
    safe_manifest_name,
)
from crawl_framework.storage.uploader import (
    LocalUploader,
)
from crawl_framework.storage.record_index import (
    RecordIndexStore,
)
from crawl_framework.storage.seen_store import (
    SQLiteSeenStore,
)


# ============================================================
# Fake PostgreSQL
# ============================================================


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


# ============================================================
# Helpers
# ============================================================


def make_manifest():

    return RecoveryManifest(
        manifest_id=(
            "site=naver_finance/"
            "country=KR/"
            "dataset=forum_post/"
            "year=2026/"
            "month=08/"
            "day=24/"
            "bucket=3f/"
            "part-test.parquet"
        ),
        local_path=(
            "/tmp/warehouse/"
            "part-test.parquet"
        ),
        relative_path=(
            "site=naver_finance/"
            "country=KR/"
            "dataset=forum_post/"
            "year=2026/"
            "month=08/"
            "day=24/"
            "bucket=3f/"
            "part-test.parquet"
        ),
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        sha256="a" * 64,
        row_count=100,
        file_size=12345,
    )


def make_real_parquet(
    tmp_path,
):

    record = CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        source_id="123",
        scope_type="instrument",
        scope_id="XKRX:005930",
        instrument_id="XKRX:005930",
        event_time=datetime(
            2026,
            8,
            24,
            1,
            0,
            tzinfo=timezone.utc,
        ),
        title="hello",
    )

    partitioner = Partitioner()

    batch = FlushBatch(
        key=(
            partitioner
            .partition_for(
                record
            )
        ),
        records=(
            record,
        ),
        estimated_bytes=1000,
    )

    writer = ParquetWriter(
        tmp_path
        / "warehouse"
    )

    return writer.write_batch(
        batch
    )


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

def attach_record_index(
    info,
    manifest,
    records,
):

    store = RecordIndexStore()

    index_info = (
        store.write_for_parquet(
            info.file_path,
            records,
        )
    )

    from dataclasses import replace

    return replace(
        manifest,
        record_index_path=(
            index_info.file_path
            .as_posix()
        ),
    )

def make_manager(
    tmp_path,
):

    store = RecoveryStore(
        tmp_path
        / "state"
        / "recovery"
    )

    uploader = LocalUploader(
        tmp_path
        / "remote",
        verify_size=True,
        verify_sha256=True,
    )

    connection = FakeConnection()

    catalog = PostgresCatalog(
        connection
    )

    cleaner = Cleaner()

    seen_store = SQLiteSeenStore(
        tmp_path
        / "state"
        / "seen.sqlite3"
    )

    manager = RecoveryManager(
        store=store,
        uploader=uploader,
        catalog=catalog,
        cleaner=cleaner,
        seen_store=seen_store,
    )

    return (
        manager,
        store,
        connection,
        seen_store,
    )

# ============================================================
# Existing store tests
# ============================================================


def test_manifest_defaults():

    manifest = make_manifest()

    assert (
        manifest.stage
        == "local"
    )

    assert manifest.created_at

    assert manifest.updated_at


def test_safe_manifest_name():

    name = safe_manifest_name(
        "../abc/test.parquet"
    )

    assert (
        len(name)
        == 69
    )

    assert name.endswith(
        ".json"
    )

    assert "/" not in name


def test_save_and_load(
    tmp_path,
):

    store = RecoveryStore(
        tmp_path
        / "recovery"
    )

    manifest = make_manifest()

    store.save(
        manifest
    )

    loaded = store.load(
        manifest.manifest_id
    )

    assert loaded is not None

    assert (
        loaded.manifest_id
        == manifest.manifest_id
    )

    assert (
        loaded.stage
        == "local"
    )


def test_advance_stage(
    tmp_path,
):

    store = RecoveryStore(
        tmp_path
        / "recovery"
    )

    manifest = make_manifest()

    store.save(
        manifest
    )

    updated = store.advance(
        manifest.manifest_id,
        "uploaded",
        remote_path=(
            "/remote/part.parquet"
        ),
    )

    assert (
        updated.stage
        == "uploaded"
    )

    assert (
        updated.remote_path
        == "/remote/part.parquet"
    )


def test_cannot_move_backward():

    manifest = make_manifest()

    manifest = manifest.advance(
        "verified"
    )

    with pytest.raises(
        ValueError
    ):

        manifest.advance(
            "uploaded"
        )


def test_mark_failed(
    tmp_path,
):

    store = RecoveryStore(
        tmp_path
        / "recovery"
    )

    manifest = make_manifest()

    store.save(
        manifest
    )

    failed = store.mark_failed(
        manifest.manifest_id,
        "rsync failed",
    )

    assert (
        failed.stage
        == "failed"
    )

    assert (
        failed.last_error
        == "rsync failed"
    )


def test_reclassify_failed_manifest_is_non_destructive(
    tmp_path,
):

    store = RecoveryStore(
        tmp_path
        / "recovery"
    )

    manifest = make_manifest()

    store.save(
        manifest
    )

    failed = store.mark_failed(
        manifest.manifest_id,
        "remote stat failed: setlocale warning",
        retryable=False,
        retry_reason="non_retryable_error",
    )

    reclassified = store.reclassify_failed(
        manifest.manifest_id,
        retryable=True,
        retry_reason="retryable_error",
    )

    assert reclassified.stage == "failed"
    assert (
        reclassified.failed_from_stage
        == failed.failed_from_stage
    )
    assert (
        reclassified.retry_count
        == failed.retry_count
    )
    assert (
        reclassified.retryable
        is True
    )
    assert (
        reclassified.retry_reason
        == "retryable_error"
    )


def test_list_pending(
    tmp_path,
):

    store = RecoveryStore(
        tmp_path
        / "recovery"
    )

    first = make_manifest()

    second = RecoveryManifest(
        manifest_id="second",
        local_path="/tmp/a.parquet",
        relative_path="a.parquet",
        site_id="demo",
        country="KR",
        dataset="forum_post",
        sha256="b" * 64,
        row_count=1,
        file_size=100,
        stage="deleted",
    )

    store.save(
        first
    )

    store.save(
        second
    )

    pending = store.list_pending()

    assert (
        len(pending)
        == 1
    )

    assert (
        pending[0].manifest_id
        == first.manifest_id
    )


def test_delete_manifest(
    tmp_path,
):

    store = RecoveryStore(
        tmp_path
        / "recovery"
    )

    manifest = make_manifest()

    store.save(
        manifest
    )

    assert (
        store.delete(
            manifest.manifest_id
        )
        is True
    )

    assert (
        store.load(
            manifest.manifest_id
        )
        is None
    )


# ============================================================
# Reconstruction
# ============================================================


def test_manifest_to_parquet_info(
    tmp_path,
):

    info = make_real_parquet(
        tmp_path
    )

    manifest = manifest_from_info(
        info
    )

    rebuilt = (
        manifest_to_parquet_info(
            manifest
        )
    )

    assert (
        rebuilt.file_path
        == info.file_path
    )

    assert (
        rebuilt.relative_path
        == info.relative_path
    )

    assert (
        rebuilt.row_count
        == info.row_count
    )

    assert (
        rebuilt.sha256
        == info.sha256
    )

    assert (
        rebuilt.partition.site_id
        == "naver_finance"
    )

    assert isinstance(
        rebuilt.partition.partition_date,
        date,
    )

    assert (
        rebuilt.partition.partition_date
        == date(
            2026,
            8,
            24,
        )
    )
    


# ============================================================
# RecoveryManager
# ============================================================


def test_recover_local_to_catalog(
    tmp_path,
):

    info = make_real_parquet(
        tmp_path
    )

    (
        manager,
        store,
        connection,
        seen_store,
    ) = make_manager(
        tmp_path
    )

    manifest = manifest_from_info(
        info,
        stage="local",
    )

    store.save(
        manifest
    )

    result = manager.recover_one(
        manifest
    )

    # ========================================================
    # Recovery succeeded
    # ========================================================

    assert (
        result.success
        is True
    )

    assert (
        result.final_stage
        == "catalog_registered"
    )

    # ========================================================
    # Manifest advanced
    # ========================================================

    loaded = store.load(
        manifest.manifest_id
    )

    assert loaded is not None

    assert (
        loaded.stage
        == "catalog_registered"
    )

    # ========================================================
    # PostgreSQL:
    #
    # 1. register_parquet_file()
    # 2. mark_uploaded()
    # ========================================================

    assert (
        len(
            connection.executed
        )
        == 2
    )

    first_sql, first_params = (
        connection.executed[0]
    )

    second_sql, second_params = (
        connection.executed[1]
    )

    # ========================================================
    # register_parquet_file()
    # ========================================================

    assert (
        "INSERT INTO marketdata.data_files"
        in first_sql
    )

    assert (
        first_params[
            "file_path"
        ]
        ==
        info.relative_path.as_posix()
    )

    # ========================================================
    # mark_uploaded()
    # ========================================================

    assert (
        "UPDATE marketdata.data_files"
        in second_sql
    )

    assert (
        "storage_status = 'uploaded'"
        in second_sql
    )

    assert (
        second_params[
            "file_path"
        ]
        ==
        info.relative_path.as_posix()
    )

    assert (
        second_params[
            "remote_path"
        ]
    )

    

def test_recover_verified_to_catalog(
    tmp_path,
):

    info = make_real_parquet(
        tmp_path
    )

    manager, store, connection, seen_store = (
        make_manager(
            tmp_path
        )
    )

    manifest = manifest_from_info(
        info,
        stage="verified",
    )

    store.save(
        manifest
    )

    result = manager.recover_one(
        manifest
    )

    assert result.success

    assert (
        result.final_stage
        == "catalog_registered"
    )

    assert (
        len(connection.executed)
        == 1
    )


def test_catalog_registered_is_kept(
    tmp_path,
):

    info = make_real_parquet(
        tmp_path
    )

    manager, store, connection, seen_store = (
        make_manager(
            tmp_path
        )
    )

    manifest = manifest_from_info(
        info,
        stage="catalog_registered",
    )

    store.save(
        manifest
    )

    result = manager.recover_one(
        manifest
    )

    assert result.success

    assert (
        result.action
        == "skipped"
    )

    assert (
        result.final_stage
        == "catalog_registered"
    )

    # 本地文件不能删。
    assert (
        info.file_path.exists()
    )


def test_seen_committed_becomes_deleted(
    tmp_path,
):

    info = make_real_parquet(
        tmp_path
    )

    manager, store, connection, seen_store = (
        make_manager(
            tmp_path
        )
    )

    manifest = manifest_from_info(
        info,
        stage="seen_committed",
    )

    store.save(
        manifest
    )

    result = manager.recover_one(
        manifest
    )

    assert result.success

    assert (
        result.final_stage
        == "deleted"
    )

    assert not (
        info.file_path.exists()
    )


def test_cleanable_deletes_local_file(
    tmp_path,
):

    info = make_real_parquet(
        tmp_path
    )

    manager, store, connection, seen_store = (
        make_manager(
            tmp_path
        )
    )

    manifest = manifest_from_info(
        info,
        stage="cleanable",
    )

    store.save(
        manifest
    )

    result = manager.recover_one(
        manifest
    )

    assert result.success

    assert (
        result.action
        == "cleaned"
    )

    assert (
        result.final_stage
        == "deleted"
    )

    assert not (
        info.file_path.exists()
    )


def test_missing_local_file_marks_failed(
    tmp_path,
):

    manager, store, connection, seen_store = (
        make_manager(
            tmp_path
        )
    )

    manifest = RecoveryManifest(
        manifest_id=(
            "site=demo/"
            "country=KR/"
            "dataset=forum_post/"
            "year=2026/"
            "month=08/"
            "day=24/"
            "bucket=00/"
            "part-missing.parquet"
        ),
        local_path=(
            str(
                tmp_path
                / "missing.parquet"
            )
        ),
        relative_path=(
            "site=demo/"
            "country=KR/"
            "dataset=forum_post/"
            "year=2026/"
            "month=08/"
            "day=24/"
            "bucket=00/"
            "part-missing.parquet"
        ),
        site_id="demo",
        country="KR",
        dataset="forum_post",
        sha256="a" * 64,
        row_count=1,
        file_size=1,
        stage="local",
    )

    store.save(
        manifest
    )

    result = manager.recover_one(
        manifest
    )

    assert (
        result.success
        is False
    )

    assert (
        result.final_stage
        == "failed"
    )

    loaded = store.load(
        manifest.manifest_id
    )

    assert loaded is not None

    assert (
        loaded.stage
        == "failed"
    )


def test_recover_pending(
    tmp_path,
):

    info = make_real_parquet(
        tmp_path
    )

    manager, store, connection, seen_store = (
        make_manager(
            tmp_path
        )
    )

    pending = manifest_from_info(
        info,
        stage="verified",
    )

    deleted = RecoveryManifest(
        manifest_id="already-deleted",
        local_path="/tmp/none",
        relative_path="none",
        site_id="demo",
        country="KR",
        dataset="forum_post",
        sha256="a" * 64,
        row_count=0,
        file_size=0,
        stage="deleted",
    )

    store.save(
        pending
    )

    store.save(
        deleted
    )

    results = (
        manager.recover_pending()
    )

    assert (
        len(results)
        == 1
    )

    assert (
        manager.stats.scanned
        == 1
    )

    assert (
        manager.stats.recovered
        == 1
    )

def test_catalog_registered_recovers_seen_store(
    tmp_path,
):

    record = CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        source_id="recover-seen-1",
        scope_type="instrument",
        scope_id="XKRX:005930",
        instrument_id="XKRX:005930",
        event_time=datetime(
            2026,
            8,
            24,
            1,
            0,
            tzinfo=timezone.utc,
        ),
        title="recover seen",
    )

    partitioner = Partitioner()

    batch = FlushBatch(
        key=(
            partitioner
            .partition_for(
                record
            )
        ),
        records=(
            record,
        ),
        estimated_bytes=1000,
    )

    writer = ParquetWriter(
        tmp_path
        / "warehouse"
    )

    info = writer.write_batch(
        batch
    )

    manager, store, connection, seen_store = (
        make_manager(
            tmp_path
        )
    )

    manifest = manifest_from_info(
        info,
        stage="catalog_registered",
    )

    record_index_store = (
        RecordIndexStore()
    )

    index_info = (
        record_index_store
        .write_for_parquet(
            info.file_path,
            batch.records,
        )
    )

    from dataclasses import replace

    manifest = replace(
        manifest,
        record_index_path=(
            index_info.file_path
            .as_posix()
        ),
    )

    store.save(
        manifest
    )

    assert (
        seen_store.count()
        == 0
    )

    result = manager.recover_one(
        manifest
    )

    assert (
        result.success
        is True
    )

    assert (
        result.final_stage
        == "deleted"
    )

    assert (
        seen_store.count()
        == 1
    )

    seen = seen_store.inspect(
        record.record_uid,
        record.version_hash,
    )

    assert (
        seen.decision
        == "unchanged"
    )

def test_catalog_registered_missing_sidecar_fails(
    tmp_path,
):

    info = make_real_parquet(
        tmp_path
    )

    manager, store, connection, seen_store = (
        make_manager(
            tmp_path
        )
    )

    manifest = manifest_from_info(
        info,
        stage="catalog_registered",
    )

    from dataclasses import replace

    manifest = replace(
        manifest,
        record_index_path=(
            str(
                tmp_path
                / "missing.records.jsonl"
            )
        ),
    )

    store.save(
        manifest
    )

    result = manager.recover_one(
        manifest
    )

    assert (
        result.success
        is False
    )

    assert (
        result.final_stage
        == "failed"
    )

def test_record_index_row_count_mismatch_fails(
    tmp_path,
):

    info = make_real_parquet(
        tmp_path
    )

    manager, store, connection, seen_store = (
        make_manager(
            tmp_path
        )
    )

    manifest = manifest_from_info(
        info,
        stage="catalog_registered",
    )

    record_index_store = (
        RecordIndexStore()
    )

    extra_record = CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        source_id="extra",
        scope_type="instrument",
        scope_id="XKRX:005930",
        instrument_id="XKRX:005930",
        event_time=datetime(
            2026,
            8,
            24,
            2,
            0,
            tzinfo=timezone.utc,
        ),
        title="extra",
    )

    index_info = (
        record_index_store
        .write_for_parquet(
            info.file_path,
            [
                extra_record,
                CanonicalRecord(
                    site_id="naver_finance",
                    country="KR",
                    dataset="forum_post",
                    source_id="extra-2",
                    scope_type="instrument",
                    scope_id="XKRX:005930",
                    instrument_id="XKRX:005930",
                    event_time=datetime(
                        2026,
                        8,
                        24,
                        3,
                        0,
                        tzinfo=timezone.utc,
                    ),
                    title="extra-2",
                ),
            ],
        )
    )

    from dataclasses import replace

    manifest = replace(
        manifest,
        record_index_path=(
            index_info.file_path
            .as_posix()
        ),
    )

    store.save(
        manifest
    )

    result = manager.recover_one(
        manifest
    )

    assert (
        result.success
        is False
    )

    assert (
        result.final_stage
        == "failed"
    )

def test_recovery_catalog_registered_deletes_parquet_and_sidecar(
    tmp_path,
):

    record = CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        source_id="recover-cleanup-1",
        scope_type="instrument",
        scope_id="XKRX:005930",
        instrument_id="XKRX:005930",
        event_time=datetime(
            2026,
            8,
            24,
            1,
            0,
            tzinfo=timezone.utc,
        ),
        title="recover cleanup",
    )

    partitioner = Partitioner()

    batch = FlushBatch(
        key=(
            partitioner
            .partition_for(
                record
            )
        ),
        records=(
            record,
        ),
        estimated_bytes=1000,
    )

    writer = ParquetWriter(
        tmp_path
        / "warehouse"
    )

    info = writer.write_batch(
        batch
    )

    record_index_store = (
        RecordIndexStore()
    )

    index_info = (
        record_index_store
        .write_for_parquet(
            info.file_path,
            batch.records,
        )
    )

    assert (
        info.file_path.exists()
        is True
    )

    assert (
        index_info.file_path.exists()
        is True
    )

    manager, store, connection, seen_store = (
        make_manager(
            tmp_path
        )
    )

    manifest = manifest_from_info(
        info,
        stage="catalog_registered",
    )

    from dataclasses import replace

    manifest = replace(
        manifest,
        record_index_path=(
            index_info
            .file_path
            .as_posix()
        ),
    )

    store.save(
        manifest
    )

    assert (
        seen_store.count()
        == 0
    )

    # ========================================================
    # 模拟程序重启后的恢复
    # ========================================================

    result = manager.recover_one(
        manifest
    )

    # ========================================================
    # Recovery 应完整闭环
    # ========================================================

    assert (
        result.success
        is True
    )

    assert (
        result.final_stage
        == "deleted"
    )

    # ========================================================
    # SeenStore 已通过 sidecar 恢复
    # ========================================================

    assert (
        seen_store.count()
        == 1
    )

    seen = seen_store.inspect(
        record.record_uid,
        record.version_hash,
    )

    assert (
        seen.decision
        == "unchanged"
    )

    # ========================================================
    # Parquet 已删除
    # ========================================================

    assert (
        info.file_path.exists()
        is False
    )

    # ========================================================
    # Sidecar 也必须删除
    # ========================================================

    assert (
        index_info.file_path.exists()
        is False
    )

    # ========================================================
    # Manifest 最终必须记录 deleted
    # ========================================================

    loaded = store.load(
        manifest.manifest_id
    )

    assert (
        loaded is not None
    )

    assert (
        loaded.stage
        == "deleted"
    )

def test_recovery_cleanable_deletes_sidecar(
    tmp_path,
):

    record = CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        source_id="recover-cleanable-1",
        scope_type="instrument",
        scope_id="XKRX:005930",
        instrument_id="XKRX:005930",
        event_time=datetime(
            2026,
            8,
            24,
            1,
            0,
            tzinfo=timezone.utc,
        ),
        title="cleanable recovery",
    )

    partitioner = Partitioner()

    batch = FlushBatch(
        key=(
            partitioner
            .partition_for(
                record
            )
        ),
        records=(
            record,
        ),
        estimated_bytes=1000,
    )

    writer = ParquetWriter(
        tmp_path
        / "warehouse"
    )

    info = writer.write_batch(
        batch
    )

    record_index_store = (
        RecordIndexStore()
    )

    index_info = (
        record_index_store
        .write_for_parquet(
            info.file_path,
            batch.records,
        )
    )

    manager, store, connection, seen_store = (
        make_manager(
            tmp_path
        )
    )

    manifest = manifest_from_info(
        info,
        stage="cleanable",
    )

    from dataclasses import replace

    manifest = replace(
        manifest,
        record_index_path=(
            index_info
            .file_path
            .as_posix()
        ),
    )

    store.save(
        manifest
    )

    assert (
        info.file_path.exists()
    )

    assert (
        index_info.file_path.exists()
    )

    result = manager.recover_one(
        manifest
    )

    assert (
        result.success
        is True
    )

    assert (
        result.action
        == "cleaned"
    )

    assert (
        result.final_stage
        == "deleted"
    )

    assert (
        info.file_path.exists()
        is False
    )

    assert (
        index_info.file_path.exists()
        is False
    )

    loaded = store.load(
        manifest.manifest_id
    )

    assert (
        loaded is not None
    )

    assert (
        loaded.stage
        == "deleted"
    )

def test_corrupt_sidecar_does_not_delete_parquet(
    tmp_path,
):

    info = make_real_parquet(
        tmp_path
    )

    manager, store, connection, seen_store = (
        make_manager(
            tmp_path
        )
    )

    manifest = manifest_from_info(
        info,
        stage="catalog_registered",
    )

    from crawl_framework.storage.record_index import (
        default_index_path,
    )

    index_path = default_index_path(
        info.file_path
    )

    index_path.write_text(
        "this is not valid json\n",
        encoding="utf-8",
    )

    from dataclasses import replace

    manifest = replace(
        manifest,
        record_index_path=(
            index_path
            .as_posix()
        ),
    )

    store.save(
        manifest
    )

    result = manager.recover_one(
        manifest
    )

    assert (
        result.success
        is False
    )

    assert (
        result.final_stage
        == "failed"
    )

    # SeenStore 绝对不能被错误提交
    assert (
        seen_store.count()
        == 0
    )

    # 最关键：
    # recovery 失败时 parquet 不能删除。
    assert (
        info.file_path.exists()
        is True
    )

    # sidecar 同样应该保留，
    # 方便后续人工检查或修复。
    assert (
        index_path.exists()
        is True
    )

    loaded = store.load(
        manifest.manifest_id
    )

    assert (
        loaded is not None
    )

    assert (
        loaded.stage
        == "failed"
    )

def test_mark_failed_records_failed_from_stage(
    tmp_path,
):

    store = RecoveryStore(
        tmp_path
        / "recovery"
    )

    manifest = make_manifest()

    manifest = manifest.advance(
        "uploaded"
    )

    store.save(
        manifest
    )

    failed = store.mark_failed(
        manifest.manifest_id,
        "network error",
    )

    assert (
        failed.stage
        == "failed"
    )

    assert (
        failed.failed_from_stage
        == "uploaded"
    )

    assert (
        failed.retry_count
        == 1
    )

    assert (
        failed.last_failed_at
        is not None
    )

def test_retry_count_increments(
    tmp_path,
):

    store = RecoveryStore(
        tmp_path
        / "recovery"
    )

    manifest = make_manifest()

    store.save(
        manifest
    )

    first = store.mark_failed(
        manifest.manifest_id,
        "first error",
    )

    assert (
        first.retry_count
        == 1
    )

    second = store.mark_failed(
        manifest.manifest_id,
        "second error",
    )

    assert (
        second.retry_count
        == 2
    )

    assert (
        second.failed_from_stage
        == "local"
    )

def test_retry_count_cannot_be_negative():

    with pytest.raises(
        ValueError
    ):

        RecoveryManifest(
            manifest_id="test",
            local_path="/tmp/a.parquet",
            relative_path="a.parquet",
            site_id="demo",
            country="KR",
            dataset="forum_post",
            sha256="a" * 64,
            row_count=1,
            file_size=100,
            retry_count=-1,
        )

def test_failed_from_stage_cannot_be_failed():

    with pytest.raises(
        ValueError
    ):

        RecoveryManifest(
            manifest_id="test",
            local_path="/tmp/a.parquet",
            relative_path="a.parquet",
            site_id="demo",
            country="KR",
            dataset="forum_post",
            sha256="a" * 64,
            row_count=1,
            file_size=100,
            stage="failed",
            failed_from_stage="failed",
        )

def test_failed_catalog_registered_resumes_without_upload(
    tmp_path,
):

    record = CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        source_id="resume-catalog-1",
        scope_type="instrument",
        scope_id="XKRX:005930",
        instrument_id="XKRX:005930",
        event_time=datetime(
            2026,
            8,
            24,
            1,
            0,
            tzinfo=timezone.utc,
        ),
        title="resume catalog",
    )

    partitioner = Partitioner()

    batch = FlushBatch(
        key=(
            partitioner
            .partition_for(
                record
            )
        ),
        records=(
            record,
        ),
        estimated_bytes=1000,
    )

    writer = ParquetWriter(
        tmp_path
        / "warehouse"
    )

    info = writer.write_batch(
        batch
    )

    record_index_store = (
        RecordIndexStore()
    )

    index_info = (
        record_index_store
        .write_for_parquet(
            info.file_path,
            batch.records,
        )
    )

    manager, store, connection, seen_store = (
        make_manager(
            tmp_path
        )
    )

    manifest = manifest_from_info(
        info,
        stage="catalog_registered",
    )

    from dataclasses import replace

    manifest = replace(
        manifest,
        record_index_path=(
            index_info.file_path
            .as_posix()
        ),
    )

    store.save(
        manifest
    )

    failed = store.mark_failed(
        manifest.manifest_id,
        "temporary seen store failure",
        failed_from_stage=(
            "catalog_registered"
        ),
    )

    assert (
        failed.stage
        == "failed"
    )

    assert (
        failed.failed_from_stage
        == "catalog_registered"
    )

    assert (
        failed.retry_count
        == 1
    )

    result = manager.recover_one(
        failed
    )

    assert (
        result.success
        is True
    )

    assert (
        result.final_stage
        == "deleted"
    )

    assert (
        seen_store.count()
        == 1
    )

    # 如果错误地从 local 开始恢复，
    # 会走 uploader。
    #
    # 最终这里至少验证 durable recovery
    # 能从 catalog_registered 顺利闭环。

    loaded = store.load(
        manifest.manifest_id
    )

    assert (
        loaded is not None
    )

    assert (
        loaded.stage
        == "deleted"
    )

    assert (
        loaded.retry_count
        == 1
    )

    assert (
        loaded.failed_from_stage
        is None
    )

def test_recovery_network_failure_is_retryable(
    tmp_path,
):

    manager, store, connection, seen_store = (
        make_manager(
            tmp_path
        )
    )

    manifest = RecoveryManifest(
        manifest_id=(
            "site=demo/"
            "country=KR/"
            "dataset=forum_post/"
            "year=2026/"
            "month=08/"
            "day=24/"
            "bucket=00/"
            "part-network.parquet"
        ),
        local_path=(
            str(
                tmp_path
                / "missing.parquet"
            )
        ),
        relative_path=(
            "site=demo/"
            "country=KR/"
            "dataset=forum_post/"
            "year=2026/"
            "month=08/"
            "day=24/"
            "bucket=00/"
            "part-network.parquet"
        ),
        site_id="demo",
        country="KR",
        dataset="forum_post",
        sha256="a" * 64,
        row_count=1,
        file_size=1,
        stage="failed",
        failed_from_stage="local",
        retry_count=1,
        last_error="connection refused",
        retryable=True,
        retry_reason="retryable_error",
    )

    store.save(
        manifest
    )

    loaded = store.load(
        manifest.manifest_id
    )

    assert loaded is not None

    assert (
        loaded.retryable
        is True
    )

    assert (
        loaded.retry_reason
        == "retryable_error"
    )

def test_terminal_failure_is_not_retried(
    tmp_path,
):

    manager, store, connection, seen_store = (
        make_manager(
            tmp_path
        )
    )

    manifest = RecoveryManifest(
        manifest_id="terminal-test",
        local_path="/tmp/a.parquet",
        relative_path="a.parquet",
        site_id="demo",
        country="KR",
        dataset="forum_post",
        sha256="a" * 64,
        row_count=1,
        file_size=100,
        stage="failed",
        failed_from_stage="catalog_registered",
        retry_count=1,
        last_error=(
            "record index row count mismatch"
        ),
        retryable=False,
        retry_reason=(
            "non_retryable_error"
        ),
    )

    store.save(
        manifest
    )

    result = manager.recover_one(
        manifest
    )

    assert result.success

    assert (
        result.action
        == "skipped"
    )

    assert (
        result.final_stage
        == "failed"
    )

    loaded = store.load(
        manifest.manifest_id
    )

    assert (
        loaded.stage
        == "failed"
    )

def test_unclassified_failed_manifest_gets_policy(
    tmp_path,
):

    manager, store, connection, seen_store = (
        make_manager(
            tmp_path
        )
    )

    manifest = RecoveryManifest(
        manifest_id="old-failed",
        local_path="/tmp/a.parquet",
        relative_path="a.parquet",
        site_id="demo",
        country="KR",
        dataset="forum_post",
        sha256="a" * 64,
        row_count=1,
        file_size=100,
        stage="failed",
        failed_from_stage=(
            "catalog_registered"
        ),
        retry_count=1,
        last_error=(
            "schema mismatch"
        ),
        retryable=None,
        retry_reason=None,
    )

    store.save(
        manifest
    )

    result = manager.recover_one(
        manifest
    )

    assert result.success

    assert (
        result.action
        == "skipped"
    )

    loaded = store.load(
        manifest.manifest_id
    )

    assert loaded is not None

    assert (
        loaded.retryable
        is False
    )

    assert (
        loaded.retry_reason
        == "non_retryable_error"
    )

def test_max_retry_failure_is_terminal(
    tmp_path,
):

    manager, store, connection, seen_store = (
        make_manager(
            tmp_path
        )
    )

    manager.retry_policy = RetryPolicy(
        max_retries=3
    )

    manifest = RecoveryManifest(
        manifest_id="max-retry-test",
        local_path="/tmp/a.parquet",
        relative_path="a.parquet",
        site_id="demo",
        country="KR",
        dataset="forum_post",
        sha256="a" * 64,
        row_count=1,
        file_size=100,
        stage="failed",
        failed_from_stage="local",
        retry_count=3,
        last_error="connection refused",
        retryable=None,
        retry_reason=None,
    )

    store.save(
        manifest
    )

    result = manager.recover_one(
        manifest
    )

    assert result.success

    assert (
        result.action
        == "skipped"
    )

    loaded = store.load(
        manifest.manifest_id
    )

    assert (
        loaded.retryable
        is False
    )

    assert (
        loaded.retry_reason
        == "max_retries_exceeded"
    )

def test_successful_resume_clears_retry_flags(
    tmp_path,
):

    record = CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        source_id="retry-clear-1",
        scope_type="instrument",
        scope_id="XKRX:005930",
        instrument_id="XKRX:005930",
        event_time=datetime(
            2026,
            8,
            24,
            1,
            0,
            tzinfo=timezone.utc,
        ),
        title="retry clear",
    )

    partitioner = Partitioner()

    batch = FlushBatch(
        key=(
            partitioner
            .partition_for(
                record
            )
        ),
        records=(
            record,
        ),
        estimated_bytes=1000,
    )

    writer = ParquetWriter(
        tmp_path
        / "warehouse"
    )

    info = writer.write_batch(
        batch
    )

    record_index_store = (
        RecordIndexStore()
    )

    index_info = (
        record_index_store
        .write_for_parquet(
            info.file_path,
            batch.records,
        )
    )

    manager, store, connection, seen_store = (
        make_manager(
            tmp_path
        )
    )

    manifest = manifest_from_info(
        info,
        stage="catalog_registered",
    )

    from dataclasses import replace

    manifest = replace(
        manifest,
        stage="failed",
        failed_from_stage=(
            "catalog_registered"
        ),
        retry_count=1,
        last_error=(
            "connection refused"
        ),
        retryable=True,
        retry_reason="retryable_error",
        record_index_path=(
            index_info
            .file_path
            .as_posix()
        ),
    )

    store.save(
        manifest
    )

    result = manager.recover_one(
        manifest
    )

    assert result.success

    assert (
        result.final_stage
        == "deleted"
    )

    loaded = store.load(
        manifest.manifest_id
    )

    assert loaded is not None

    assert (
        loaded.retry_count
        == 1
    )

    assert (
        loaded.retryable
        is None
    )

    assert (
        loaded.retry_reason
        is None
    )

    assert (
        loaded.failed_from_stage
        is None
    )


