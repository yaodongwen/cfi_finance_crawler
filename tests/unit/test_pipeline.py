from datetime import (
    datetime,
    timezone,
)

from crawl_framework.core.models import (
    CanonicalRecord,
)
from crawl_framework.core.pipeline import (
    StoragePipeline,
)
from crawl_framework.storage.buffer import (
    BufferConfig,
    RecordBuffer,
)
from crawl_framework.storage.cleaner import (
    Cleaner,
    CleanupResult,
)
from crawl_framework.storage.parquet_writer import (
    ParquetWriter,
)
from crawl_framework.storage.postgres import (
    PostgresCatalog,
)
from crawl_framework.storage.recovery import (
    RecoveryStore,
)
from crawl_framework.storage.seen_store import (
    SQLiteSeenStore,
)
from crawl_framework.storage.uploader import (
    LocalUploader,
)


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


class KeepFilesCleaner(
    Cleaner
):
    """
    Pipeline integration test 专用 Cleaner。

    模拟已经满足清理条件，
    但暂时保留本地 parquet 和 sidecar，
    方便测试读取 sidecar 内容。

    生产代码不使用。
    """

    def clean(
        self,
        info,
        context,
    ):

        return CleanupResult(
            file_path=info.file_path,
            decision="delete",
            deleted=False,
            reason="test: keep files",
            record_index_deleted=False,
        )

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


def make_pipeline_keep_files(
    tmp_path,
):

    seen_store = SQLiteSeenStore(
        tmp_path
        / "state"
        / "seen.sqlite3"
    )

    buffer = RecordBuffer(
        config=BufferConfig(
            min_rows=2,
            max_rows=2,
            target_bytes=(
                100
                * 1024
                * 1024
            ),
            flush_seconds=30,
        )
    )

    writer = ParquetWriter(
        tmp_path
        / "warehouse"
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

    recovery = RecoveryStore(
        tmp_path
        / "state"
        / "recovery"
    )

    pipeline = StoragePipeline(
        seen_store=seen_store,
        buffer=buffer,
        parquet_writer=writer,
        uploader=uploader,
        catalog=catalog,
        recovery_store=recovery,
        cleaner=KeepFilesCleaner(),
    )

    return (
        pipeline,
        seen_store,
        connection,
        recovery,
    )

def make_record(
    source_id: str,
    *,
    title: str = "hello",
) -> CanonicalRecord:

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
            tzinfo=timezone.utc,
        ),
        title=title,
    )


def make_pipeline(
    tmp_path,
):

    seen_store = (
        SQLiteSeenStore(
            tmp_path
            / "state"
            / "seen.sqlite3"
        )
    )

    buffer = RecordBuffer(
        config=BufferConfig(
            min_rows=2,
            max_rows=2,
            target_bytes=(
                100 * 1024 * 1024
            ),
            flush_seconds=30,
        )
    )

    writer = ParquetWriter(
        tmp_path
        / "warehouse"
    )

    uploader = LocalUploader(
        tmp_path
        / "remote",
        verify_size=True,
        verify_sha256=True,
    )

    connection = (
        FakeConnection()
    )

    catalog = PostgresCatalog(
        connection
    )

    recovery = RecoveryStore(
        tmp_path
        / "state"
        / "recovery"
    )

    cleaner = Cleaner()

    pipeline = StoragePipeline(
        seen_store=seen_store,
        buffer=buffer,
        parquet_writer=writer,
        uploader=uploader,
        catalog=catalog,
        recovery_store=recovery,
        cleaner=cleaner,
    )

    return (
        pipeline,
        seen_store,
        connection,
        recovery,
    )


def test_submit_new_record(
    tmp_path,
):

    (
        pipeline,
        seen_store,
        connection,
        recovery,
    ) = make_pipeline(
        tmp_path
    )

    record = make_record(
        "1"
    )

    decision, batches = (
        pipeline.submit(
            record
        )
    )

    assert (
        decision.decision
        == "new"
    )

    assert (
        decision.buffered
        is True
    )

    assert (
        len(batches)
        == 0
    )

    assert (
        seen_store.count()
        == 0
    )


def test_two_records_trigger_flush(
    tmp_path,
):

    (
        pipeline,
        seen_store,
        connection,
        recovery,
    ) = make_pipeline(
        tmp_path
    )

    pipeline.submit(
        make_record(
            "1"
        )
    )

    decision, batches = (
        pipeline.submit(
            make_record(
                "2"
            )
        )
    )

    assert (
        len(batches)
        == 1
    )

    result = (
        batches[0]
    )

    assert (
        result.upload_result.verified
        is True
    )

    assert (
        result.catalog_registered
        is True
    )

    assert (
        result.seen_committed
        is True
    )

    assert (
        result.local_deleted
        is True
    )

    assert (
        seen_store.count()
        == 2
    )


def test_remote_file_exists_after_cleanup(
    tmp_path,
):

    (
        pipeline,
        seen_store,
        connection,
        recovery,
    ) = make_pipeline(
        tmp_path
    )

    pipeline.submit(
        make_record(
            "1"
        )
    )

    _, batches = pipeline.submit(
        make_record(
            "2"
        )
    )

    result = batches[0]

    assert not (
        result.parquet_info
        .file_path
        .exists()
    )

    remote_path = (
        result.upload_result
        .remote_path
    )

    from pathlib import Path

    assert (
        Path(
            remote_path
        ).exists()
    )


def test_unchanged_record_skipped(
    tmp_path,
):

    (
        pipeline,
        seen_store,
        connection,
        recovery,
    ) = make_pipeline(
        tmp_path
    )

    first = make_record(
        "1"
    )

    second = make_record(
        "2"
    )

    pipeline.submit(
        first
    )

    pipeline.submit(
        second
    )

    decision, batches = (
        pipeline.submit(
            first
        )
    )

    assert (
        decision.decision
        == "unchanged"
    )

    assert (
        decision.buffered
        is False
    )

    assert (
        batches
        == []
    )


def test_updated_record_is_processed(
    tmp_path,
):

    (
        pipeline,
        seen_store,
        connection,
        recovery,
    ) = make_pipeline(
        tmp_path
    )

    old = make_record(
        "1",
        title="old",
    )

    filler = make_record(
        "2"
    )

    pipeline.submit(
        old
    )

    pipeline.submit(
        filler
    )

    updated = make_record(
        "1",
        title="new",
    )

    decision, batches = (
        pipeline.submit(
            updated
        )
    )

    assert (
        decision.decision
        == "updated"
    )

    assert (
        decision.buffered
        is True
    )


def test_flush_all(
    tmp_path,
):

    (
        pipeline,
        seen_store,
        connection,
        recovery,
    ) = make_pipeline(
        tmp_path
    )

    pipeline.submit(
        make_record(
            "1"
        )
    )

    assert (
        seen_store.count()
        == 0
    )

    results = (
        pipeline.flush_all()
    )

    assert (
        len(results)
        == 1
    )

    assert (
        seen_store.count()
        == 1
    )


def test_recovery_manifest_deleted_stage(
    tmp_path,
):

    (
        pipeline,
        seen_store,
        connection,
        recovery,
    ) = make_pipeline(
        tmp_path
    )

    pipeline.submit(
        make_record(
            "1"
        )
    )

    _, batches = pipeline.submit(
        make_record(
            "2"
        )
    )

    manifests = (
        recovery.list_all()
    )

    assert (
        len(manifests)
        == 1
    )

    assert (
        manifests[0].stage
        == "deleted"
    )


def test_pipeline_stats(
    tmp_path,
):

    (
        pipeline,
        seen_store,
        connection,
        recovery,
    ) = make_pipeline(
        tmp_path
    )

    pipeline.submit(
        make_record(
            "1"
        )
    )

    pipeline.submit(
        make_record(
            "2"
        )
    )

    assert (
        pipeline.stats.records_seen
        == 2
    )

    assert (
        pipeline.stats.records_new
        == 2
    )

    assert (
        pipeline.stats.files_written
        == 1
    )

    assert (
        pipeline.stats.files_verified
        == 1
    )

    assert (
        pipeline.stats.files_registered
        == 1
    )

    assert (
        pipeline.stats.files_deleted
        == 1
    )


def test_postgres_was_called(
    tmp_path,
):

    (
        pipeline,
        seen_store,
        connection,
        recovery,
    ) = make_pipeline(
        tmp_path
    )

    pipeline.submit(
        make_record(
            "1"
        )
    )

    pipeline.submit(
        make_record(
            "2"
        )
    )

    # ========================================================
    # 现在完整持久化流程应执行两条 PostgreSQL SQL：
    #
    # 1. INSERT / UPSERT data_files
    # 2. UPDATE storage_status / remote_path
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
    # First SQL:
    # register_parquet_file()
    # ========================================================

    assert (
        "INSERT INTO marketdata.data_files"
        in first_sql
    )

    assert (
        "file_path"
        in first_params
    )

    # ========================================================
    # Second SQL:
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
        "remote_path"
        in second_sql
    )

    assert (
        second_params[
            "file_path"
        ]
        ==
        first_params[
            "file_path"
        ]
    )

    assert (
        second_params[
            "remote_path"
        ]
    )

    
def test_submit_with_scope_token(
    tmp_path,
):

    (
        pipeline,
        seen_store,
        connection,
        recovery,
    ) = make_pipeline(
        tmp_path
    )

    record = make_record(
        "scope-test-1"
    )

    decision, batches = (
        pipeline.submit(
            record,
            scope_token="scope-005930",
        )
    )

    assert (
        decision.decision
        == "new"
    )

    assert (
        decision.buffered
        is True
    )

    assert (
        pipeline.has_scope(
            "scope-005930"
        )
        is True
    )


def test_flush_scope_makes_record_durable(
    tmp_path,
):

    (
        pipeline,
        seen_store,
        connection,
        recovery,
    ) = make_pipeline(
        tmp_path
    )

    # make_pipeline() 中 max_rows=2，
    # 所以只提交 1 条，不会自动 flush。
    record = make_record(
        "scope-durable-1"
    )

    pipeline.submit(
        record,
        scope_token="scope-005930",
    )

    assert (
        seen_store.count()
        == 0
    )

    assert (
        pipeline.has_scope(
            "scope-005930"
        )
        is True
    )

    results = (
        pipeline.flush_scope(
            "scope-005930"
        )
    )

    assert (
        len(results)
        == 1
    )

    assert (
        seen_store.count()
        == 1
    )

    assert (
        pipeline.has_scope(
            "scope-005930"
        )
        is False
    )

    assert (
        results[0]
        .upload_result
        .verified
        is True
    )


def test_flush_unknown_scope_returns_empty(
    tmp_path,
):

    (
        pipeline,
        seen_store,
        connection,
        recovery,
    ) = make_pipeline(
        tmp_path
    )

    results = (
        pipeline.flush_scope(
            "scope-not-found"
        )
    )

    assert (
        results
        == []
    )


def test_submit_many_with_scope_token(
    tmp_path,
):

    (
        pipeline,
        seen_store,
        connection,
        recovery,
    ) = make_pipeline(
        tmp_path
    )

    records = [
        make_record(
            "many-1"
        ),
        make_record(
            "many-2"
        ),
        make_record(
            "many-3"
        ),
    ]

    decisions = (
        pipeline.submit_many(
            records,
            scope_token="scope-many",
        )
    )

    assert (
        len(decisions)
        == 3
    )

    # max_rows=2，
    # 前两条已经自动形成 durable batch。
    assert (
        seen_store.count()
        == 2
    )

    # 第三条还在 Buffer，
    # scope 应仍然存在。
    assert (
        pipeline.has_scope(
            "scope-many"
        )
        is True
    )

    pipeline.flush_scope(
        "scope-many"
    )

    assert (
        seen_store.count()
        == 3
    )

    assert (
        pipeline.has_scope(
            "scope-many"
        )
        is False
    )


def test_scope_partition_count(
    tmp_path,
):

    (
        pipeline,
        seen_store,
        connection,
        recovery,
    ) = make_pipeline(
        tmp_path
    )

    record = make_record(
        "partition-count-1"
    )

    pipeline.submit(
        record,
        scope_token="scope-count",
    )

    assert (
        pipeline
        .buffered_scope_partition_count(
            "scope-count"
        )
        >= 1
    )

    pipeline.flush_scope(
        "scope-count"
    )

    assert (
        pipeline
        .buffered_scope_partition_count(
            "scope-count"
        )
        == 0
    )

def test_pipeline_records_record_index_path(
    tmp_path,
):

    (
        pipeline,
        seen_store,
        connection,
        recovery,
    ) = make_pipeline(
        tmp_path
    )

    pipeline.submit(
        make_record(
            "index-1"
        )
    )

    _, batches = pipeline.submit(
        make_record(
            "index-2"
        )
    )

    assert (
        len(batches)
        == 1
    )

    manifests = (
        recovery.list_all()
    )

    assert (
        len(manifests)
        == 1
    )

    manifest = manifests[0]

    # Pipeline 确实创建并记录了 sidecar 路径。
    assert (
        manifest.record_index_path
        is not None
    )

    assert (
        manifest.record_index_path
        .endswith(
            ".records.jsonl"
        )
    )

    # 完整生命周期成功后，
    # Cleaner 应该已经删除 parquet + sidecar。
    from pathlib import Path

    index_path = Path(
        manifest.record_index_path
    )

    assert (
        index_path.exists()
        is False
    )

    assert (
        manifest.stage
        == "deleted"
    )

def test_record_index_matches_batch_records(
    tmp_path,
):

    (
        pipeline,
        seen_store,
        connection,
        recovery,
    ) = make_pipeline_keep_files(
        tmp_path
    )

    first = make_record(
        "index-a"
    )

    second = make_record(
        "index-b"
    )

    pipeline.submit(
        first
    )

    pipeline.submit(
        second
    )

    manifest = (
        recovery.list_all()[0]
    )

    assert (
        manifest.record_index_path
        is not None
    )

    from pathlib import Path

    index_path = Path(
        manifest.record_index_path
    )

    # 测试专用 Cleaner 没删文件，
    # 所以这里应该仍然存在。
    assert (
        index_path.exists()
        is True
    )

    pairs = (
        pipeline
        .record_index_store
        .reader
        .read_pairs(
            index_path
        )
    )

    assert pairs == [
        (
            first.record_uid,
            first.version_hash,
        ),
        (
            second.record_uid,
            second.version_hash,
        ),
    ]

    (
        pipeline,
        seen_store,
        connection,
        recovery,
    ) = make_pipeline(
        tmp_path
    )

    first = make_record(
        "index-a"
    )

    second = make_record(
        "index-b"
    )

    pipeline.submit(
        first
    )

    pipeline.submit(
        second
    )

    manifest = (
        recovery.list_all()[0]
    )

    pairs = (
        pipeline
        .record_index_store
        .reader
        .read_pairs(
            manifest.record_index_path
        )
    )

    assert pairs == [
        (
            first.record_uid,
            first.version_hash,
        ),
        (
            second.record_uid,
            second.version_hash,
        ),
    ]