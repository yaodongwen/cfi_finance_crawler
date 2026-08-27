from datetime import (
    datetime,
    timezone,
)

from crawl_framework.core.models import (
    CanonicalRecord,
)
from crawl_framework.storage.buffer import (
    FlushBatch,
)
from crawl_framework.storage.cleaner import (
    Cleaner,
    CleanupContext,
    cleanup_context_from_upload,
)
from crawl_framework.storage.parquet_writer import (
    ParquetWriter,
)
from crawl_framework.storage.partition import (
    Partitioner,
)
from crawl_framework.storage.record_index import (
    RecordIndexStore,
)
from crawl_framework.storage.uploader import (
    LocalUploader,
)


def make_parquet_info(
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

    partitioner = (
        Partitioner()
    )

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


def safe_context():
    return CleanupContext(
        uploaded=True,
        verified=True,
        catalog_registered=True,
        seen_committed=True,
        checkpoint_committed=True,
    )


def test_safe_file_can_delete():

    cleaner = Cleaner()

    allowed, reason = (
        cleaner.can_delete(
            safe_context()
        )
    )

    assert (
        allowed
        is True
    )

    assert (
        reason
        == "safe to delete"
    )


def test_not_uploaded_kept():

    cleaner = Cleaner()

    context = CleanupContext(
        uploaded=False,
        verified=False,
        catalog_registered=False,
        seen_committed=False,
        checkpoint_committed=False,
    )

    allowed, reason = (
        cleaner.can_delete(
            context
        )
    )

    assert (
        allowed
        is False
    )

    assert (
        "upload"
        in reason
    )


def test_not_verified_kept():

    cleaner = Cleaner()

    context = CleanupContext(
        uploaded=True,
        verified=False,
        catalog_registered=True,
        seen_committed=True,
        checkpoint_committed=True,
    )

    allowed, reason = (
        cleaner.can_delete(
            context
        )
    )

    assert (
        allowed
        is False
    )

    assert (
        "verification"
        in reason
    )


def test_not_registered_kept():

    cleaner = Cleaner()

    context = CleanupContext(
        uploaded=True,
        verified=True,
        catalog_registered=False,
        seen_committed=True,
        checkpoint_committed=True,
    )

    allowed, reason = (
        cleaner.can_delete(
            context
        )
    )

    assert (
        allowed
        is False
    )

    assert (
        "catalog"
        in reason
    )


def test_seen_not_committed_kept():

    cleaner = Cleaner()

    context = CleanupContext(
        uploaded=True,
        verified=True,
        catalog_registered=True,
        seen_committed=False,
        checkpoint_committed=True,
    )

    allowed, reason = (
        cleaner.can_delete(
            context
        )
    )

    assert (
        allowed
        is False
    )

    assert (
        "seen"
        in reason
    )


def test_checkpoint_not_committed_kept():

    cleaner = Cleaner()

    context = CleanupContext(
        uploaded=True,
        verified=True,
        catalog_registered=True,
        seen_committed=True,
        checkpoint_committed=False,
    )

    allowed, reason = (
        cleaner.can_delete(
            context
        )
    )

    assert (
        allowed
        is False
    )

    assert (
        "checkpoint"
        in reason
    )


def test_clean_deletes_file(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    assert (
        info.file_path.exists()
    )

    cleaner = Cleaner()

    result = cleaner.clean(
        info,
        safe_context(),
    )

    assert (
        result.deleted
        is True
    )

    assert (
        result.decision
        == "delete"
    )

    assert not (
        info.file_path.exists()
    )


def test_clean_keeps_unsafe_file(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    cleaner = Cleaner()

    context = CleanupContext(
        uploaded=True,
        verified=False,
        catalog_registered=True,
        seen_committed=True,
        checkpoint_committed=True,
    )

    result = cleaner.clean(
        info,
        context,
    )

    assert (
        result.deleted
        is False
    )

    assert (
        result.decision
        == "keep"
    )

    assert (
        info.file_path.exists()
    )


def test_dry_run_keeps_file(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    cleaner = Cleaner(
        dry_run=True
    )

    result = cleaner.clean(
        info,
        safe_context(),
    )

    assert (
        result.deleted
        is False
    )

    assert (
        result.decision
        == "delete"
    )

    assert (
        info.file_path.exists()
    )


def test_context_from_verified_upload(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    uploader = LocalUploader(
        tmp_path
        / "remote",
        verify_size=True,
        verify_sha256=True,
    )

    upload_result = (
        uploader.upload(
            info
        )
    )

    context = (
        cleanup_context_from_upload(
            upload_result,
            catalog_registered=True,
            seen_committed=True,
            checkpoint_committed=True,
        )
    )

    assert (
        context.uploaded
        is True
    )

    assert (
        context.verified
        is True
    )

    cleaner = Cleaner()

    allowed, _ = (
        cleaner.can_delete(
            context
        )
    )

    assert (
        allowed
        is True
    )

def test_clean_deletes_record_index(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    record_index_store = (
        RecordIndexStore()
    )

    record_index_store.write_for_parquet(
        info.file_path,
        [
            CanonicalRecord(
                site_id="naver_finance",
                country="KR",
                dataset="forum_post",
                source_id="sidecar-1",
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
                title="sidecar",
            )
        ],
    )

    assert (
        record_index_store
        .exists_for_parquet(
            info.file_path
        )
        is True
    )

    cleaner = Cleaner(
        record_index_store=(
            record_index_store
        )
    )

    result = cleaner.clean(
        info,
        safe_context(),
    )

    assert (
        result.deleted
        is True
    )

    assert (
        result.record_index_deleted
        is True
    )

    assert (
        record_index_store
        .exists_for_parquet(
            info.file_path
        )
        is False
    )


def test_unsafe_clean_keeps_record_index(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    record_index_store = (
        RecordIndexStore()
    )

    record_index_store.write_for_parquet(
        info.file_path,
        [
            CanonicalRecord(
                site_id="naver_finance",
                country="KR",
                dataset="forum_post",
                source_id="sidecar-2",
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
                title="sidecar",
            )
        ],
    )

    context = CleanupContext(
        uploaded=True,
        verified=False,
        catalog_registered=True,
        seen_committed=True,
        checkpoint_committed=True,
    )

    cleaner = Cleaner(
        record_index_store=(
            record_index_store
        )
    )

    result = cleaner.clean(
        info,
        context,
    )

    assert (
        result.deleted
        is False
    )

    assert (
        result.record_index_deleted
        is False
    )

    assert (
        record_index_store
        .exists_for_parquet(
            info.file_path
        )
        is True
    )


def test_dry_run_keeps_record_index(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    record_index_store = (
        RecordIndexStore()
    )

    record_index_store.write_for_parquet(
        info.file_path,
        [
            CanonicalRecord(
                site_id="naver_finance",
                country="KR",
                dataset="forum_post",
                source_id="sidecar-3",
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
                title="sidecar",
            )
        ],
    )

    cleaner = Cleaner(
        dry_run=True,
        record_index_store=(
            record_index_store
        ),
    )

    result = cleaner.clean(
        info,
        safe_context(),
    )

    assert (
        result.deleted
        is False
    )

    assert (
        result.record_index_deleted
        is False
    )

    assert (
        info.file_path.exists()
    )

    assert (
        record_index_store
        .exists_for_parquet(
            info.file_path
        )
        is True
    )


def test_missing_parquet_can_cleanup_sidecar(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    record_index_store = (
        RecordIndexStore()
    )

    record_index_store.write_for_parquet(
        info.file_path,
        [
            CanonicalRecord(
                site_id="naver_finance",
                country="KR",
                dataset="forum_post",
                source_id="sidecar-4",
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
                title="sidecar",
            )
        ],
    )

    info.file_path.unlink()

    cleaner = Cleaner(
        record_index_store=(
            record_index_store
        )
    )

    result = cleaner.clean(
        info,
        safe_context(),
    )

    assert (
        result.deleted
        is False
    )

    assert (
        result.record_index_deleted
        is True
    )

    assert (
        record_index_store
        .exists_for_parquet(
            info.file_path
        )
        is False
    )