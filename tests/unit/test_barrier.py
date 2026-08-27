from datetime import (
    datetime,
    timezone,
)

import pytest

from crawl_framework.core.barrier import (
    PipelineBarrier,
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


    def cursor(
        self,
    ):

        return FakeCursor(
            self.executed
        )


    def commit(
        self,
    ):

        pass


# ============================================================
# Helpers
# ============================================================


def make_record(
    source_id: str,
    *,
    instrument_id: str = "XKRX:005930",
) -> CanonicalRecord:

    return CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        source_id=source_id,
        scope_type="instrument",
        scope_id=instrument_id,
        instrument_id=instrument_id,
        event_time=datetime(
            2026,
            8,
            24,
            1,
            0,
            tzinfo=timezone.utc,
        ),
        title=f"title-{source_id}",
    )


def make_pipeline(
    tmp_path,
):

    seen = SQLiteSeenStore(
        tmp_path
        / "state"
        / "seen.sqlite3"
    )

    buffer = RecordBuffer(
        config=BufferConfig(
            min_rows=100,
            max_rows=100,
            target_bytes=(
                100_000_000
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

    catalog = PostgresCatalog(
        FakeConnection()
    )

    recovery = RecoveryStore(
        tmp_path
        / "state"
        / "recovery"
    )

    pipeline = StoragePipeline(
        seen_store=seen,
        buffer=buffer,
        parquet_writer=writer,
        uploader=uploader,
        catalog=catalog,
        recovery_store=recovery,
        cleaner=Cleaner(),
    )

    return (
        pipeline,
        seen,
    )


# ============================================================
# Tests
# ============================================================


def test_begin_scope(
    tmp_path,
):

    pipeline, _ = (
        make_pipeline(
            tmp_path
        )
    )

    barrier = PipelineBarrier(
        pipeline
    )

    barrier.begin_scope(
        "scope-005930"
    )

    assert (
        barrier.active
        is True
    )

    assert (
        barrier.scope_token
        == "scope-005930"
    )

    assert (
        barrier.tracked_count
        == 0
    )


def test_empty_barrier(
    tmp_path,
):

    pipeline, _ = (
        make_pipeline(
            tmp_path
        )
    )

    barrier = PipelineBarrier(
        pipeline
    )

    barrier.begin_scope(
        "scope-005930"
    )

    result = (
        barrier.wait_until_durable()
    )

    assert (
        result.scope_token
        == "scope-005930"
    )

    assert (
        result.submitted_records
        == 0
    )

    assert (
        result.durable_records
        == 0
    )

    assert (
        result.flushed_batches
        == 0
    )


def test_track_requires_active_scope(
    tmp_path,
):

    pipeline, _ = (
        make_pipeline(
            tmp_path
        )
    )

    barrier = PipelineBarrier(
        pipeline
    )

    with pytest.raises(
        RuntimeError
    ):

        barrier.track(
            make_record(
                "1"
            )
        )


def test_wait_requires_active_scope(
    tmp_path,
):

    pipeline, _ = (
        make_pipeline(
            tmp_path
        )
    )

    barrier = PipelineBarrier(
        pipeline
    )

    with pytest.raises(
        RuntimeError
    ):

        barrier.wait_until_durable()


def test_cannot_begin_second_scope(
    tmp_path,
):

    pipeline, _ = (
        make_pipeline(
            tmp_path
        )
    )

    barrier = PipelineBarrier(
        pipeline
    )

    barrier.begin_scope(
        "scope-A"
    )

    with pytest.raises(
        RuntimeError
    ):

        barrier.begin_scope(
            "scope-B"
        )


def test_track_record(
    tmp_path,
):

    pipeline, _ = (
        make_pipeline(
            tmp_path
        )
    )

    barrier = PipelineBarrier(
        pipeline
    )

    record = make_record(
        "1"
    )

    barrier.begin_scope(
        "scope-005930"
    )

    barrier.track(
        record
    )

    assert (
        barrier.tracked_count
        == 1
    )

    assert (
        barrier.all_durable()
        is False
    )


def test_scope_barrier_flushes_pending_record(
    tmp_path,
):

    pipeline, seen = (
        make_pipeline(
            tmp_path
        )
    )

    token = "scope-005930"

    barrier = PipelineBarrier(
        pipeline
    )

    barrier.begin_scope(
        token
    )

    record = make_record(
        "1"
    )

    pipeline.submit(
        record,
        scope_token=token,
    )

    barrier.track(
        record
    )

    assert (
        seen.count()
        == 0
    )

    assert (
        pipeline.has_scope(
            token
        )
        is True
    )

    result = (
        barrier.wait_until_durable()
    )

    assert (
        result.submitted_records
        == 1
    )

    assert (
        result.durable_records
        == 1
    )

    assert (
        result.flushed_batches
        == 1
    )

    assert (
        seen.count()
        == 1
    )

    assert (
        pipeline.has_scope(
            token
        )
        is False
    )


def test_multiple_records_become_durable(
    tmp_path,
):

    pipeline, seen = (
        make_pipeline(
            tmp_path
        )
    )

    token = "scope-005930"

    barrier = PipelineBarrier(
        pipeline
    )

    barrier.begin_scope(
        token
    )

    records = [
        make_record(
            str(index)
        )
        for index
        in range(
            10
        )
    ]

    for record in records:

        pipeline.submit(
            record,
            scope_token=token,
        )

        barrier.track(
            record
        )

    result = (
        barrier.wait_until_durable()
    )

    assert (
        result.submitted_records
        == 10
    )

    assert (
        result.durable_records
        == 10
    )

    assert (
        seen.count()
        == 10
    )


def test_already_durable_needs_no_scope_flush(
    tmp_path,
):

    pipeline, seen = (
        make_pipeline(
            tmp_path
        )
    )

    token = "scope-005930"

    record = make_record(
        "1"
    )

    pipeline.submit(
        record,
        scope_token=token,
    )

    pipeline.flush_scope(
        token
    )

    assert (
        seen.count()
        == 1
    )

    barrier = PipelineBarrier(
        pipeline
    )

    barrier.begin_scope(
        token
    )

    barrier.track(
        record
    )

    result = (
        barrier.wait_until_durable()
    )

    assert (
        result.durable_records
        == 1
    )

    assert (
        result.flushed_batches
        == 0
    )


def test_end_scope_clears_state(
    tmp_path,
):

    pipeline, _ = (
        make_pipeline(
            tmp_path
        )
    )

    barrier = PipelineBarrier(
        pipeline
    )

    barrier.begin_scope(
        "scope-005930"
    )

    barrier.track(
        make_record(
            "1"
        )
    )

    barrier.end_scope()

    assert (
        barrier.active
        is False
    )

    assert (
        barrier.scope_token
        is None
    )

    assert (
        barrier.tracked_count
        == 0
    )


def test_empty_scope_token_rejected(
    tmp_path,
):

    pipeline, _ = (
        make_pipeline(
            tmp_path
        )
    )

    barrier = PipelineBarrier(
        pipeline
    )

    with pytest.raises(
        ValueError
    ):

        barrier.begin_scope(
            "   "
        )


def test_flush_scope_does_not_flush_unrelated_partition(
    tmp_path,
):

    pipeline, seen = (
        make_pipeline(
            tmp_path
        )
    )

    first = make_record(
        "first",
        instrument_id="XKRX:005930",
    )

    first_key = (
        pipeline.buffer
        .partitioner
        .partition_for(
            first
        )
    )

    second = None

    candidates = [
        "XKRX:000660",
        "XKRX:035420",
        "XKRX:051910",
        "XKRX:068270",
        "XNAS:AAPL",
        "XNAS:MSFT",
    ]

    for instrument_id in candidates:

        candidate = make_record(
            instrument_id,
            instrument_id=instrument_id,
        )

        candidate_key = (
            pipeline.buffer
            .partitioner
            .partition_for(
                candidate
            )
        )

        if (
            candidate_key
            != first_key
        ):

            second = candidate

            break

    assert (
        second is not None
    )

    pipeline.submit(
        first,
        scope_token="scope-A",
    )

    pipeline.submit(
        second,
        scope_token="scope-B",
    )

    barrier = PipelineBarrier(
        pipeline
    )

    barrier.begin_scope(
        "scope-A"
    )

    barrier.track(
        first
    )

    result = (
        barrier.wait_until_durable()
    )

    assert (
        result.durable_records
        == 1
    )

    # A 已经 durable
    assert (
        barrier.is_durable(
            first.record_uid,
            first.version_hash,
        )
        is True
    )

    # B 仍然留在 Buffer
    assert (
        pipeline.has_scope(
            "scope-B"
        )
        is True
    )

    # SeenStore 只有 A
    assert (
        seen.count()
        == 1
    )