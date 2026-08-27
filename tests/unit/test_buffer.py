from datetime import (
    datetime,
    timezone,
)

from crawl_framework.core.models import (
    CanonicalRecord,
)
from crawl_framework.storage.buffer import (
    BufferConfig,
    RecordBuffer,
    estimate_record_bytes,
)
from crawl_framework.storage.partition import (
    Partitioner,
)


def make_record(
    *,
    source_id: str,
    instrument_id: str = "XKRX:005930",
    content: str = "hello",
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
        content=content,
    )


def test_estimate_record_bytes():

    record = make_record(
        source_id="1",
        content="hello world",
    )

    size = estimate_record_bytes(
        record
    )

    assert (
        size > 0
    )


def test_add_record_without_flush():

    buffer = RecordBuffer(
        config=BufferConfig(
            min_rows=10,
            max_rows=100,
            target_bytes=1_000_000,
            flush_seconds=30,
        )
    )

    result = buffer.add(
        make_record(
            source_id="1"
        )
    )

    assert (
        result is None
    )

    assert (
        len(buffer)
        == 1
    )

    assert (
        buffer.partition_count
        == 1
    )


def test_flush_by_max_rows():

    buffer = RecordBuffer(
        config=BufferConfig(
            min_rows=2,
            max_rows=3,
            target_bytes=1_000_000,
            flush_seconds=30,
        )
    )

    assert (
        buffer.add(
            make_record(
                source_id="1"
            )
        )
        is None
    )

    assert (
        buffer.add(
            make_record(
                source_id="2"
            )
        )
        is None
    )

    batch = buffer.add(
        make_record(
            source_id="3"
        )
    )

    assert (
        batch is not None
    )

    assert (
        batch.row_count
        == 3
    )

    assert (
        len(buffer)
        == 0
    )


def test_flush_by_target_bytes():

    buffer = RecordBuffer(
        config=BufferConfig(
            min_rows=2,
            max_rows=100,
            target_bytes=100,
            flush_seconds=30,
        )
    )

    assert (
        buffer.add(
            make_record(
                source_id="1",
                content="x" * 500,
            )
        )
        is None
    )

    batch = buffer.add(
        make_record(
            source_id="2",
            content="y" * 500,
        )
    )

    assert (
        batch is not None
    )

    assert (
        batch.row_count
        == 2
    )

    assert (
        batch.estimated_bytes
        >= 100
    )


def test_different_instruments_can_use_different_buckets():

    partitioner = Partitioner(
        bucket_count=256
    )

    buffer = RecordBuffer(
        partitioner=partitioner,
        config=BufferConfig(
            min_rows=100,
            max_rows=1000,
            target_bytes=1_000_000,
            flush_seconds=30,
        ),
    )

    records = [
        make_record(
            source_id="1",
            instrument_id="XKRX:005930",
        ),
        make_record(
            source_id="2",
            instrument_id="XKRX:000660",
        ),
    ]

    buffer.add_many(
        records
    )

    assert (
        len(buffer)
        == 2
    )

    assert (
        buffer.partition_count
        >= 1
    )


def test_same_instrument_same_partition():

    buffer = RecordBuffer(
        config=BufferConfig(
            min_rows=100,
            max_rows=1000,
            target_bytes=1_000_000,
            flush_seconds=30,
        )
    )

    buffer.add_many(
        [
            make_record(
                source_id="1"
            ),
            make_record(
                source_id="2"
            ),
        ]
    )

    assert (
        len(buffer)
        == 2
    )

    assert (
        buffer.partition_count
        == 1
    )


def test_collect_expired():

    buffer = RecordBuffer(
        config=BufferConfig(
            min_rows=100,
            max_rows=1000,
            target_bytes=1_000_000,
            flush_seconds=10,
        )
    )

    buffer.add(
        make_record(
            source_id="1"
        )
    )

    internal_partition = next(
        iter(
            buffer._buffers.values()
        )
    )

    now = (
        internal_partition.created_monotonic
        + 11
    )

    batches = buffer.collect_expired(
        now=now
    )

    assert (
        len(batches)
        == 1
    )

    assert (
        batches[0].row_count
        == 1
    )

    assert (
        len(buffer)
        == 0
    )


def test_flush_all():

    buffer = RecordBuffer(
        config=BufferConfig(
            min_rows=100,
            max_rows=1000,
            target_bytes=1_000_000,
            flush_seconds=30,
        )
    )

    buffer.add_many(
        [
            make_record(
                source_id="1",
                instrument_id="XKRX:005930",
            ),
            make_record(
                source_id="2",
                instrument_id="XKRX:000660",
            ),
            make_record(
                source_id="3",
                instrument_id="XKRX:035420",
            ),
        ]
    )

    batches = buffer.flush_all()

    assert (
        sum(
            batch.row_count
            for batch in batches
        )
        == 3
    )

    assert (
        len(buffer)
        == 0
    )

    assert (
        buffer.partition_count
        == 0
    )


def test_add_many_can_emit_multiple_batches():

    buffer = RecordBuffer(
        config=BufferConfig(
            min_rows=2,
            max_rows=2,
            target_bytes=1_000_000,
            flush_seconds=30,
        )
    )

    records = [
        make_record(
            source_id=str(index)
        )
        for index
        in range(
            5
        )
    ]

    batches = buffer.add_many(
        records
    )

    assert (
        len(batches)
        == 2
    )

    assert (
        batches[0].row_count
        == 2
    )

    assert (
        batches[1].row_count
        == 2
    )

    assert (
        len(buffer)
        == 1
    )

def test_scope_token_is_recorded():

    buffer = RecordBuffer(
        config=BufferConfig(
            min_rows=100,
            max_rows=1000,
            target_bytes=1_000_000,
            flush_seconds=30,
        )
    )

    buffer.add(
        make_record(
            source_id="1"
        ),
        scope_token="scope-005930",
    )

    assert (
        buffer.has_scope(
            "scope-005930"
        )
        is True
    )

    assert (
        buffer.has_scope(
            "scope-000660"
        )
        is False
    )


def test_flush_scope():

    buffer = RecordBuffer(
        config=BufferConfig(
            min_rows=100,
            max_rows=1000,
            target_bytes=1_000_000,
            flush_seconds=30,
        )
    )

    buffer.add(
        make_record(
            source_id="1",
            instrument_id="XKRX:005930",
        ),
        scope_token="scope-005930",
    )

    buffer.add(
        make_record(
            source_id="2",
            instrument_id="XKRX:000660",
        ),
        scope_token="scope-000660",
    )

    assert (
        len(buffer)
        == 2
    )

    batches = buffer.flush_scope(
        "scope-005930"
    )

    assert (
        len(batches)
        >= 1
    )

    assert (
        sum(
            batch.row_count
            for batch
            in batches
        )
        >= 1
    )

    assert (
        buffer.has_scope(
            "scope-005930"
        )
        is False
    )


def test_flush_scope_does_not_flush_unrelated_scope():

    buffer = RecordBuffer(
        partitioner=Partitioner(
            bucket_count=256
        ),
        config=BufferConfig(
            min_rows=100,
            max_rows=1000,
            target_bytes=1_000_000,
            flush_seconds=30,
        ),
    )

    first = make_record(
        source_id="1",
        instrument_id="XKRX:005930",
    )

    second = None

    # 找一个实际落入不同 partition bucket 的证券。
    candidates = [
        "XKRX:000660",
        "XKRX:035420",
        "XKRX:051910",
        "XKRX:068270",
        "XNAS:AAPL",
        "XNAS:MSFT",
    ]

    first_key = (
        buffer.partitioner
        .partition_for(
            first
        )
    )

    for instrument_id in candidates:

        candidate = make_record(
            source_id=instrument_id,
            instrument_id=instrument_id,
        )

        candidate_key = (
            buffer.partitioner
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

    buffer.add(
        first,
        scope_token="scope-first",
    )

    buffer.add(
        second,
        scope_token="scope-second",
    )

    assert (
        buffer.has_scope(
            "scope-first"
        )
    )

    assert (
        buffer.has_scope(
            "scope-second"
        )
    )

    buffer.flush_scope(
        "scope-first"
    )

    assert (
        buffer.has_scope(
            "scope-first"
        )
        is False
    )

    # 不相关 partition 不应被 flush。
    assert (
        buffer.has_scope(
            "scope-second"
        )
        is True
    )


def test_batch_contains_scope_token():

    buffer = RecordBuffer(
        config=BufferConfig(
            min_rows=2,
            max_rows=2,
            target_bytes=1_000_000,
            flush_seconds=30,
        )
    )

    assert (
        buffer.add(
            make_record(
                source_id="1"
            ),
            scope_token="scope-A",
        )
        is None
    )

    batch = buffer.add(
        make_record(
            source_id="2"
        ),
        scope_token="scope-A",
    )

    assert (
        batch is not None
    )

    assert (
        batch.contains_scope(
            "scope-A"
        )
        is True
    )

    assert (
        batch.scope_tokens
        == frozenset(
            {
                "scope-A"
            }
        )
    )


def test_multiple_scope_tokens_in_same_partition():

    buffer = RecordBuffer(
        config=BufferConfig(
            min_rows=100,
            max_rows=1000,
            target_bytes=1_000_000,
            flush_seconds=30,
        )
    )

    buffer.add(
        make_record(
            source_id="1"
        ),
        scope_token="scope-A",
    )

    buffer.add(
        make_record(
            source_id="2"
        ),
        scope_token="scope-B",
    )

    batches = buffer.flush_scope(
        "scope-A"
    )

    assert (
        len(batches)
        == 1
    )

    batch = batches[0]

    assert (
        batch.scope_tokens
        == frozenset(
            {
                "scope-A",
                "scope-B",
            }
        )
    )

    # 同 partition 一起被 flush。
    assert (
        buffer.has_scope(
            "scope-B"
        )
        is False
    )


def test_add_many_with_scope_token():

    buffer = RecordBuffer(
        config=BufferConfig(
            min_rows=100,
            max_rows=1000,
            target_bytes=1_000_000,
            flush_seconds=30,
        )
    )

    buffer.add_many(
        [
            make_record(
                source_id="1"
            ),
            make_record(
                source_id="2"
            ),
            make_record(
                source_id="3"
            ),
        ],
        scope_token="scope-batch",
    )

    assert (
        buffer.has_scope(
            "scope-batch"
        )
        is True
    )

    batches = buffer.flush_scope(
        "scope-batch"
    )

    assert (
        sum(
            batch.row_count
            for batch
            in batches
        )
        == 3
    )


def test_old_add_api_still_works():

    """
    旧代码没有 scope_token 时必须保持兼容。
    """

    buffer = RecordBuffer(
        config=BufferConfig(
            min_rows=100,
            max_rows=1000,
            target_bytes=1_000_000,
            flush_seconds=30,
        )
    )

    record = make_record(
        source_id="1"
    )

    result = buffer.add(
        record
    )

    assert (
        result is None
    )

    assert (
        len(buffer)
        == 1
    )

    batches = buffer.flush_all()

    assert (
        len(batches)
        == 1
    )

    assert (
        batches[0].scope_tokens
        == frozenset()
    )