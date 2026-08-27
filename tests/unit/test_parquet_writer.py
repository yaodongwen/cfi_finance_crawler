from datetime import (
    datetime,
    timezone,
)

import pyarrow.parquet as pq
import pyarrow as pa
import pytest

from crawl_framework.core.models import (
    CanonicalRecord,
)
from crawl_framework.storage.buffer import (
    FlushBatch,
)
from crawl_framework.storage.parquet_writer import (
    ParquetWriter,
    file_sha256,
    records_to_arrow_table,
)
from crawl_framework.storage.partition import (
    Partitioner,
)


def make_record(
    *,
    source_id: str,
    title: str = "hello",
    event_time: datetime | None = None,
    schema_version: int = 1,
) -> CanonicalRecord:

    if event_time is None:

        event_time = datetime(
            2026,
            8,
            24,
            1,
            0,
            tzinfo=timezone.utc,
        )

    return CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        source_id=source_id,
        scope_type="instrument",
        scope_id="XKRX:005930",
        instrument_id="XKRX:005930",
        event_time=event_time,
        title=title,
        schema_version=schema_version,
    )


def make_batch(
    records,
):

    records = tuple(
        records
    )

    partitioner = (
        Partitioner()
    )

    key = partitioner.partition_for(
        records[0]
    )

    return FlushBatch(
        key=key,
        records=records,
        estimated_bytes=1000,
    )


def test_records_to_arrow_table():

    records = [
        make_record(
            source_id="1"
        ),
        make_record(
            source_id="2"
        ),
    ]

    table = (
        records_to_arrow_table(
            records
        )
    )

    assert (
        table.num_rows
        == 2
    )

    assert (
        "record_uid"
        in table.column_names
    )

    assert (
        "version_hash"
        in table.column_names
    )


def test_empty_records_fail():

    with pytest.raises(
        ValueError
    ):
        records_to_arrow_table(
            []
        )


def test_write_parquet(
    tmp_path,
):

    writer = ParquetWriter(
        tmp_path
        / "warehouse"
    )

    batch = make_batch(
        [
            make_record(
                source_id="1",
                title="first",
            ),
            make_record(
                source_id="2",
                title="second",
            ),
        ]
    )

    info = writer.write_batch(
        batch
    )

    assert (
        info.file_path.exists()
    )

    assert (
        info.file_path.suffix
        == ".parquet"
    )

    assert (
        info.row_count
        == 2
    )

    assert (
        info.file_size
        > 0
    )

    assert (
        len(info.sha256)
        == 64
    )


def test_parquet_can_be_read(
    tmp_path,
):

    writer = ParquetWriter(
        tmp_path
        / "warehouse"
    )

    batch = make_batch(
        [
            make_record(
                source_id="1",
                title="测试新闻",
            ),
            make_record(
                source_id="2",
                title="第二条",
            ),
        ]
    )

    info = writer.write_batch(
        batch
    )

    table = pq.read_table(
        info.file_path
    )

    assert (
        table.num_rows
        == 2
    )

    titles = (
        table[
            "title"
        ]
        .to_pylist()
    )

    assert (
        "测试新闻"
        in titles
    )


def test_relative_partition_path(
    tmp_path,
):

    writer = ParquetWriter(
        tmp_path
        / "warehouse"
    )

    batch = make_batch(
        [
            make_record(
                source_id="1"
            )
        ]
    )

    info = writer.write_batch(
        batch
    )

    path = (
        info.relative_path
        .as_posix()
    )

    assert (
        "site=naver_finance"
        in path
    )

    assert (
        "country=KR"
        in path
    )

    assert (
        "dataset=forum_post"
        in path
    )

    assert (
        "year=2026"
        in path
    )

    assert (
        "month=08"
        in path
    )

    assert (
        "day=24"
        in path
    )


def test_sha256_matches(
    tmp_path,
):

    writer = ParquetWriter(
        tmp_path
        / "warehouse"
    )

    info = writer.write_batch(
        make_batch(
            [
                make_record(
                    source_id="1"
                )
            ]
        )
    )

    assert (
        info.sha256
        == file_sha256(
            info.file_path
        )
    )


def test_event_time_range(
    tmp_path,
):

    writer = ParquetWriter(
        tmp_path
        / "warehouse"
    )

    early = datetime(
        2026,
        8,
        24,
        1,
        0,
        tzinfo=timezone.utc,
    )

    late = datetime(
        2026,
        8,
        24,
        5,
        30,
        tzinfo=timezone.utc,
    )

    info = writer.write_batch(
        make_batch(
            [
                make_record(
                    source_id="1",
                    event_time=late,
                ),
                make_record(
                    source_id="2",
                    event_time=early,
                ),
            ]
        )
    )

    assert (
        info.min_event_time
        == early
    )

    assert (
        info.max_event_time
        == late
    )


def test_unique_filename(
    tmp_path,
):

    writer = ParquetWriter(
        tmp_path
        / "warehouse"
    )

    first = writer.write_batch(
        make_batch(
            [
                make_record(
                    source_id="1"
                )
            ]
        )
    )

    second = writer.write_batch(
        make_batch(
            [
                make_record(
                    source_id="2"
                )
            ]
        )
    )

    assert (
        first.file_path
        != second.file_path
    )


def test_write_batches(
    tmp_path,
):

    writer = ParquetWriter(
        tmp_path
        / "warehouse"
    )

    batches = [
        make_batch(
            [
                make_record(
                    source_id="1"
                )
            ]
        ),
        make_batch(
            [
                make_record(
                    source_id="2"
                )
            ]
        ),
    ]

    results = writer.write_batches(
        batches
    )

    assert (
        len(results)
        == 2
    )

    assert all(
        result.file_path.exists()
        for result in results
    )


def test_schema_version(
    tmp_path,
):

    writer = ParquetWriter(
        tmp_path
        / "warehouse"
    )

    batch = make_batch(
        [
            make_record(
                source_id="1",
                schema_version=1,
            ),
            make_record(
                source_id="2",
                schema_version=1,
            ),
        ]
    )

    info = writer.write_batch(
        batch
    )

    assert (
        info.schema_version
        == 1
    )

def test_arrow_schema_is_fixed():

    record = CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        source_id="123",
    )

    table = records_to_arrow_table(
        [
            record
        ]
    )

    assert (
        table.schema.field(
            "instrument_id"
        ).type
        == pa.string()
    )

    assert (
        table.schema.field(
            "scope_id"
        ).type
        == pa.string()
    )


def test_event_time_is_timestamp():

    record = CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        source_id="123",
    )

    table = records_to_arrow_table(
        [
            record
        ]
    )

    field = table.schema.field(
        "event_time"
    )

    assert (
        pa.types.is_timestamp(
            field.type
        )
    )

    assert (
        field.type.tz
        == "UTC"
    )


def test_crawled_at_is_timestamp():

    record = CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        source_id="123",
    )

    table = records_to_arrow_table(
        [
            record
        ]
    )

    field = table.schema.field(
        "crawled_at"
    )

    assert (
        pa.types.is_timestamp(
            field.type
        )
    )

    assert (
        field.type.tz
        == "UTC"
    )


def test_payload_json_is_large_string():

    record = CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        source_id="123",
        payload={
            "views": 100,
            "likes": 20,
        },
    )

    table = records_to_arrow_table(
        [
            record
        ]
    )

    field = table.schema.field(
        "payload_json"
    )

    assert (
        pa.types.is_large_string(
            field.type
        )
    )

    values = (
        table[
            "payload_json"
        ]
        .to_pylist()
    )

    assert (
        '"views":100'
        in values[0]
    )


def test_multiple_datasets_fail():

    records = [
        CanonicalRecord(
            site_id="demo",
            country="KR",
            dataset="forum_post",
            source_id="1",
        ),

        CanonicalRecord(
            site_id="demo",
            country="KR",
            dataset="news_article",
            source_id="2",
        ),
    ]

    with pytest.raises(
        ValueError
    ):
        records_to_arrow_table(
            records
        )