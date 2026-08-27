from datetime import (
    datetime,
    timezone,
)

import pytest

from crawl_framework.core.models import (
    CanonicalRecord,
)
from crawl_framework.storage.record_index import (
    RecordIndexEntry,
    RecordIndexReader,
    RecordIndexStore,
    RecordIndexWriter,
    default_index_path,
    record_to_index_entry,
    records_to_index_entries,
)


def make_record(
    source_id: str,
    *,
    title: str | None = None,
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
        title=(
            title
            or f"title-{source_id}"
        ),
    )


def test_record_index_entry_validation():

    with pytest.raises(
        ValueError
    ):

        RecordIndexEntry(
            record_uid="",
            version_hash="abc",
        )

    with pytest.raises(
        ValueError
    ):

        RecordIndexEntry(
            record_uid="abc",
            version_hash="",
        )


def test_record_to_index_entry():

    record = make_record(
        "1"
    )

    entry = record_to_index_entry(
        record
    )

    assert (
        entry.record_uid
        == record.record_uid
    )

    assert (
        entry.version_hash
        == record.version_hash
    )


def test_records_to_index_entries():

    records = [
        make_record(
            "1"
        ),
        make_record(
            "2"
        ),
        make_record(
            "3"
        ),
    ]

    entries = (
        records_to_index_entries(
            records
        )
    )

    assert (
        len(entries)
        == 3
    )

    assert (
        entries[0].record_uid
        == records[0].record_uid
    )


def test_default_index_path(
    tmp_path,
):

    parquet = (
        tmp_path
        / "part-abc.parquet"
    )

    path = default_index_path(
        parquet
    )

    assert (
        path
        == (
            tmp_path
            / "part-abc.records.jsonl"
        )
    )


def test_writer_creates_file(
    tmp_path,
):

    parquet = (
        tmp_path
        / "part-test.parquet"
    )

    writer = (
        RecordIndexWriter()
    )

    records = [
        make_record(
            "1"
        ),
        make_record(
            "2"
        ),
    ]

    info = writer.write(
        parquet,
        records,
    )

    assert (
        info.file_path.exists()
    )

    assert (
        info.row_count
        == 2
    )

    assert (
        info.file_size
        > 0
    )


def test_writer_rejects_empty(
    tmp_path,
):

    writer = (
        RecordIndexWriter()
    )

    with pytest.raises(
        ValueError
    ):

        writer.write(
            tmp_path
            / "part.parquet",
            [],
        )


def test_reader_round_trip(
    tmp_path,
):

    parquet = (
        tmp_path
        / "part-test.parquet"
    )

    records = [
        make_record(
            "1"
        ),
        make_record(
            "2"
        ),
    ]

    writer = (
        RecordIndexWriter()
    )

    info = writer.write(
        parquet,
        records,
    )

    reader = (
        RecordIndexReader()
    )

    entries = reader.read(
        info.file_path
    )

    assert (
        len(entries)
        == 2
    )

    assert (
        entries[0].record_uid
        == records[0].record_uid
    )

    assert (
        entries[1].version_hash
        == records[1].version_hash
    )


def test_reader_pairs(
    tmp_path,
):

    parquet = (
        tmp_path
        / "part-test.parquet"
    )

    records = [
        make_record(
            "1"
        ),
        make_record(
            "2"
        ),
    ]

    writer = (
        RecordIndexWriter()
    )

    info = writer.write(
        parquet,
        records,
    )

    reader = (
        RecordIndexReader()
    )

    pairs = reader.read_pairs(
        info.file_path
    )

    assert pairs == [
        (
            records[0].record_uid,
            records[0].version_hash,
        ),
        (
            records[1].record_uid,
            records[1].version_hash,
        ),
    ]


def test_store_facade(
    tmp_path,
):

    parquet = (
        tmp_path
        / "part-test.parquet"
    )

    records = [
        make_record(
            "1"
        ),
        make_record(
            "2"
        ),
    ]

    store = (
        RecordIndexStore()
    )

    info = (
        store.write_for_parquet(
            parquet,
            records,
        )
    )

    assert (
        store.exists_for_parquet(
            parquet
        )
        is True
    )

    pairs = (
        store.read_pairs_for_parquet(
            parquet
        )
    )

    assert (
        len(pairs)
        == 2
    )

    assert (
        info.row_count
        == 2
    )


def test_delete_sidecar(
    tmp_path,
):

    parquet = (
        tmp_path
        / "part-test.parquet"
    )

    store = (
        RecordIndexStore()
    )

    store.write_for_parquet(
        parquet,
        [
            make_record(
                "1"
            )
        ],
    )

    assert (
        store.delete_for_parquet(
            parquet
        )
        is True
    )

    assert (
        store.exists_for_parquet(
            parquet
        )
        is False
    )

    assert (
        store.delete_for_parquet(
            parquet
        )
        is False
    )


def test_changed_version_hash_is_preserved(
    tmp_path,
):

    old = make_record(
        "1",
        title="old",
    )

    new = make_record(
        "1",
        title="new",
    )

    assert (
        old.record_uid
        == new.record_uid
    )

    assert (
        old.version_hash
        != new.version_hash
    )

    store = (
        RecordIndexStore()
    )

    parquet = (
        tmp_path
        / "part-test.parquet"
    )

    store.write_for_parquet(
        parquet,
        [
            new
        ],
    )

    pairs = (
        store.read_pairs_for_parquet(
            parquet
        )
    )

    assert pairs == [
        (
            new.record_uid,
            new.version_hash,
        )
    ]