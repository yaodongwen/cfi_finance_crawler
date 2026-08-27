from datetime import datetime, timezone

import pytest

from crawl_framework.core.models import (
    CanonicalRecord,
)
from crawl_framework.storage.partition import (
    Partitioner,
    PartitionKey,
    bucket_name,
    safe_partition_value,
    stable_bucket,
)


def test_safe_partition_value():

    assert (
        safe_partition_value(
            "naver_finance"
        )
        == "naver_finance"
    )

    assert (
        "/" not in safe_partition_value(
            "../abc/test"
        )
    )


def test_stable_bucket():

    a = stable_bucket(
        "XKRX:005930"
    )

    b = stable_bucket(
        "XKRX:005930"
    )

    assert (
        a == b
    )

    assert (
        0 <= a < 256
    )


def test_bucket_name():

    assert (
        bucket_name(
            0
        )
        == "00"
    )

    assert (
        bucket_name(
            255
        )
        == "ff"
    )


def test_partition_key_path():

    key = PartitionKey(
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        partition_date=datetime(
            2026,
            8,
            24,
            tzinfo=timezone.utc,
        ).date(),
        bucket=63,
    )

    path = (
        key.relative_path()
        .as_posix()
    )

    assert (
        path
        == (
            "site=naver_finance/"
            "country=KR/"
            "dataset=forum_post/"
            "year=2026/"
            "month=08/"
            "day=24/"
            "bucket=3f"
        )
    )


def test_record_partition():

    record = CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        source_id="123456",
        scope_type="instrument",
        scope_id="XKRX:005930",
        instrument_id="XKRX:005930",
        event_time=datetime(
            2026,
            8,
            24,
            3,
            30,
            tzinfo=timezone.utc,
        ),
    )

    partitioner = Partitioner()

    key = partitioner.partition_for(
        record
    )

    assert (
        key.site_id
        == "naver_finance"
    )

    assert (
        key.country
        == "KR"
    )

    assert (
        key.dataset
        == "forum_post"
    )

    assert (
        key.partition_date.isoformat()
        == "2026-08-24"
    )


def test_same_instrument_same_bucket():

    partitioner = Partitioner()

    a = CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        source_id="1",
        scope_type="instrument",
        scope_id="XKRX:005930",
        instrument_id="XKRX:005930",
    )

    b = CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        source_id="2",
        scope_type="instrument",
        scope_id="XKRX:005930",
        instrument_id="XKRX:005930",
    )

    assert (
        partitioner.partition_for(
            a
        ).bucket
        == partitioner.partition_for(
            b
        ).bucket
    )


def test_missing_event_time_falls_back_to_crawled_at():

    record = CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        source_id="123",
        crawled_at=datetime(
            2026,
            8,
            24,
            10,
            0,
            tzinfo=timezone.utc,
        ),
    )

    partitioner = Partitioner()

    key = partitioner.partition_for(
        record
    )

    assert (
        key.partition_date.isoformat()
        == "2026-08-24"
    )


def test_holding_snapshot_requires_event_time():

    record = CanonicalRecord(
        site_id="demo",
        country="US",
        dataset="holding_snapshot",
        source_id="snapshot-1",
    )

    partitioner = Partitioner()

    with pytest.raises(
        ValueError
    ):
        partitioner.partition_for(
            record
        )


def test_custom_bucket_count():

    partitioner = Partitioner(
        bucket_count=16
    )

    record = CanonicalRecord(
        site_id="demo",
        country="KR",
        dataset="forum_post",
        source_id="1",
        instrument_id="XKRX:005930",
    )

    key = partitioner.partition_for(
        record
    )

    assert (
        0
        <= key.bucket
        < 16
    )