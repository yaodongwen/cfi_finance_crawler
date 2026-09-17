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


def test_financial_reports_coalesce_by_year_across_instruments():
    partitioner = Partitioner()
    first = CanonicalRecord(
        site_id="hkexnews",
        country="HK",
        dataset="financial_report",
        source_id="a",
        instrument_id="XHKG:00005",
        event_time=datetime(2026, 3, 26, tzinfo=timezone.utc),
    )
    second = CanonicalRecord(
        site_id="hkexnews",
        country="HK",
        dataset="financial_report",
        source_id="b",
        instrument_id="XHKG:00700",
        event_time=datetime(2026, 9, 1, tzinfo=timezone.utc),
    )

    first_key = partitioner.partition_for(first)
    second_key = partitioner.partition_for(second)

    assert first_key == second_key
    assert first_key.partition_date.isoformat() == "2026-01-01"
    assert first_key.bucket_count == 1
    assert first_key.bucket == 0


def test_existing_daily_dataset_partitioning_is_unchanged():
    record = CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        source_id="post",
        instrument_id="XKRX:005930",
        event_time=datetime(2026, 9, 15, tzinfo=timezone.utc),
    )

    key = Partitioner().partition_for(record)

    assert key.partition_date.isoformat() == "2026-09-15"
    assert key.bucket_count == 256
