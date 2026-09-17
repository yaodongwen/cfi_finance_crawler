from datetime import datetime, timezone

from crawl_framework.core.models import (
    CanonicalRecord,
    InstrumentRef,
    RecordRelation,
)


def test_instrument_ref():

    instrument = InstrumentRef(
        instrument_id="XKRX:005930",
        source_symbol="005930",
        name="Samsung Electronics",
        country="KR",
    )

    assert (
        instrument.instrument_id
        == "XKRX:005930"
    )


def test_record_uid_is_stable():

    record1 = CanonicalRecord(
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
            1,
            0,
            tzinfo=timezone.utc,
        ),
        title="test",
        content="hello",
    )

    record2 = CanonicalRecord(
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
            1,
            0,
            tzinfo=timezone.utc,
        ),
        title="test changed",
        content="new content",
    )

    # 内容变化不改变 record_uid
    assert (
        record1.record_uid
        == record2.record_uid
    )

    # 但版本 hash 应变化
    assert (
        record1.version_hash
        != record2.version_hash
    )


def test_record_identity_scope_can_differ_from_operational_scope():
    first = CanonicalRecord(
        site_id="example",
        country="JP",
        dataset="news_article",
        source_id="n123456789012",
        scope_type="month",
        scope_id="2026-08",
        identity_scope_type="global",
        title="same",
    )
    second = CanonicalRecord(
        site_id="example",
        country="JP",
        dataset="news_article",
        source_id="n123456789012",
        scope_type="month",
        scope_id="2026-09",
        identity_scope_type="global",
        title="same",
    )

    assert first.record_uid == second.record_uid
    assert first.version_hash == second.version_hash
    assert first.scope_id != second.scope_id


def test_different_sites_do_not_conflict():

    naver = CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        source_id="123456",
        scope_type="instrument",
        scope_id="XKRX:005930",
    )

    hotcopper = CanonicalRecord(
        site_id="hotcopper",
        country="AU",
        dataset="forum_post",
        source_id="123456",
        scope_type="instrument",
        scope_id="XASX:BHP",
    )

    assert (
        naver.record_uid
        != hotcopper.record_uid
    )


def test_relations():

    record = CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="news_article",
        source_id="news001",
        relations=[
            RecordRelation(
                instrument_id=(
                    "XKRX:005930"
                ),
                relation_type="mentions",
            ),
            RecordRelation(
                instrument_id=(
                    "XKRX:000660"
                ),
                relation_type="mentions",
            ),
        ],
    )

    data = record.to_dict()

    assert (
        len(data["relations"])
        == 2
    )


def test_version_hash_ignores_crawl_time():

    a = CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="news_article",
        source_id="abc",
        title="hello",
        crawled_at=datetime(
            2026,
            8,
            24,
            1,
            0,
            tzinfo=timezone.utc,
        ),
    )

    b = CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="news_article",
        source_id="abc",
        title="hello",
        crawled_at=datetime(
            2026,
            8,
            25,
            1,
            0,
            tzinfo=timezone.utc,
        ),
    )

    assert (
        a.record_uid
        == b.record_uid
    )

    assert (
        a.version_hash
        == b.version_hash
    )
