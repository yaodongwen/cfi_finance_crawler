from crawl_framework.core.models import (
    CanonicalRecord,
)
from crawl_framework.storage.seen_store import (
    SQLiteSeenStore,
)


def make_record(
    *,
    title: str,
) -> CanonicalRecord:

    return CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        source_id="123456",
        scope_type="instrument",
        scope_id="XKRX:005930",
        instrument_id="XKRX:005930",
        title=title,
    )


def test_new_record(
    tmp_path,
):

    db = (
        tmp_path
        / "seen.sqlite3"
    )

    with SQLiteSeenStore(
        db
    ) as store:

        record = make_record(
            title="hello"
        )

        result = store.inspect(
            record.record_uid,
            record.version_hash,
        )

        assert (
            result.decision
            == "new"
        )

        assert (
            store.count()
            == 0
        )


def test_commit_and_unchanged(
    tmp_path,
):

    db = (
        tmp_path
        / "seen.sqlite3"
    )

    with SQLiteSeenStore(
        db
    ) as store:

        record = make_record(
            title="hello"
        )

        store.commit(
            record.record_uid,
            record.version_hash,
        )

        result = store.inspect(
            record.record_uid,
            record.version_hash,
        )

        assert (
            result.decision
            == "unchanged"
        )

        assert store.contains(
            record.record_uid
        )

        assert (
            store.count()
            == 1
        )


def test_updated_record(
    tmp_path,
):

    db = (
        tmp_path
        / "seen.sqlite3"
    )

    with SQLiteSeenStore(
        db
    ) as store:

        old = make_record(
            title="old"
        )

        new = make_record(
            title="new"
        )

        assert (
            old.record_uid
            == new.record_uid
        )

        assert (
            old.version_hash
            != new.version_hash
        )

        store.commit(
            old.record_uid,
            old.version_hash,
        )

        result = store.inspect(
            new.record_uid,
            new.version_hash,
        )

        assert (
            result.decision
            == "updated"
        )


def test_update_commit_changes_version(
    tmp_path,
):

    db = (
        tmp_path
        / "seen.sqlite3"
    )

    with SQLiteSeenStore(
        db
    ) as store:

        old = make_record(
            title="old"
        )

        new = make_record(
            title="new"
        )

        store.commit(
            old.record_uid,
            old.version_hash,
        )

        assert (
            store.inspect(
                new.record_uid,
                new.version_hash,
            ).decision
            == "updated"
        )

        store.commit(
            new.record_uid,
            new.version_hash,
        )

        assert (
            store.inspect(
                new.record_uid,
                new.version_hash,
            ).decision
            == "unchanged"
        )


def test_commit_many(
    tmp_path,
):

    db = (
        tmp_path
        / "seen.sqlite3"
    )

    with SQLiteSeenStore(
        db
    ) as store:

        records = [
            CanonicalRecord(
                site_id="demo",
                country="KR",
                dataset="news_article",
                source_id=str(
                    index
                ),
                title=(
                    f"title-{index}"
                ),
            )
            for index
            in range(
                100
            )
        ]

        count = store.commit_many(
            (
                (
                    record.record_uid,
                    record.version_hash,
                )
                for record
                in records
            )
        )

        assert (
            count
            == 100
        )

        assert (
            store.count()
            == 100
        )


def test_same_source_id_different_sites(
    tmp_path,
):

    db = (
        tmp_path
        / "seen.sqlite3"
    )

    with SQLiteSeenStore(
        db
    ) as store:

        naver = CanonicalRecord(
            site_id="naver_finance",
            country="KR",
            dataset="forum_post",
            source_id="123",
        )

        hotcopper = CanonicalRecord(
            site_id="hotcopper",
            country="AU",
            dataset="forum_post",
            source_id="123",
        )

        assert (
            naver.record_uid
            != hotcopper.record_uid
        )

        store.commit_many(
            [
                (
                    naver.record_uid,
                    naver.version_hash,
                ),
                (
                    hotcopper.record_uid,
                    hotcopper.version_hash,
                ),
            ]
        )

        assert (
            store.count()
            == 2
        )