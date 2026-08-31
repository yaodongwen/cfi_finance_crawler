from __future__ import annotations

from datetime import datetime
from pathlib import Path

from crawl_framework.storage.audit import (
    audit_catalog_files,
    audit_orphan_remote_files,
)
from crawl_framework.storage.parquet_writer import (
    file_sha256,
)
from crawl_framework.storage.postgres import (
    CatalogDataFile,
)


def make_file(
    *,
    file_id: int,
    file_path: str,
    remote_path: str | None,
    sha256: str = "sha",
    file_size: int = 1,
    storage_status: str = "uploaded",
    lifecycle_status: str = "active",
) -> CatalogDataFile:

    return CatalogDataFile(
        id=file_id,
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        partition_date="2026-08-26",
        bucket="20",
        file_path=file_path,
        sha256=sha256,
        row_count=1,
        file_size=file_size,
        min_event_time=None,
        max_event_time=None,
        schema_version=1,
        storage_status=storage_status,
        lifecycle_status=lifecycle_status,
        remote_path=remote_path,
    )


def test_audit_detects_active_uploaded_missing_remote_path():

    report = audit_catalog_files(
        [
            make_file(
                file_id=1,
                file_path="missing-path.parquet",
                remote_path=None,
            )
        ]
    )

    assert report.files_checked == 1
    assert report.catalog_errors == 1
    assert report.issues[0].code == "missing_remote_path"


def test_audit_detects_missing_remote_file(
    tmp_path,
):

    report = audit_catalog_files(
        [
            make_file(
                file_id=1,
                file_path="missing.parquet",
                remote_path=str(
                    tmp_path
                    /
                    "missing.parquet"
                ),
            )
        ]
    )

    assert report.missing_remote == 1
    assert report.issues[0].code == "remote_file_missing"


def test_audit_detects_size_and_checksum_mismatch(
    tmp_path,
):

    path = tmp_path / "file.parquet"

    path.write_text(
        "actual",
        encoding="utf-8",
    )

    report = audit_catalog_files(
        [
            make_file(
                file_id=1,
                file_path="file.parquet",
                remote_path=str(
                    path
                ),
                sha256="wrong",
                file_size=999,
            )
        ]
    )

    assert report.size_mismatch == 1
    assert report.checksum_mismatch == 1

    assert {
        issue.code
        for issue in report.issues
    } == {
        "file_size_mismatch",
        "checksum_mismatch",
    }


def test_audit_accepts_matching_uploaded_file(
    tmp_path,
):

    path = tmp_path / "ok.parquet"

    path.write_bytes(
        b"ok"
    )

    report = audit_catalog_files(
        [
            make_file(
                file_id=1,
                file_path="ok.parquet",
                remote_path=str(
                    path
                ),
                sha256=file_sha256(
                    path
                ),
                file_size=path.stat().st_size,
            )
        ]
    )

    assert report.files_checked == 1
    assert report.issues == []
    assert report.to_dict()["files_checked"] == 1


def test_audit_detects_duplicate_conflicting_metadata():

    report = audit_catalog_files(
        [
            make_file(
                file_id=1,
                file_path="same.parquet",
                remote_path=None,
                sha256="a",
            ),
            make_file(
                file_id=2,
                file_path="same.parquet",
                remote_path=None,
                sha256="b",
            ),
        ],
        verify_checksum=False,
    )

    assert report.catalog_errors == 3

    codes = [
        issue.code
        for issue in report.issues
    ]

    assert codes.count(
        "missing_remote_path"
    ) == 2

    assert codes.count(
        "duplicate_conflicting_metadata"
    ) == 1


def test_audit_detects_lifecycle_inconsistency():

    report = audit_catalog_files(
        [
            make_file(
                file_id=1,
                file_path="local-active.parquet",
                remote_path=None,
                storage_status="local",
                lifecycle_status="active",
            )
        ]
    )

    assert report.lifecycle_errors == 1
    assert report.issues[0].code == "lifecycle_inconsistent"


def test_audit_orphan_remote_files(
    tmp_path,
):

    known = (
        tmp_path
        /
        "site=x"
        /
        "known.parquet"
    )

    orphan = (
        tmp_path
        /
        "site=x"
        /
        "orphan.parquet"
    )

    known.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    known.write_bytes(
        b"known"
    )

    orphan.write_bytes(
        b"orphan"
    )

    report = audit_orphan_remote_files(
        remote_root=tmp_path,
        catalog_file_paths=[
            "site=x/known.parquet",
        ],
    )

    assert report.orphans == 1
    assert report.issues[0].file_path == "site=x/orphan.parquet"
