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
from crawl_framework.storage.parquet_writer import (
    ParquetWriter,
)
from crawl_framework.storage.partition import (
    Partitioner,
)
from crawl_framework.storage.uploader import (
    LocalUploader,
    RsyncUploader,
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
        content="test content",
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


def test_local_upload(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    remote_root = (
        tmp_path
        / "remote"
    )

    uploader = LocalUploader(
        remote_root,
        verify_size=True,
        verify_sha256=True,
    )

    result = uploader.upload(
        info
    )

    assert (
        result.status
        == "verified"
    )

    assert (
        result.verified
        is True
    )

    remote_file = (
        remote_root
        / info.relative_path
    )

    assert (
        remote_file.exists()
    )

    assert (
        remote_file.stat().st_size
        == info.file_size
    )

    assert (
        result.remote_sha256
        == info.sha256
    )


def test_local_upload_keeps_source(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    uploader = LocalUploader(
        tmp_path
        / "remote"
    )

    uploader.upload(
        info
    )

    assert (
        info.file_path.exists()
    )


def test_local_dry_run(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    remote_root = (
        tmp_path
        / "remote"
    )

    uploader = LocalUploader(
        remote_root,
        dry_run=True,
    )

    result = uploader.upload(
        info
    )

    assert (
        result.status
        == "dry_run"
    )

    remote_file = (
        remote_root
        / info.relative_path
    )

    assert not (
        remote_file.exists()
    )


def test_local_remote_path_matches_relative_path(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    remote_root = (
        tmp_path
        / "server"
        / "stocklake"
        / "v2"
    )

    uploader = LocalUploader(
        remote_root
    )

    result = uploader.upload(
        info
    )

    expected = (
        remote_root
        / info.relative_path
    )

    assert (
        result.remote_path
        == expected.as_posix()
    )


def test_rsync_remote_file_path(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    uploader = RsyncUploader(
        remote_host="example.com",
        remote_user="stock",
        remote_root=(
            "/data/stocklake/v2"
        ),
        dry_run=True,
    )

    remote = (
        uploader.remote_file_path(
            info
        )
    )

    assert remote.startswith(
        "/data/stocklake/v2/"
    )

    assert remote.endswith(
        info.relative_path.as_posix()
    )


def test_rsync_target():

    uploader = RsyncUploader(
        remote_host="example.com",
        remote_user="stock",
        remote_root="/data",
        dry_run=True,
    )

    assert (
        uploader.ssh_target
        == "stock@example.com"
    )


def test_rsync_without_user():

    uploader = RsyncUploader(
        remote_host="example.com",
        remote_root="/data",
        dry_run=True,
    )

    assert (
        uploader.ssh_target
        == "example.com"
    )


def test_rsync_command(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    uploader = RsyncUploader(
        remote_host="example.com",
        remote_user="stock",
        remote_root=(
            "/data/stocklake/v2"
        ),
        ssh_port=2222,
        dry_run=True,
    )

    command = (
        uploader.build_rsync_command(
            info
        )
    )

    assert (
        command[0]
        == "rsync"
    )

    assert (
        "--dry-run"
        in command
    )

    assert (
        "-e"
        in command
    )

    text = " ".join(
        command
    )

    assert (
        "ssh -p 2222"
        in text
    )

    assert (
        "stock@example.com:"
        in text
    )


def test_rsync_dry_run_does_not_execute(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    uploader = RsyncUploader(
        remote_host="example.com",
        remote_user="stock",
        remote_root="/data",
        dry_run=True,
    )

    result = uploader.upload(
        info
    )

    assert (
        result.status
        == "dry_run"
    )

    assert (
        "rsync"
        in (
            result.message
            or ""
        )
    )


def test_rsync_directory(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    uploader = RsyncUploader(
        remote_host="server",
        remote_root=(
            "/mnt/data/stocklake/v2"
        ),
        dry_run=True,
    )

    remote_dir = (
        uploader.remote_directory(
            info
        )
    )

    assert (
        "site=naver_finance"
        in remote_dir
    )

    assert (
        "dataset=forum_post"
        in remote_dir
    )

    assert not remote_dir.endswith(
        ".parquet"
    )