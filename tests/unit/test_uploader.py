from datetime import (
    datetime,
    timezone,
)
import subprocess

import pytest

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
    UploadError,
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


def test_rsync_ssh_multiplex_options_are_reused_by_commands(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    uploader = RsyncUploader(
        remote_host="example.com",
        remote_user="stock",
        remote_root="/data",
        ssh_port=2222,
        dry_run=True,
        ssh_multiplex=True,
        ssh_control_path=(
            "/tmp/crawl-fw-%r@%h:%p"
        ),
        ssh_control_persist="5m",
    )

    rsync_command = (
        uploader.build_rsync_command(
            info
        )
    )

    mkdir_command = (
        uploader.build_mkdir_command(
            info
        )
    )

    rsync_text = " ".join(
        rsync_command
    )

    mkdir_text = " ".join(
        mkdir_command
    )

    for text in (
        rsync_text,
        mkdir_text,
    ):

        assert (
            "ControlMaster=auto"
            in text
        )

        assert (
            "ControlPersist=5m"
            in text
        )

        assert (
            "ControlPath=/tmp/crawl-fw-%r@%h:%p"
            in text
        )


def test_rsync_ssh_multiplex_default_control_path_is_short():

    uploader = RsyncUploader(
        remote_host="example.com",
        remote_user="stock",
        remote_root="/data",
        ssh_multiplex=True,
    )

    options = uploader.ssh_options()

    assert (
        "ControlPath=/tmp/cfw-%C"
        in options
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


class FakeCompletedProcess:

    def __init__(
        self,
        *,
        returncode=0,
        stdout="",
        stderr="",
    ):

        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def test_rsync_mkdir_allows_locale_warning_when_command_succeeds(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    uploader = RsyncUploader(
        remote_host="server",
        remote_root="/data",
        verify_size=False,
        verify_sha256=False,
    )

    calls = []

    def fake_run(command):

        calls.append(
            command
        )

        if command[0] == "ssh":

            return FakeCompletedProcess(
                returncode=0,
                stderr=(
                    "bash: warning: setlocale: "
                    "LC_ALL: cannot change locale "
                    "(C.UTF-8)\n"
                ),
            )

        return FakeCompletedProcess(
            returncode=0,
            stdout="uploaded",
        )

    uploader._run = fake_run

    result = uploader.upload(
        info
    )

    assert (
        result.status
        == "uploaded"
    )

    assert (
        len(
            calls
        )
        == 2
    )


def test_rsync_mkdir_still_fails_on_nonzero_exit_with_locale_warning(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    uploader = RsyncUploader(
        remote_host="server",
        remote_root="/data",
    )

    def fake_run(command):

        del command

        return FakeCompletedProcess(
            returncode=1,
            stderr=(
                "bash: warning: setlocale: "
                "LC_ALL: cannot change locale "
                "(C.UTF-8)\npermission denied"
            ),
        )

    uploader._run = fake_run

    with pytest.raises(
        UploadError,
        match="permission denied",
    ):

        uploader.upload(
            info
        )


def test_rsync_remote_stat_ignores_locale_warning_on_success(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    uploader = RsyncUploader(
        remote_host="server",
        remote_root="/data",
        verify_size=True,
        verify_sha256=False,
    )

    def fake_run(command):

        if command[0] == "ssh" and "stat" in command:

            return FakeCompletedProcess(
                returncode=0,
                stdout=str(
                    info.file_size
                ),
                stderr=(
                    "bash: warning: setlocale: "
                    "LC_ALL: cannot change locale "
                    "(C.UTF-8)\n"
                ),
            )

        return FakeCompletedProcess(
            returncode=0,
        )

    uploader._run = fake_run

    result = uploader.upload(
        info
    )

    assert result.status == "verified"

    assert result.verified is True


def test_rsync_caches_remote_directory_mkdir(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    uploader = RsyncUploader(
        remote_host="server",
        remote_root="/data",
        verify_size=False,
        verify_sha256=False,
    )

    calls = []

    def fake_run(command):

        calls.append(
            command
        )

        return FakeCompletedProcess(
            returncode=0,
        )

    uploader._run = fake_run

    uploader.upload(
        info
    )
    uploader.upload(
        info
    )

    mkdir_calls = [
        command
        for command in calls
        if command[0] == "ssh"
        and "mkdir" in command
    ]

    assert len(mkdir_calls) == 1


def test_rsync_can_trust_success_without_remote_stat(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    uploader = RsyncUploader(
        remote_host="server",
        remote_root="/data",
        verify_size=False,
        verify_sha256=False,
        trust_rsync_success=True,
    )

    calls = []

    def fake_run(command):

        calls.append(
            command
        )

        return FakeCompletedProcess(
            returncode=0,
        )

    uploader._run = fake_run

    result = uploader.upload(
        info
    )

    assert result.status == "verified"
    assert result.verified is True
    assert result.remote_size == info.file_size
    assert not any(
        command[0] == "ssh"
        and "stat" in command
        for command in calls
    )


def test_rsync_retries_transient_command_failure(
    monkeypatch,
):

    uploader = RsyncUploader(
        remote_host="server",
        remote_root="/data",
        command_attempts=2,
        retry_sleep_seconds=0,
    )

    calls = []

    def fake_subprocess_run(command, **kwargs):

        calls.append(
            (
                command,
                kwargs,
            )
        )

        if len(calls) == 1:
            return subprocess.CompletedProcess(
                command,
                returncode=255,
                stderr="connection reset",
            )

        return subprocess.CompletedProcess(
            command,
            returncode=0,
        )

    monkeypatch.setattr(
        subprocess,
        "run",
        fake_subprocess_run,
    )

    result = uploader._run(
        [
            "ssh",
            "server",
            "true",
        ]
    )

    assert result.returncode == 0
    assert len(calls) == 2
    assert calls[0][1]["timeout"] == 120.0


def test_rsync_mkdir_failure_reports_unknown_error_when_stderr_is_empty(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    uploader = RsyncUploader(
        remote_host="server",
        remote_root="/data",
        command_attempts=1,
    )

    def fake_run(command):

        del command
        return FakeCompletedProcess(
            returncode=1,
            stderr="",
        )

    uploader._run = fake_run

    with pytest.raises(
        UploadError,
        match="unknown error",
    ):
        uploader.upload(
            info
        )


def test_rsync_failure_cleans_locale_warning_from_message(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    uploader = RsyncUploader(
        remote_host="server",
        remote_root="/data",
        verify_size=False,
        verify_sha256=False,
    )

    def fake_run(command):

        if command[0] == "ssh":

            return FakeCompletedProcess(
                returncode=0,
            )

        return FakeCompletedProcess(
            returncode=20,
            stderr=(
                "bash: warning: setlocale: "
                "LC_ALL: cannot change locale "
                "(C.UTF-8)\n"
                "rsync error"
            ),
        )

    uploader._run = fake_run

    with pytest.raises(
        UploadError,
        match="rsync error",
    ) as exc_info:

        uploader.upload(
            info
        )

    assert "setlocale" not in str(
        exc_info.value
    )
