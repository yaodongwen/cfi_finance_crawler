from __future__ import annotations

import hashlib
import os
import shlex
import shutil
import subprocess
import time

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from crawl_framework.storage.parquet_writer import (
    ParquetFileInfo,
    file_sha256,
)


UploadStatus = Literal[
    "uploaded",
    "verified",
    "dry_run",
    "failed",
]


@dataclass(
    frozen=True,
    slots=True,
)
class UploadResult:
    """
    一次上传操作的结果。

    local_path:
        本地文件

    remote_path:
        最终远端文件路径

    status:
        uploaded
        verified
        dry_run
        failed
    """

    local_path: Path

    remote_path: str

    status: UploadStatus

    local_size: int

    remote_size: int | None = None

    local_sha256: str | None = None

    remote_sha256: str | None = None

    message: str | None = None


    @property
    def verified(
        self,
    ) -> bool:

        return (
            self.status
            == "verified"
        )


class UploadError(
    RuntimeError
):
    pass


class BaseUploader:
    """
    Uploader 抽象接口。

    ParquetWriter 只负责本地文件。

    Uploader 负责：

        本地文件
            ↓
        远端存储
            ↓
        验证

    Uploader 不负责：

        删除本地文件
        SeenStore commit
        PostgreSQL commit
    """

    def upload(
        self,
        info: ParquetFileInfo,
    ) -> UploadResult:
        raise NotImplementedError


def _validate_local_file(
    info: ParquetFileInfo,
) -> None:
    """
    上传前检查本地文件。
    """

    path = info.file_path

    if not path.exists():
        raise UploadError(
            f"local file does not exist: {path}"
        )

    if not path.is_file():
        raise UploadError(
            f"local path is not file: {path}"
        )

    actual_size = (
        path.stat().st_size
    )

    if actual_size != info.file_size:
        raise UploadError(
            "local file size changed: "
            f"expected={info.file_size}, "
            f"actual={actual_size}, "
            f"path={path}"
        )


def _sha256_stream(
    path: Path,
    *,
    chunk_size: int = 1024 * 1024,
) -> str:
    """
    本地通用 SHA256。
    """

    digest = hashlib.sha256()

    with path.open(
        "rb"
    ) as file:

        while True:

            chunk = file.read(
                chunk_size
            )

            if not chunk:
                break

            digest.update(
                chunk
            )

    return digest.hexdigest()


class LocalUploader(
    BaseUploader
):
    """
    本地目录模拟远端服务器。

    例如：

        warehouse/
            ↓
        /tmp/fake_remote/

    用于：

        unit test
        integration test
        本机验证 pipeline

    复制过程：

        source
            ↓
        .tmp
            ↓
        fsync
            ↓
        os.replace
            ↓
        verify
    """

    def __init__(
        self,
        remote_root: str | Path,
        *,
        dry_run: bool = False,
        verify_size: bool = True,
        verify_sha256: bool = True,
    ) -> None:

        self.remote_root = Path(
            remote_root
        )

        self.dry_run = bool(
            dry_run
        )

        self.verify_size = bool(
            verify_size
        )

        self.verify_sha256 = bool(
            verify_sha256
        )

        if not self.dry_run:

            self.remote_root.mkdir(
                parents=True,
                exist_ok=True,
            )


    def upload(
        self,
        info: ParquetFileInfo,
    ) -> UploadResult:

        _validate_local_file(
            info
        )

        local_path = (
            info.file_path
        )

        remote_path = (
            self.remote_root
            / info.relative_path
        )

        local_size = (
            local_path.stat().st_size
        )

        if self.dry_run:

            return UploadResult(
                local_path=local_path,
                remote_path=(
                    remote_path.as_posix()
                ),
                status="dry_run",
                local_size=local_size,
                local_sha256=info.sha256,
                message="dry-run: no file copied",
            )

        remote_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        tmp_path = (
            remote_path.with_name(
                remote_path.name
                + ".tmp"
            )
        )

        try:

            with local_path.open(
                "rb"
            ) as source:

                with tmp_path.open(
                    "wb"
                ) as target:

                    shutil.copyfileobj(
                        source,
                        target,
                        length=(
                            1024
                            * 1024
                        ),
                    )

                    target.flush()

                    os.fsync(
                        target.fileno()
                    )

            os.replace(
                tmp_path,
                remote_path,
            )

        finally:

            if tmp_path.exists():

                try:
                    tmp_path.unlink()

                except OSError:
                    pass

        remote_size = (
            remote_path.stat().st_size
        )

        remote_sha256 = None

        if self.verify_size:

            if (
                remote_size
                != local_size
            ):
                raise UploadError(
                    "remote size mismatch: "
                    f"local={local_size}, "
                    f"remote={remote_size}"
                )

        if self.verify_sha256:

            remote_sha256 = (
                _sha256_stream(
                    remote_path
                )
            )

            if (
                remote_sha256
                != info.sha256
            ):
                raise UploadError(
                    "remote sha256 mismatch: "
                    f"local={info.sha256}, "
                    f"remote={remote_sha256}"
                )

        status: UploadStatus

        if (
            self.verify_size
            or self.verify_sha256
        ):
            status = "verified"

        else:
            status = "uploaded"

        return UploadResult(
            local_path=local_path,
            remote_path=(
                remote_path.as_posix()
            ),
            status=status,
            local_size=local_size,
            remote_size=remote_size,
            local_sha256=info.sha256,
            remote_sha256=remote_sha256,
        )


class RsyncUploader(
    BaseUploader
):
    """
    rsync 上传器。

    示例 remote：

        user@server:/mnt/nas/.../stocklake/v2

    最终命令类似：

        rsync -a --partial
        local.parquet
        user@server:/.../bucket/

    注意：

    这里不会使用 shell=True，
    避免 shell injection。

    remote_root 必须是远端根目录，
    不包含具体 parquet 文件名。
    """

    def __init__(
        self,
        *,
        remote_host: str,
        remote_root: str,
        remote_user: str | None = None,
        ssh_port: int = 22,
        dry_run: bool = False,
        verify_size: bool = True,
        verify_sha256: bool = False,
        trust_rsync_success: bool = False,
        cache_remote_directories: bool = True,
        ssh_multiplex: bool = False,
        ssh_control_path: str | None = None,
        ssh_control_persist: str = "10m",
        command_timeout_seconds: float = 120.0,
        command_attempts: int = 2,
        retry_sleep_seconds: float = 1.0,
        rsync_binary: str = "rsync",
        ssh_binary: str = "ssh",
    ) -> None:

        self.remote_host = str(
            remote_host
        ).strip()

        self.remote_root = str(
            remote_root
        ).rstrip(
            "/"
        )

        self.remote_user = (
            str(
                remote_user
            ).strip()
            if remote_user
            else None
        )

        self.ssh_port = int(
            ssh_port
        )

        self.dry_run = bool(
            dry_run
        )

        self.verify_size = bool(
            verify_size
        )

        self.verify_sha256 = bool(
            verify_sha256
        )

        self.trust_rsync_success = bool(
            trust_rsync_success
        )

        self.cache_remote_directories = bool(
            cache_remote_directories
        )

        self._ensured_remote_directories: set[str] = set()

        self.ssh_multiplex = bool(
            ssh_multiplex
        )

        self.ssh_control_path = (
            str(
                ssh_control_path
            ).strip()
            if ssh_control_path
            else None
        )

        self.ssh_control_persist = str(
            ssh_control_persist
        ).strip()

        self.command_timeout_seconds = float(
            command_timeout_seconds
        )

        self.command_attempts = max(
            1,
            int(
                command_attempts
            ),
        )

        self.retry_sleep_seconds = max(
            0.0,
            float(
                retry_sleep_seconds
            ),
        )

        self.rsync_binary = (
            rsync_binary
        )

        self.ssh_binary = (
            ssh_binary
        )

        if not self.remote_host:
            raise ValueError(
                "remote_host cannot be empty"
            )

        if not self.remote_root:
            raise ValueError(
                "remote_root cannot be empty"
            )

        if not (
            1
            <= self.ssh_port
            <= 65535
        ):
            raise ValueError(
                "invalid ssh_port"
            )

        if (
            self.ssh_multiplex
            and not self.ssh_control_persist
        ):
            raise ValueError(
                "ssh_control_persist cannot be empty"
            )


    @property
    def ssh_target(
        self,
    ) -> str:

        if self.remote_user:

            return (
                f"{self.remote_user}"
                f"@{self.remote_host}"
            )

        return self.remote_host

    def ssh_options(
        self,
    ) -> list[str]:

        options = [
            "-p",
            str(
                self.ssh_port
            ),
        ]

        if not self.ssh_multiplex:
            return options

        control_path = (
            self.ssh_control_path
            or (
                "/tmp/cfw-%C"
            )
        )

        options.extend(
            [
                "-o",
                "ControlMaster=auto",
                "-o",
                f"ControlPersist={self.ssh_control_persist}",
                "-o",
                f"ControlPath={control_path}",
            ]
        )

        return options


    def ssh_command_text(
        self,
    ) -> str:

        return " ".join(
            [
                self.ssh_binary,
                *self.ssh_options(),
            ]
        )


    def remote_file_path(
        self,
        info: ParquetFileInfo,
    ) -> str:

        relative = (
            info.relative_path
            .as_posix()
        )

        return (
            f"{self.remote_root}/"
            f"{relative}"
        )


    def remote_directory(
        self,
        info: ParquetFileInfo,
    ) -> str:

        relative_parent = (
            info.relative_path
            .parent
            .as_posix()
        )

        if relative_parent == ".":
            return self.remote_root

        return (
            f"{self.remote_root}/"
            f"{relative_parent}"
        )


    def build_rsync_command(
        self,
        info: ParquetFileInfo,
    ) -> list[str]:

        remote_dir = (
            self.remote_directory(
                info
            )
        )

        command = [
            self.rsync_binary,
            "-a",
            "--partial",
            # "--protect-args",
        ]

        if self.dry_run:

            command.append(
                "--dry-run"
            )

        command.extend(
            [
                "-e",
                self.ssh_command_text(),
                str(
                    info.file_path
                ),
                (
                    f"{self.ssh_target}:"
                    f"{remote_dir}/"
                ),
            ]
        )

        return command


    def build_mkdir_command(
        self,
        info: ParquetFileInfo,
    ) -> list[str]:

        remote_dir = (
            self.remote_directory(
                info
            )
        )

        return [
            self.ssh_binary,
            *self.ssh_options(),
            self.ssh_target,
            "mkdir",
            "-p",
            remote_dir,
        ]


    def _run(
        self,
        command: list[str],
    ) -> subprocess.CompletedProcess:

        last_result: subprocess.CompletedProcess | None = None

        for attempt in range(
            1,
            self.command_attempts + 1,
        ):
            try:
                result = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=self.command_timeout_seconds,
                )
            except subprocess.TimeoutExpired as exc:
                result = subprocess.CompletedProcess(
                    command,
                    124,
                    stdout=(
                        exc.stdout
                        if isinstance(exc.stdout, str)
                        else ""
                    ),
                    stderr=(
                        "command timed out after "
                        f"{self.command_timeout_seconds:g}s"
                    ),
                )

            last_result = result

            if result.returncode == 0:
                return result

            if attempt < self.command_attempts:
                time.sleep(
                    self.retry_sleep_seconds
                )

        assert last_result is not None
        return last_result

    @staticmethod
    def _clean_stderr(
        stderr: str,
    ) -> str:
        """
        Remove known non-fatal remote shell noise from stderr.
        """

        lines = []

        for line in str(
            stderr
            or ""
        ).splitlines():

            text = line.strip()

            if not text:

                continue

            if (
                "setlocale: LC_ALL: cannot change locale"
                in text
            ):

                continue

            lines.append(
                text
            )

        return "\n".join(
            lines
        )


    def _ensure_remote_directory(
        self,
        info: ParquetFileInfo,
    ) -> None:

        if self.dry_run:
            return

        remote_dir = self.remote_directory(
            info
        )

        if (
            self.cache_remote_directories
            and remote_dir in self._ensured_remote_directories
        ):
            return

        command = (
            self.build_mkdir_command(
                info
            )
        )

        result = self._run(
            command
        )

        if result.returncode != 0:

            raise UploadError(
                "remote mkdir failed: "
                f"{self._clean_stderr(result.stderr) or 'unknown error'}"
            )

        if self.cache_remote_directories:
            self._ensured_remote_directories.add(
                remote_dir
            )


    def _remote_stat_size(
        self,
        remote_path: str,
    ) -> int:

        command = [
            self.ssh_binary,
            *self.ssh_options(),
            self.ssh_target,
            "stat",
            "-c",
            "%s",
            remote_path,
        ]

        result = self._run(
            command
        )

        if result.returncode != 0:

            raise UploadError(
                "remote stat failed: "
                f"{self._clean_stderr(result.stderr)}"
            )

        text = (
            result.stdout
            .strip()
        )

        try:
            return int(
                text
            )

        except ValueError as exc:
            raise UploadError(
                "invalid remote stat output: "
                f"{text!r}"
            ) from exc


    def _remote_sha256(
        self,
        remote_path: str,
    ) -> str:

        command = [
            self.ssh_binary,
            *self.ssh_options(),
            self.ssh_target,
            "sha256sum",
            remote_path,
        ]

        result = self._run(
            command
        )

        if result.returncode != 0:

            raise UploadError(
                "remote sha256sum failed: "
                f"{result.stderr.strip()}"
            )

        text = (
            result.stdout
            .strip()
        )

        if not text:
            raise UploadError(
                "empty sha256sum output"
            )

        digest = (
            text.split()[0]
        )

        if len(
            digest
        ) != 64:
            raise UploadError(
                "invalid remote sha256: "
                f"{digest!r}"
            )

        return digest


    def upload(
        self,
        info: ParquetFileInfo,
    ) -> UploadResult:

        _validate_local_file(
            info
        )

        local_size = (
            info.file_path
            .stat()
            .st_size
        )

        remote_path = (
            self.remote_file_path(
                info
            )
        )

        if self.dry_run:

            return UploadResult(
                local_path=(
                    info.file_path
                ),
                remote_path=remote_path,
                status="dry_run",
                local_size=local_size,
                local_sha256=info.sha256,
                message=(
                    "dry-run: "
                    + " ".join(
                        shlex.quote(
                            part
                        )
                        for part
                        in self.build_rsync_command(
                            info
                        )
                    )
                ),
            )

        self._ensure_remote_directory(
            info
        )

        command = (
            self.build_rsync_command(
                info
            )
        )

        result = self._run(
            command
        )

        if result.returncode != 0:

            raise UploadError(
                "rsync failed: "
                f"returncode="
                f"{result.returncode}, "
                f"stderr="
                f"{self._clean_stderr(result.stderr)}"
            )

        remote_size = None
        remote_sha256 = None

        if (
            self.trust_rsync_success
            and not self.verify_sha256
        ):
            remote_size = local_size

        elif self.verify_size:

            remote_size = (
                self._remote_stat_size(
                    remote_path
                )
            )

            if (
                remote_size
                != local_size
            ):
                raise UploadError(
                    "remote size mismatch: "
                    f"local={local_size}, "
                    f"remote={remote_size}"
                )

        if self.verify_sha256:

            remote_sha256 = (
                self._remote_sha256(
                    remote_path
                )
            )

            if (
                remote_sha256
                != info.sha256
            ):
                raise UploadError(
                    "remote sha256 mismatch: "
                    f"local={info.sha256}, "
                    f"remote={remote_sha256}"
                )

        status: UploadStatus

        if (
            self.verify_size
            or self.verify_sha256
            or self.trust_rsync_success
        ):
            status = "verified"

        else:
            status = "uploaded"

        return UploadResult(
            local_path=(
                info.file_path
            ),
            remote_path=remote_path,
            status=status,
            local_size=local_size,
            remote_size=remote_size,
            local_sha256=info.sha256,
            remote_sha256=remote_sha256,
            message=(
                result.stdout.strip()
                or None
            ),
        )
