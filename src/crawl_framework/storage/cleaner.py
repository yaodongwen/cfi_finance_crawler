from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from crawl_framework.storage.parquet_writer import (
    ParquetFileInfo,
)
from crawl_framework.storage.uploader import (
    UploadResult,
)
from crawl_framework.storage.record_index import (
    RecordIndexStore,
)

CleanupDecision = Literal[
    "keep",
    "delete",
]


@dataclass(
    frozen=True,
    slots=True,
)
class CleanupContext:
    """
    Cleaner 做删除决策时需要的状态。

    uploaded:
        文件是否完成远端上传

    verified:
        远端文件是否完成校验

    catalog_registered:
        PostgreSQL data_files 是否已注册

    seen_committed:
        该批记录是否已写入 SeenStore

    checkpoint_committed:
        对应爬虫 checkpoint 是否已安全提交

    当前第一版全部设为显式布尔值，
    后面 pipeline 会自动生成。
    """

    uploaded: bool

    verified: bool

    catalog_registered: bool

    seen_committed: bool

    checkpoint_committed: bool


@dataclass(
    frozen=True,
    slots=True,
)
class CleanupResult:
    """
    Cleaner 执行结果。

    deleted:
        parquet 是否被删除。

    record_index_deleted:
        对应 record sidecar 是否被删除。
    """

    file_path: Path

    decision: CleanupDecision

    deleted: bool

    reason: str

    record_index_deleted: bool = False


    @property
    def kept(
        self,
    ) -> bool:

        return not self.deleted

class Cleaner:
    """
    本地 warehouse 清理器。

    删除前必须同时满足：

        uploaded
        verified
        catalog_registered
        seen_committed
        checkpoint_committed

    任何一项失败：

        KEEP

    Cleaner 不自行推断状态。
    """

    def __init__(
        self,
        *,
        dry_run: bool = False,
        record_index_store: RecordIndexStore | None = None,
    ) -> None:

        self.dry_run = bool(
            dry_run
        )

        self.record_index_store = (
            record_index_store
            or RecordIndexStore()
        )


    def can_delete(
        self,
        context: CleanupContext,
    ) -> tuple[
        bool,
        str,
    ]:

        if not context.uploaded:

            return (
                False,
                "remote upload not completed",
            )

        if not context.verified:

            return (
                False,
                "remote verification not completed",
            )

        if not context.catalog_registered:

            return (
                False,
                "postgres catalog not registered",
            )

        if not context.seen_committed:

            return (
                False,
                "seen store not committed",
            )

        if not context.checkpoint_committed:

            return (
                False,
                "checkpoint not committed",
            )

        return (
            True,
            "safe to delete",
        )


    def clean(
        self,
        info: ParquetFileInfo,
        context: CleanupContext,
    ) -> CleanupResult:
        """
        根据生命周期状态决定是否删除：

            parquet
            +
            record recovery sidecar

        删除原则：

            只有 parquet 已满足完整 durability 条件，
            才允许一起清理 sidecar。
        """

        path = (
            info.file_path
        )

        allowed, reason = (
            self.can_delete(
                context
            )
        )

        if not allowed:

            return CleanupResult(
                file_path=path,
                decision="keep",
                deleted=False,
                reason=reason,
                record_index_deleted=False,
            )

        # --------------------------------------------------------
        # parquet 本地文件已经不存在
        # --------------------------------------------------------

        if not path.exists():

            index_deleted = False

            if (
                self.record_index_store
                .exists_for_parquet(
                    path
                )
            ):

                if not self.dry_run:

                    index_deleted = (
                        self.record_index_store
                        .delete_for_parquet(
                            path
                        )
                    )

            return CleanupResult(
                file_path=path,
                decision="delete",
                deleted=False,
                reason=(
                    "parquet already missing"
                ),
                record_index_deleted=(
                    index_deleted
                ),
            )

        # --------------------------------------------------------
        # 不是普通文件
        # --------------------------------------------------------

        if not path.is_file():

            return CleanupResult(
                file_path=path,
                decision="keep",
                deleted=False,
                reason=(
                    "path is not a file"
                ),
                record_index_deleted=False,
            )

        # --------------------------------------------------------
        # Dry run
        # --------------------------------------------------------

        if self.dry_run:

            return CleanupResult(
                file_path=path,
                decision="delete",
                deleted=False,
                reason=(
                    "dry-run: files kept"
                ),
                record_index_deleted=False,
            )

        # --------------------------------------------------------
        # 1. 删除 Parquet
        # --------------------------------------------------------

        path.unlink()

        # --------------------------------------------------------
        # 2. 删除对应 sidecar
        #
        # Sidecar 不存在不视为失败，
        # 因为旧版本 pipeline 可能没有 sidecar。
        # --------------------------------------------------------

        record_index_deleted = (
            self.record_index_store
            .delete_for_parquet(
                path
            )
        )

        return CleanupResult(
            file_path=path,
            decision="delete",
            deleted=True,
            reason=(
                "local parquet deleted"
            ),
            record_index_deleted=(
                record_index_deleted
            ),
        )

def cleanup_context_from_upload(
    upload_result: UploadResult,
    *,
    catalog_registered: bool,
    seen_committed: bool,
    checkpoint_committed: bool,
) -> CleanupContext:
    """
    根据 UploadResult 快速生成 CleanupContext。

    uploaded:
        uploaded / verified

    verified:
        只有 verified 才为 True
    """

    uploaded = (
        upload_result.status
        in {
            "uploaded",
            "verified",
        }
    )

    verified = (
        upload_result.status
        == "verified"
    )

    return CleanupContext(
        uploaded=uploaded,
        verified=verified,
        catalog_registered=(
            catalog_registered
        ),
        seen_committed=(
            seen_committed
        ),
        checkpoint_committed=(
            checkpoint_committed
        ),
    )