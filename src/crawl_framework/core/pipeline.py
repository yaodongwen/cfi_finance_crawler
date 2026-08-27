from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from crawl_framework.core.models import (
    CanonicalRecord,
)
from crawl_framework.storage.buffer import (
    FlushBatch,
    RecordBuffer,
)
from crawl_framework.storage.record_index import (
    RecordIndexStore,
)
from crawl_framework.storage.checkpoint import (
    CheckpointKey,
    CheckpointStore,
)
from crawl_framework.storage.cleaner import (
    Cleaner,
    CleanupContext,
)
from crawl_framework.storage.parquet_writer import (
    ParquetFileInfo,
    ParquetWriter,
)
from crawl_framework.storage.postgres import (
    PostgresCatalog,
)
from crawl_framework.storage.recovery import (
    RecoveryManifest,
    RecoveryStore,
)
from crawl_framework.storage.seen_store import (
    SeenDecision,
    SeenStore,
)
from crawl_framework.storage.uploader import (
    BaseUploader,
    UploadResult,
)


# ============================================================
# Stats
# ============================================================


@dataclass(
    slots=True,
)
class PipelineStats:
    """
    StoragePipeline 进程内统计。

    RuntimeStats：
        关注 crawler/scope/raw record。

    PipelineStats：
        关注 storage 生命周期。
    """

    records_seen: int = 0

    records_new: int = 0

    records_updated: int = 0

    records_unchanged: int = 0

    records_buffered: int = 0

    files_written: int = 0

    files_uploaded: int = 0

    files_verified: int = 0

    files_registered: int = 0

    files_deleted: int = 0

    errors: int = 0


# ============================================================
# Record decision
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class PipelineRecordDecision:
    """
    单条 CanonicalRecord 进入 Pipeline 后的判断。
    """

    record_uid: str

    version_hash: str

    decision: SeenDecision

    buffered: bool


# ============================================================
# Batch result
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class BatchProcessResult:
    """
    一个 FlushBatch 完整生命周期执行结果。
    """

    parquet_info: ParquetFileInfo

    upload_result: UploadResult

    catalog_registered: bool

    seen_committed: bool

    checkpoint_committed: bool

    local_deleted: bool


# ============================================================
# Storage Pipeline
# ============================================================


class StoragePipeline:
    """
    Crawl Framework Storage Pipeline。

    核心顺序：

        CanonicalRecord
            ↓
        SeenStore.inspect
            ↓
        RecordBuffer
            ↓
        FlushBatch
            ↓
        ParquetWriter
            ↓
        RecoveryManifest
            ↓
        Uploader
            ↓
        Verify
            ↓
        PostgreSQL Catalog
            ↓
        SeenStore.commit
            ↓
        Cleaner

    ----------------------------------------------------------

    scope_token：

    Runtime 可以：

        pipeline.submit(
            record,
            scope_token="..."
        )

    当一个 scope 结束时：

        pipeline.flush_scope(
            scope_token
        )

    只 flush 与该 scope 有关的 Buffer partition。

    ----------------------------------------------------------

    SeenStore 原则：

    inspect：
        只检查。

    commit：
        必须等到 durable storage 完成。

    绝不能：

        inspect
        ↓
        立刻 mark seen
        ↓
        storage 失败

    否则会产生永久数据缺口。
    """

    def __init__(
        self,
        *,
        seen_store: SeenStore,
        buffer: RecordBuffer,
        parquet_writer: ParquetWriter,
        uploader: BaseUploader,
        catalog: PostgresCatalog,
        recovery_store: RecoveryStore,
        cleaner: Cleaner,
        checkpoint_store: CheckpointStore | None = None,
        record_index_store: RecordIndexStore | None = None,
    ) -> None:

        self.seen_store = seen_store

        self.buffer = buffer

        self.parquet_writer = parquet_writer

        self.uploader = uploader

        self.catalog = catalog

        self.recovery_store = recovery_store

        self.cleaner = cleaner

        self.checkpoint_store = (
            checkpoint_store
        )

        self.record_index_store = (
            record_index_store
            or RecordIndexStore()
        )

        self.stats = PipelineStats()


    # ========================================================
    # Record intake
    # ========================================================


    def submit(
        self,
        record: CanonicalRecord,
        *,
        scope_token: str | None = None,
    ) -> tuple[
        PipelineRecordDecision,
        list[BatchProcessResult],
    ]:
        """
        提交一条 CanonicalRecord。

        scope_token=None：

            完全兼容旧代码。

        scope_token="..."：

            RecordBuffer 会记录该记录来自哪个 CrawlScope。

            后面：

                flush_scope(scope_token)

            可以只处理与该 scope 有关的 partition。

        返回：

            PipelineRecordDecision
            +
            因自动 flush 产生的 BatchProcessResult
        """

        self.stats.records_seen += 1

        # ----------------------------------------------------
        # 1. SeenStore inspect
        # ----------------------------------------------------

        seen = self.seen_store.inspect(
            record.record_uid,
            record.version_hash,
        )

        if seen.decision == "unchanged":

            self.stats.records_unchanged += 1

            return (
                PipelineRecordDecision(
                    record_uid=(
                        record.record_uid
                    ),
                    version_hash=(
                        record.version_hash
                    ),
                    decision="unchanged",
                    buffered=False,
                ),
                [],
            )

        if seen.decision == "new":

            self.stats.records_new += 1

        elif seen.decision == "updated":

            self.stats.records_updated += 1

        # ----------------------------------------------------
        # 2. Buffer
        # ----------------------------------------------------

        batch = self.buffer.add(
            record,
            scope_token=scope_token,
        )

        self.stats.records_buffered += 1

        results: list[
            BatchProcessResult
        ] = []

        # ----------------------------------------------------
        # 3. Buffer 自动 flush
        # ----------------------------------------------------

        if batch is not None:

            results.append(
                self.process_batch(
                    batch
                )
            )

        return (
            PipelineRecordDecision(
                record_uid=(
                    record.record_uid
                ),
                version_hash=(
                    record.version_hash
                ),
                decision=seen.decision,
                buffered=True,
            ),
            results,
        )


    def submit_many(
        self,
        records: Iterable[
            CanonicalRecord
        ],
        *,
        scope_token: str | None = None,
    ) -> list[
        PipelineRecordDecision
    ]:
        """
        批量提交记录。

        同一个调用中的 records 可绑定到同一个 scope_token。

        自动 flush 的 batch 会立即完整持久化。
        """

        decisions: list[
            PipelineRecordDecision
        ] = []

        for record in records:

            decision, _ = self.submit(
                record,
                scope_token=scope_token,
            )

            decisions.append(
                decision
            )

        return decisions


    # ========================================================
    # Scope flush
    # ========================================================


    def flush_scope(
        self,
        scope_token: str,
    ) -> list[
        BatchProcessResult
    ]:
        """
        强制 flush 当前 Buffer 中与 scope_token 有关的 partition。

        与 flush_all() 的区别：

        flush_all():
            整个进程全部 Buffer。

        flush_scope():
            只处理包含指定 scope_token 的 partition。

        ------------------------------------------------------

        注意：

        如果 partition A 同时包含：

            scope-A
            scope-B

        那么：

            flush_scope("scope-A")

        会完整 flush partition A。

        因此 scope-B 的部分数据也可能一起被持久化。

        这是正常且安全的。

        它只是：

            提前 durable

        不会：

            丢数据
            重复 checkpoint
            错误 mark seen
        """

        batches = (
            self.buffer
            .flush_scope(
                scope_token
            )
        )

        return [
            self.process_batch(
                batch
            )
            for batch
            in batches
        ]


    # ========================================================
    # Timeout flush
    # ========================================================


    def flush_expired(
        self,
    ) -> list[
        BatchProcessResult
    ]:
        """
        处理 timeout 到期的 partition。
        """

        batches = (
            self.buffer
            .collect_expired()
        )

        return [
            self.process_batch(
                batch
            )
            for batch
            in batches
        ]


    # ========================================================
    # Full flush
    # ========================================================


    def flush_all(
        self,
    ) -> list[
        BatchProcessResult
    ]:
        """
        强制处理全部 Buffer。

        主要用于：

            graceful shutdown
            CLI 最终退出
            integration test

        Scope durability barrier 后续应优先使用：

            flush_scope(scope_token)
        """

        batches = (
            self.buffer
            .flush_all()
        )

        return [
            self.process_batch(
                batch
            )
            for batch
            in batches
        ]


    # ========================================================
    # Buffer queries
    # ========================================================


    def has_scope(
        self,
        scope_token: str,
    ) -> bool:
        """
        当前 Buffer 是否还有指定 scope 的未 flush 数据。
        """

        return self.buffer.has_scope(
            scope_token
        )


    def buffered_scope_partition_count(
        self,
        scope_token: str,
    ) -> int:
        """
        当前 scope 涉及多少个未 flush partition。

        后面可用于：

            metrics
            debugging
            runtime monitoring
        """

        return len(
            self.buffer
            .partition_keys_for_scope(
                scope_token
            )
        )


    # ========================================================
    # Batch lifecycle
    # ========================================================


    def process_batch(
        self,
        batch: FlushBatch,
        *,
        checkpoint_key: CheckpointKey | None = None,
        checkpoint=None,
    ) -> BatchProcessResult:
        """
        一个 FlushBatch 的完整 durable lifecycle。

        顺序不能随意改变。
        """

        parquet_info = None

        manifest = None

        try:

            # =================================================
            # 1. Write local Parquet
            # =================================================

            parquet_info = (
                self.parquet_writer
                .write_batch(
                    batch
                )
            )

            self.stats.files_written += 1

            # =================================================
            # 1.5 Write recovery record index
            # =================================================

            record_index_info = (
                self.record_index_store
                .write_for_parquet(
                    parquet_info.file_path,
                    batch.records,
                )
            )

            # =================================================
            # 2. Recovery Manifest
            # =================================================

            manifest = (
                self._create_manifest(
                    parquet_info,
                    record_index_path=(
                        record_index_info
                        .file_path
                    ),
                )
            )

            self.recovery_store.save(
                manifest
            )

            # =================================================
            # 3. Upload
            # =================================================

            upload_result = (
                self.uploader.upload(
                    parquet_info
                )
            )

            if upload_result.status in {
                "uploaded",
                "verified",
            }:

                self.stats.files_uploaded += 1

            else:

                raise RuntimeError(
                    "unexpected upload status: "
                    f"{upload_result.status!r}"
                )

            manifest = (
                self.recovery_store
                .advance(
                    manifest.manifest_id,
                    "uploaded",
                    remote_path=(
                        upload_result
                        .remote_path
                    ),
                )
            )

            # =================================================
            # 4. Verify
            # =================================================

            verified = (
                upload_result.status
                == "verified"
            )

            if not verified:

                raise RuntimeError(
                    "upload completed but "
                    "remote file was not verified"
                )

            self.stats.files_verified += 1

            manifest = (
                self.recovery_store
                .advance(
                    manifest.manifest_id,
                    "verified",
                )
            )

            # =================================================
            # 5. PostgreSQL Catalog
            # =================================================

            self.catalog.register_parquet_file(
                parquet_info
            )

            # =================================================
            # Mark remote upload in PostgreSQL catalog
            # =================================================

            # if (
            #     upload_result.remote_path
            #     is not None
            # ):

            #     self.catalog.mark_uploaded(
            #         file_path=(
            #             parquet_info.relative_path
            #         ),
            #         remote_path=(
            #             upload_result.remote_path
            #         ),
            #     )
            remote_path = str(
                upload_result.remote_path
            ).strip()

            if not remote_path:

                raise RuntimeError(
                    "verified upload has no "
                    "remote_path"
                )

            self.catalog.mark_uploaded(
                file_path=(
                    parquet_info.relative_path
                ),
                remote_path=(
                    remote_path
                ),
            )

            self.stats.files_registered += 1

            manifest = (
                self.recovery_store
                .advance(
                    manifest.manifest_id,
                    "catalog_registered",
                )
            )

            # =================================================
            # 6. SeenStore commit
            # =================================================

            seen_rows = [
                (
                    record.record_uid,
                    record.version_hash,
                )
                for record
                in batch.records
            ]

            self.seen_store.commit_many(
                seen_rows
            )

            seen_committed = True

            manifest = (
                self.recovery_store
                .advance(
                    manifest.manifest_id,
                    "seen_committed",
                )
            )

            # =================================================
            # 7. Optional legacy batch checkpoint
            #
            # Runtime V2 已经把 scope checkpoint 放到 Runtime
            # + Barrier 层。
            #
            # 这里暂时保留兼容能力。
            # =================================================

            checkpoint_committed = True

            if (
                checkpoint_key is not None
                or checkpoint is not None
            ):

                if (
                    checkpoint_key is None
                    or checkpoint is None
                ):

                    raise ValueError(
                        "checkpoint_key and "
                        "checkpoint must be "
                        "provided together"
                    )

                if self.checkpoint_store is None:

                    raise RuntimeError(
                        "checkpoint_store "
                        "is not configured"
                    )

                self.checkpoint_store.save(
                    checkpoint_key,
                    checkpoint,
                )

            manifest = (
                self.recovery_store
                .advance(
                    manifest.manifest_id,
                    "checkpoint_committed",
                )
            )

            # =================================================
            # 8. Cleanable
            # =================================================

            manifest = (
                self.recovery_store
                .advance(
                    manifest.manifest_id,
                    "cleanable",
                )
            )

            # =================================================
            # 9. Cleaner
            # =================================================

            cleanup_context = (
                CleanupContext(
                    uploaded=True,
                    verified=True,
                    catalog_registered=True,
                    seen_committed=True,
                    checkpoint_committed=True,
                )
            )

            cleanup_result = (
                self.cleaner.clean(
                    parquet_info,
                    cleanup_context,
                )
            )

            local_deleted = (
                cleanup_result.deleted
            )

            if local_deleted:

                self.stats.files_deleted += 1

                self.recovery_store.advance(
                    manifest.manifest_id,
                    "deleted",
                )

            return BatchProcessResult(
                parquet_info=parquet_info,
                upload_result=upload_result,
                catalog_registered=True,
                seen_committed=(
                    seen_committed
                ),
                checkpoint_committed=(
                    checkpoint_committed
                ),
                local_deleted=(
                    local_deleted
                ),
            )

        except Exception as exc:

            self.stats.errors += 1

            # ------------------------------------------------
            # 如果 manifest 已创建，
            # 尽量记录失败状态。
            #
            # recovery 自己失败时不覆盖原始异常。
            # ------------------------------------------------

            if manifest is not None:

                try:

                    self.recovery_store.mark_failed(
                        manifest.manifest_id,
                        str(
                            exc
                        ),
                    )

                except Exception:
                    pass

            raise


    # ========================================================
    # Recovery manifest
    # ========================================================

    def _create_manifest(
        self,
        info: ParquetFileInfo,
        *,
        record_index_path,
    ) -> RecoveryManifest:
        """
        根据 ParquetFileInfo 创建 RecoveryManifest。
        record_index_path:
            与当前 parquet 对应的 record recovery sidecar。
        例如：
            part-abc.parquet
            part-abc.records.jsonl
        """
        manifest_id = (
            info.relative_path
            .as_posix()
        )
        return RecoveryManifest(
            manifest_id=manifest_id,
            local_path=(
                info.file_path
                .as_posix()
            ),
            relative_path=(
                info.relative_path
                .as_posix()
            ),
            site_id=(
                info.partition.site_id
            ),
            country=(
                info.partition.country
            ),
            dataset=(
                info.partition.dataset
            ),
            sha256=(
                info.sha256
            ),
            row_count=(
                info.row_count
            ),
            file_size=(
                info.file_size
            ),
            record_index_path=(
                record_index_path
                .as_posix()
            ),
            stage="local",
        )
        manifest_id = (
            info.relative_path
            .as_posix()
        )
        return RecoveryManifest(
            manifest_id=manifest_id,
            local_path=(
                info.file_path
                .as_posix()
            ),
            relative_path=(
                info.relative_path
                .as_posix()
            ),
            site_id=(
                info.partition.site_id
            ),
            country=(
                info.partition.country
            ),
            dataset=(
                info.partition.dataset
            ),
            sha256=(
                info.sha256
            ),
            row_count=(
                info.row_count
            ),
            file_size=(
                info.file_size
            ),
            record_index_path=(
                record_index_path
                .as_posix()
            ),
            stage="local",
        )
        manifest_id = (
            info.relative_path
            .as_posix()
        )
        return RecoveryManifest(
            manifest_id=manifest_id,
            local_path=(
                info.file_path
                .as_posix()
            ),
            relative_path=(
                info.relative_path
                .as_posix()
            ),
            site_id=(
                info.partition.site_id
            ),
            country=(
                info.partition.country
            ),
            dataset=(
                info.partition.dataset
            ),
            sha256=(
                info.sha256
            ),
            row_count=(
                info.row_count
            ),
            file_size=(
                info.file_size
            ),
            stage="local",
        )