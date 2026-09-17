from __future__ import annotations

import asyncio

from dataclasses import dataclass
from typing import Any

from crawl_framework.core.barrier import (
    PipelineBarrier,
)
from crawl_framework.core.pipeline import (
    StoragePipeline,
)
from crawl_framework.core.plugin import (
    CrawlCheckpoint,
    CrawlContext,
    CrawlScope,
    SitePlugin,
)
from crawl_framework.storage.checkpoint import (
    CheckpointStore,
    checkpoint_key_for_scope,
)


# ============================================================
# Stats
# ============================================================


@dataclass(
    slots=True,
)
class RuntimeStats:
    """
    Runtime 层统计。

    RuntimeStats 关注：

        dataset
        scope
        raw record
        normalize
        checkpoint
        crawl error

    StoragePipeline.stats 关注：

        seen
        buffer
        parquet
        upload
        postgres
        cleanup
    """

    datasets_started: int = 0

    datasets_finished: int = 0

    scopes_discovered: int = 0

    scopes_started: int = 0

    scopes_finished: int = 0

    raw_records: int = 0

    normalized_records: int = 0

    skipped_records: int = 0

    checkpoints_loaded: int = 0

    checkpoints_saved: int = 0

    errors: int = 0


# ============================================================
# Scope runtime state
# ============================================================


@dataclass(
    slots=True,
)
class ScopeRuntimeState:
    """
    一个 CrawlScope 的 Runtime 状态。
    """

    dataset: str

    scope: CrawlScope

    scope_token: str

    checkpoint: CrawlCheckpoint

    raw_count: int = 0

    normalized_count: int = 0

    skipped_count: int = 0



@dataclass(
    frozen=True,
    slots=True,
)
class ScopePipelineStats:
    """
    单个 CrawlScope 对 StoragePipeline.stats
    产生的增量统计。

    注意：

    StoragePipeline.stats 本身是整个进程累计值。

    ScopePipelineStats 使用：

        after - before

    得到当前 scope 自己产生的 storage 行为，
    避免多股票、多 scope 运行时统计互相污染。
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
# Scope result
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class ScopeRunResult:
    """
    一个 CrawlScope 完成后的结果。

    pipeline:

        当前 scope 对 StoragePipeline.stats
        产生的增量。

    例如：

        raw_count = 20
        pipeline.records_unchanged = 20
        pipeline.files_written = 0

    表示：

        crawler 抓到并规范化了 20 条，
        但全部已经存在，
        因此没有重新写 Parquet。
    """

    dataset: str

    scope_type: str

    scope_id: str | None

    source_key: str

    scope_token: str

    raw_count: int

    normalized_count: int

    skipped_count: int

    checkpoint_state: dict[
        str,
        Any,
    ]

    pipeline: ScopePipelineStats

# ============================================================
# Scope token
# ============================================================


def make_scope_token(
    *,
    site_id: str,
    dataset: str,
    scope: CrawlScope,
) -> str:
    """
    生成稳定 CrawlScope token。

    格式：

        site_id|dataset|scope_type|source_key

    示例：

        demo_site|forum_post|instrument|005930

    为什么使用 source_key 而不是 scope_id：

        source_key 是 crawler 在该网站上的稳定抓取键。

    例如：

        Naver:
            source_key = 005930

        Toss:
            source_key = 005930

        HotCopper:
            source_key = BHP

    scope_id 则更偏向 canonical identity，例如：

        XKRX:005930
        XASX:BHP

    Runtime checkpoint / crawl isolation 更应该绑定 source_key。
    """

    site_id = str(
        site_id
    ).strip()

    dataset = str(
        dataset
    ).strip()

    scope_type = str(
        scope.scope_type
    ).strip()

    source_key = str(
        scope.source_key
    ).strip()

    if not site_id:

        raise ValueError(
            "site_id cannot be empty"
        )

    if not dataset:

        raise ValueError(
            "dataset cannot be empty"
        )

    if not scope_type:

        raise ValueError(
            "scope.scope_type cannot be empty"
        )

    if not source_key:

        raise ValueError(
            "scope.source_key cannot be empty"
        )

    return "|".join(
        (
            site_id,
            dataset,
            scope_type,
            source_key,
        )
    )


# ============================================================
# Runtime
# ============================================================


class CrawlRuntime:
    """
    SitePlugin Runtime。

    总体流程：

        plugin.discover()
            ↓
        CrawlScope
            ↓
        make_scope_token()
            ↓
        CheckpointStore.load()
            ↓
        barrier.begin_scope(scope_token)
            ↓
        plugin.crawl()
            ↓
        plugin.normalize()
            ↓
        pipeline.submit(
            record,
            scope_token=scope_token
        )
            ↓
        barrier.track(record)
            ↓
        barrier.wait_until_durable()
            ↓
        checkpoint.save()
            ↓
        barrier.end_scope()

    ----------------------------------------------------------

    安全语义：

        checkpoint 只有在当前 scope 全部记录 durable 后
        才允许推进。

    ----------------------------------------------------------

    durable 当前意味着至少已经完成：

        Parquet
        Upload
        Verify
        PostgreSQL Catalog
        SeenStore Commit
    """

    def __init__(
        self,
        *,
        plugin: SitePlugin,
        pipeline: StoragePipeline,
        checkpoint_store: CheckpointStore,
        context: CrawlContext | None = None,
    ) -> None:

        self.plugin = plugin

        self.pipeline = pipeline

        self.checkpoint_store = (
            checkpoint_store
        )

        self.context = (
            context
            or CrawlContext()
        )

        self.stats = RuntimeStats()

        self.barrier = PipelineBarrier(
            self.pipeline
        )

        self._validate_plugin()


    # ========================================================
    # Validation
    # ========================================================


    def _validate_plugin(
        self,
    ) -> None:
        """
        Runtime 启动前验证 SitePlugin。
        """

        site_id = str(
            self.plugin.site_id
        ).strip()

        country = str(
            self.plugin.country
        ).strip()

        timezone = str(
            self.plugin.timezone
        ).strip()

        if not site_id:

            raise ValueError(
                "plugin.site_id cannot be empty"
            )

        if not country:

            raise ValueError(
                "plugin.country cannot be empty"
            )

        if not timezone:

            raise ValueError(
                "plugin.timezone cannot be empty"
            )

        self.plugin.validate_datasets()


    # ========================================================
    # Whole plugin
    # ========================================================


    async def run(
        self,
        *,
        datasets: (
            list[str]
            | tuple[str, ...]
            | None
        ) = None,
        flush_at_end: bool = True,
    ) -> list[
        ScopeRunResult
    ]:
        """
        执行 SitePlugin。

        datasets=None：
            执行插件声明的全部 dataset。

        flush_at_end=True：
            Runtime 正常退出或异常退出时，
            最终执行 pipeline.flush_all()。

        final flush 主要用于：

            graceful shutdown
            清理未绑定 scope 的记录
            清理其他残余 partition

        scope durability 本身不依赖 final flush。
        """

        if datasets is None:

            selected = list(
                self.plugin
                .validate_datasets()
            )

        else:

            selected = [
                str(
                    dataset
                ).strip()
                for dataset
                in datasets
            ]

        results: list[
            ScopeRunResult
        ] = []

        try:

            for dataset in selected:

                if not (
                    self.plugin
                    .supports_dataset(
                        dataset
                    )
                ):

                    raise ValueError(
                        f"{self.plugin.site_id}: "
                        f"unsupported dataset "
                        f"{dataset!r}"
                    )

                dataset_results = (
                    await self.run_dataset(
                        dataset
                    )
                )

                results.extend(
                    dataset_results
                )

        finally:

            if flush_at_end:

                self.pipeline.flush_all()

        return results


    # ========================================================
    # Dataset
    # ========================================================


    async def run_dataset(
        self,
        dataset: str,
    ) -> list[
        ScopeRunResult
    ]:
        """
        执行一个 dataset 下全部 CrawlScope。
        """

        dataset = str(
            dataset
        ).strip()

        if not (
            self.plugin
            .supports_dataset(
                dataset
            )
        ):

            raise ValueError(
                f"{self.plugin.site_id}: "
                f"unsupported dataset "
                f"{dataset!r}"
            )

        self.stats.datasets_started += 1

        results: list[
            ScopeRunResult
        ] = []

        try:

            async for scope in (
                self.plugin.discover(
                    dataset,
                    self.context,
                )
            ):

                self.stats.scopes_discovered += 1

                result = (
                    await self.run_scope(
                        dataset,
                        scope,
                    )
                )

                results.append(
                    result
                )

        except Exception:

            self.stats.errors += 1

            raise

        else:

            self.stats.datasets_finished += 1

        return results


    # ========================================================
    # Scope
    # ========================================================

    def _pipeline_stats_snapshot(
        self,
    ) -> ScopePipelineStats:
        """
        获取当前 Pipeline 统计快照。

        正式 StoragePipeline：

            提供 .stats，
            因此可以得到完整 storage 生命周期统计。

        旧测试 Pipeline / 第三方 Pipeline：

            可能没有 .stats。

        stats 属于可观测性能力，
        不能因为缺少 stats 就破坏 crawler 核心运行。

        因此：

            没有 stats
                -> 返回全 0 ScopePipelineStats

            有 stats
                -> 读取已知字段

        同时对单个统计字段使用 getattr(..., 0)，
        兼容只实现部分统计字段的 Pipeline。
        """

        stats = getattr(
            self.pipeline,
            "stats",
            None,
        )

        # ====================================================
        # Backward compatibility
        # ====================================================

        if stats is None:

            return ScopePipelineStats()

        # ====================================================
        # Snapshot
        # ====================================================

        return ScopePipelineStats(
            records_seen=int(
                getattr(
                    stats,
                    "records_seen",
                    0,
                )
            ),
            records_new=int(
                getattr(
                    stats,
                    "records_new",
                    0,
                )
            ),
            records_updated=int(
                getattr(
                    stats,
                    "records_updated",
                    0,
                )
            ),
            records_unchanged=int(
                getattr(
                    stats,
                    "records_unchanged",
                    0,
                )
            ),
            records_buffered=int(
                getattr(
                    stats,
                    "records_buffered",
                    0,
                )
            ),
            files_written=int(
                getattr(
                    stats,
                    "files_written",
                    0,
                )
            ),
            files_uploaded=int(
                getattr(
                    stats,
                    "files_uploaded",
                    0,
                )
            ),
            files_verified=int(
                getattr(
                    stats,
                    "files_verified",
                    0,
                )
            ),
            files_registered=int(
                getattr(
                    stats,
                    "files_registered",
                    0,
                )
            ),
            files_deleted=int(
                getattr(
                    stats,
                    "files_deleted",
                    0,
                )
            ),
            errors=int(
                getattr(
                    stats,
                    "errors",
                    0,
                )
            ),
        )

    @staticmethod
    def _pipeline_stats_delta(
        before: ScopePipelineStats,
        after: ScopePipelineStats,
    ) -> ScopePipelineStats:
        """
        计算单个 scope 对 PipelineStats
        产生的增量。
        """

        return ScopePipelineStats(
            records_seen=(
                after.records_seen
                - before.records_seen
            ),
            records_new=(
                after.records_new
                - before.records_new
            ),
            records_updated=(
                after.records_updated
                - before.records_updated
            ),
            records_unchanged=(
                after.records_unchanged
                - before.records_unchanged
            ),
            records_buffered=(
                after.records_buffered
                - before.records_buffered
            ),
            files_written=(
                after.files_written
                - before.files_written
            ),
            files_uploaded=(
                after.files_uploaded
                - before.files_uploaded
            ),
            files_verified=(
                after.files_verified
                - before.files_verified
            ),
            files_registered=(
                after.files_registered
                - before.files_registered
            ),
            files_deleted=(
                after.files_deleted
                - before.files_deleted
            ),
            errors=(
                after.errors
                - before.errors
            ),
        )

    async def run_scope(
        self,
        dataset: str,
        scope: CrawlScope,
    ) -> ScopeRunResult:
        """
        执行一个 CrawlScope。

        安全顺序：

            checkpoint.load
                ↓
            pipeline stats snapshot BEFORE
                ↓
            barrier.begin_scope
                ↓
            crawl
                ↓
            normalize
                ↓
            pipeline.submit
                ↓
            barrier.track
                ↓
            计算 candidate checkpoint
                ↓
            barrier.wait_until_durable
                ↓
            checkpoint.save
                ↓
            pipeline stats snapshot AFTER
                ↓
            barrier.end_scope

        checkpoint 只有在当前 scope
        已经通过 durability barrier 后
        才真正写入磁盘。

        PipelineStats 本身是进程累计值。

        本函数通过：

            after - before

        计算当前 scope 自己的 storage 统计。
        """

        self.stats.scopes_started += 1

        # ====================================================
        # 1. Scope token
        # ====================================================

        scope_token = make_scope_token(
            site_id=(
                self.plugin.site_id
            ),
            dataset=dataset,
            scope=scope,
        )

        # ====================================================
        # 2. Checkpoint key
        # ====================================================

        checkpoint_key = (
            checkpoint_key_for_scope(
                site_id=(
                    self.plugin.site_id
                ),
                dataset=dataset,
                scope=scope,
            )
        )

        # ====================================================
        # 3. Load durable checkpoint
        # ====================================================

        checkpoint = (
            self.checkpoint_store
            .load(
                checkpoint_key
            )
        )

        self.stats.checkpoints_loaded += 1

        candidate_checkpoint = (
            checkpoint
        )

        state = ScopeRuntimeState(
            dataset=dataset,
            scope=scope,
            scope_token=scope_token,
            checkpoint=checkpoint,
        )

        # ====================================================
        # 3.5 Pipeline stats BEFORE
        # ====================================================

        pipeline_before = (
            self._pipeline_stats_snapshot()
        )

        # ====================================================
        # 4. Begin durability scope
        # ====================================================

        self.barrier.begin_scope(
            scope_token
        )

        try:

            # =================================================
            # Crawl
            # =================================================

            async for raw in (
                self.plugin.crawl(
                    dataset,
                    scope,
                    checkpoint,
                    self.context,
                )
            ):

                self.stats.raw_records += 1

                state.raw_count += 1

                # =============================================
                # Normalize
                # =============================================

                record = (
                    self.plugin
                    .normalize_and_validate(
                        dataset,
                        raw,
                        scope,
                    )
                )

                if record is None:

                    self.stats.skipped_records += 1

                    state.skipped_count += 1

                    continue

                self.stats.normalized_records += 1

                state.normalized_count += 1

                # =============================================
                # Storage submit
                # =============================================

                self.pipeline.submit(
                    record,
                    scope_token=(
                        scope_token
                    ),
                )

                # =============================================
                # Durability tracking
                # =============================================

                self.barrier.track(
                    record
                )

                # =============================================
                # Candidate checkpoint
                # =============================================

                checkpoint_hook = getattr(
                    self.plugin,
                    "checkpoint_after_record",
                    None,
                )

                if callable(
                    checkpoint_hook
                ):

                    candidate_checkpoint = (
                        checkpoint_hook(
                            dataset,
                            scope,
                            raw,
                            record,
                            candidate_checkpoint,
                            self.context,
                        )
                    )

            scope_checkpoint_hook = getattr(
                self.plugin,
                "checkpoint_after_scope",
                None,
            )
            if callable(scope_checkpoint_hook):
                candidate_checkpoint = scope_checkpoint_hook(
                    dataset,
                    scope,
                    candidate_checkpoint,
                    self.context,
                )

            # =================================================
            # Durability Barrier
            # =================================================

            self.barrier.wait_until_durable()

            # =================================================
            # Save Checkpoint
            # =================================================

            if (
                candidate_checkpoint
                is not None
            ):

                self.checkpoint_store.save(
                    checkpoint_key,
                    candidate_checkpoint,
                )

                self.stats.checkpoints_saved += 1

                state.checkpoint = (
                    candidate_checkpoint
                )

        except Exception:

            self.stats.errors += 1

            # candidate checkpoint 不保存。
            #
            # 已 durable 的记录：
            #     SeenStore 去重。
            #
            # 未 durable 的记录：
            #     下次重新抓。

            raise

        else:

            self.stats.scopes_finished += 1

        finally:

            self.barrier.end_scope()

        # ====================================================
        # 5. Pipeline stats AFTER
        # ====================================================

        pipeline_after = (
            self._pipeline_stats_snapshot()
        )

        pipeline_delta = (
            self._pipeline_stats_delta(
                pipeline_before,
                pipeline_after,
            )
        )

        # ====================================================
        # 6. Result checkpoint
        # ====================================================

        final_checkpoint = (
            state.checkpoint
        )

        checkpoint_state = {}

        if (
            final_checkpoint is not None
            and isinstance(
                final_checkpoint.state,
                dict,
            )
        ):

            checkpoint_state = dict(
                final_checkpoint.state
            )

        # ====================================================
        # 7. Result
        # ====================================================

        return ScopeRunResult(
            dataset=dataset,
            scope_type=(
                scope.scope_type
            ),
            scope_id=(
                scope.scope_id
            ),
            source_key=(
                scope.source_key
            ),
            scope_token=(
                scope_token
            ),
            raw_count=(
                state.raw_count
            ),
            normalized_count=(
                state.normalized_count
            ),
            skipped_count=(
                state.skipped_count
            ),
            checkpoint_state=(
                checkpoint_state
            ),
            pipeline=(
                pipeline_delta
            ),
        )

# ============================================================
# Sync entry
# ============================================================


def run_runtime(
    runtime: CrawlRuntime,
    *,
    datasets: (
        list[str]
        | tuple[str, ...]
        | None
    ) = None,
    flush_at_end: bool = True,
) -> list[
    ScopeRunResult
]:
    """
    CLI / 普通脚本同步入口。

    示例：

        results = run_runtime(
            runtime
        )
    """

    return asyncio.run(
        runtime.run(
            datasets=datasets,
            flush_at_end=flush_at_end,
        )
    )
