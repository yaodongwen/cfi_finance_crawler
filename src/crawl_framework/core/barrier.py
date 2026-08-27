from __future__ import annotations

from dataclasses import dataclass

from crawl_framework.core.models import (
    CanonicalRecord,
)
from crawl_framework.core.pipeline import (
    StoragePipeline,
)


@dataclass(
    frozen=True,
    slots=True,
)
class BarrierResult:
    """
    一次 durability barrier 的结果。

    scope_token:
        当前 barrier 对应的 CrawlScope。

    submitted_records:
        当前 scope 一共 track 了多少条记录。

    durable_records:
        barrier 完成后有多少条记录已经 durable。

    flushed_batches:
        本次 barrier 实际触发多少个 batch flush。
    """

    scope_token: str

    submitted_records: int

    durable_records: int

    flushed_batches: int


class PipelineBarrier:
    """
    StoragePipeline scope-level durability barrier。

    使用方式：

        barrier.begin_scope(
            scope_token
        )

        pipeline.submit(
            record,
            scope_token=scope_token,
        )

        barrier.track(
            record
        )

        ...

        barrier.wait_until_durable()

        checkpoint.save(...)

        barrier.end_scope()

    核心规则：

        checkpoint
        必须晚于
        scope records durable
    """

    def __init__(
        self,
        pipeline: StoragePipeline,
    ) -> None:

        self.pipeline = pipeline

        self._scope_token: (
            str
            | None
        ) = None

        self._scope_records: list[
            tuple[
                str,
                str,
            ]
        ] = []


    # ========================================================
    # Scope lifecycle
    # ========================================================


    def begin_scope(
        self,
        scope_token: str,
    ) -> None:
        """
        开始一个新的 scope barrier。

        一个 PipelineBarrier 同一时间只允许跟踪一个 scope。
        """

        token = self._normalize_scope_token(
            scope_token
        )

        if self._scope_token is not None:

            raise RuntimeError(
                "barrier scope already active: "
                f"{self._scope_token!r}"
            )

        self._scope_token = token

        self._scope_records.clear()


    def end_scope(
        self,
    ) -> None:
        """
        结束当前 scope tracking。

        即使 Runtime 发生异常，
        finally 中也应该调用。
        """

        self._scope_records.clear()

        self._scope_token = None


    @property
    def scope_token(
        self,
    ) -> str | None:

        return self._scope_token


    @property
    def active(
        self,
    ) -> bool:

        return (
            self._scope_token
            is not None
        )


    # ========================================================
    # Track
    # ========================================================


    def track(
        self,
        record: CanonicalRecord,
    ) -> None:
        """
        跟踪当前 scope 提交的 CanonicalRecord。

        这里只保存：

            record_uid
            version_hash

        不复制正文。

        注意：

        必须先 begin_scope()。
        """

        self._require_active_scope()

        self._scope_records.append(
            (
                record.record_uid,
                record.version_hash,
            )
        )


    @property
    def tracked_count(
        self,
    ) -> int:

        return len(
            self._scope_records
        )


    # ========================================================
    # Durability check
    # ========================================================


    def is_durable(
        self,
        record_uid: str,
        version_hash: str,
    ) -> bool:
        """
        判断一条记录是否已经 durable。

        当前 V1 使用 SeenStore 作为 durability marker。

        因为 StoragePipeline 只有完成：

            Parquet
            Upload
            Verify
            PostgreSQL Catalog

        后才执行：

            SeenStore.commit

        所以：

            SeenStore == unchanged

        可以代表该 version 已经 durable。
        """

        result = (
            self.pipeline
            .seen_store
            .inspect(
                record_uid,
                version_hash,
            )
        )

        return (
            result.decision
            == "unchanged"
        )


    def durable_count(
        self,
    ) -> int:

        return sum(
            1
            for (
                record_uid,
                version_hash,
            )
            in self._scope_records
            if self.is_durable(
                record_uid,
                version_hash,
            )
        )


    def all_durable(
        self,
    ) -> bool:

        return (
            self.durable_count()
            == self.tracked_count
        )


    # ========================================================
    # Barrier
    # ========================================================


    def wait_until_durable(
        self,
    ) -> BarrierResult:
        """
        当前 scope durability barrier。

        流程：

            1. 必须存在 active scope
            2. 检查 track records 是否已经 durable
            3. 如果不是：
                   pipeline.flush_scope(scope_token)
            4. 再检查 SeenStore
            5. 如果仍然不 durable：
                   raise RuntimeError

        与旧版最大的区别：

            不再调用 pipeline.flush_all()

        因此不会主动 flush 无关 scope。
        """

        token = (
            self._require_active_scope()
        )

        submitted = (
            self.tracked_count
        )

        # ----------------------------------------------------
        # 空 scope
        # ----------------------------------------------------

        if submitted == 0:

            return BarrierResult(
                scope_token=token,
                submitted_records=0,
                durable_records=0,
                flushed_batches=0,
            )

        # ----------------------------------------------------
        # 已经 durable
        #
        # 例如 buffer 达到 max_rows，
        # submit() 时已经自动 flush。
        # ----------------------------------------------------

        if self.all_durable():

            return BarrierResult(
                scope_token=token,
                submitted_records=submitted,
                durable_records=submitted,
                flushed_batches=0,
            )

        # ----------------------------------------------------
        # 只 flush 当前 scope
        # ----------------------------------------------------

        flush_results = (
            self.pipeline
            .flush_scope(
                token
            )
        )

        durable = (
            self.durable_count()
        )

        # ----------------------------------------------------
        # Barrier 失败
        # ----------------------------------------------------

        if durable != submitted:

            missing: list[
                str
            ] = []

            for (
                record_uid,
                version_hash,
            ) in self._scope_records:

                if not self.is_durable(
                    record_uid,
                    version_hash,
                ):

                    missing.append(
                        record_uid
                    )

            preview = (
                missing[:10]
            )

            raise RuntimeError(
                "pipeline barrier failed: "
                f"scope_token={token!r}, "
                f"submitted={submitted}, "
                f"durable={durable}, "
                f"missing={preview!r}"
            )

        return BarrierResult(
            scope_token=token,
            submitted_records=submitted,
            durable_records=durable,
            flushed_batches=len(
                flush_results
            ),
        )


    # ========================================================
    # Helpers
    # ========================================================


    def _require_active_scope(
        self,
    ) -> str:

        if self._scope_token is None:

            raise RuntimeError(
                "no active barrier scope"
            )

        return self._scope_token


    @staticmethod
    def _normalize_scope_token(
        scope_token: str,
    ) -> str:

        token = str(
            scope_token
        ).strip()

        if not token:

            raise ValueError(
                "scope_token cannot be empty"
            )

        return token