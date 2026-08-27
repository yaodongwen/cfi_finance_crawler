from __future__ import annotations

import json
import time

from dataclasses import dataclass, field
from typing import Iterable

from crawl_framework.core.models import (
    CanonicalRecord,
)
from crawl_framework.storage.partition import (
    PartitionKey,
    Partitioner,
)


# ============================================================
# Config
# ============================================================


@dataclass(
    slots=True,
)
class BufferConfig:
    """
    Buffer flush 规则。

    任意条件满足即可 flush：

        row_count >= max_rows

        estimated_bytes >= target_bytes
            AND
        row_count >= min_rows

        age >= flush_seconds

    min_rows 主要防止非常小的 batch 因字节估算误差
    频繁 flush。

    timeout flush 不受 min_rows 限制。
    """

    target_bytes: int = (
        256
        * 1024
        * 1024
    )

    min_rows: int = 10_000

    max_rows: int = 500_000

    flush_seconds: float = 30.0


    def __post_init__(
        self,
    ) -> None:

        if self.target_bytes < 1:

            raise ValueError(
                "target_bytes must be >= 1"
            )

        if self.min_rows < 1:

            raise ValueError(
                "min_rows must be >= 1"
            )

        if (
            self.max_rows
            < self.min_rows
        ):

            raise ValueError(
                "max_rows must be >= min_rows"
            )

        if self.flush_seconds <= 0:

            raise ValueError(
                "flush_seconds must be > 0"
            )


# ============================================================
# Buffered partition
# ============================================================


@dataclass(
    slots=True,
)
class BufferedPartition:
    """
    一个 PartitionKey 对应的一批暂存记录。

    scope_tokens：

        当前 partition 中包含哪些 CrawlScope。

    例如：

        {
            "naver_finance:forum_post:005930",
            "naver_finance:forum_post:000660",
        }

    注意：

    scope_token 只用于 durability/barrier 控制，

    不会写入最终 Parquet。
    """

    key: PartitionKey

    records: list[
        CanonicalRecord
    ] = field(
        default_factory=list
    )

    estimated_bytes: int = 0

    scope_tokens: set[
        str
    ] = field(
        default_factory=set
    )

    created_monotonic: float = field(
        default_factory=time.monotonic
    )

    updated_monotonic: float = field(
        default_factory=time.monotonic
    )


    @property
    def row_count(
        self,
    ) -> int:

        return len(
            self.records
        )


    def age_seconds(
        self,
        *,
        now: float | None = None,
    ) -> float:

        if now is None:

            now = time.monotonic()

        return max(
            0.0,
            now
            - self.created_monotonic,
        )


    def contains_scope(
        self,
        scope_token: str,
    ) -> bool:
        """
        当前 partition 是否包含指定 scope。
        """

        token = str(
            scope_token
        ).strip()

        if not token:

            return False

        return (
            token
            in self.scope_tokens
        )


# ============================================================
# Flush batch
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class FlushBatch:
    """
    Buffer 交给 ParquetWriter 的逻辑 batch。

    一个 FlushBatch 对应一个 PartitionKey。

    scope_tokens：

        该 batch 内涉及的所有 scope。

    这个字段不会进入 Parquet，
    仅用于 Pipeline / Barrier 生命周期控制。
    """

    key: PartitionKey

    records: tuple[
        CanonicalRecord,
        ...
    ]

    estimated_bytes: int

    scope_tokens: frozenset[
        str
    ] = field(
        default_factory=frozenset
    )


    @property
    def row_count(
        self,
    ) -> int:

        return len(
            self.records
        )


    def contains_scope(
        self,
        scope_token: str,
    ) -> bool:

        token = str(
            scope_token
        ).strip()

        if not token:

            return False

        return (
            token
            in self.scope_tokens
        )


# ============================================================
# Size estimation
# ============================================================


def estimate_record_bytes(
    record: CanonicalRecord,
) -> int:
    """
    粗略估算 CanonicalRecord JSON 序列化后的字节数。

    这里只用于 flush 决策。

    最终 file_size 必须以实际 Parquet stat() 为准。
    """

    payload = (
        record.to_dict()
    )

    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(
            ",",
            ":",
        ),
        default=str,
    ).encode(
        "utf-8"
    )

    return len(
        encoded
    )


# ============================================================
# Scope token
# ============================================================


def normalize_scope_token(
    scope_token: str | None,
) -> str | None:
    """
    scope_token 标准化。

    None:
        表示该记录没有绑定具体 Runtime scope。

    这样旧代码：

        buffer.add(record)

    仍然完全兼容。
    """

    if scope_token is None:

        return None

    token = str(
        scope_token
    ).strip()

    if not token:

        raise ValueError(
            "scope_token cannot be empty"
        )

    return token


# ============================================================
# Record Buffer
# ============================================================


class RecordBuffer:
    """
    按 PartitionKey 聚合 CanonicalRecord。

    当前支持三类 flush：

        size flush
        timeout flush
        scope flush

    scope flush 的重要语义：

        flush_scope(token)

    只 flush 包含指定 scope_token 的 partition。

    不再像 flush_all() 一样影响整个 Buffer。

    但如果一个 partition 同时包含：

        scope A
        scope B

    flush scope A 时，

    整个 partition 都会一起 flush。

    这是刻意设计的：

        保持 Parquet partition 完整
        避免按 scope 产生大量小文件
    """

    def __init__(
        self,
        *,
        partitioner: Partitioner | None = None,
        config: BufferConfig | None = None,
    ) -> None:

        self.partitioner = (
            partitioner
            or Partitioner()
        )

        self.config = (
            config
            or BufferConfig()
        )

        self._buffers: dict[
            PartitionKey,
            BufferedPartition,
        ] = {}


    def __len__(
        self,
    ) -> int:
        """
        当前缓存中的总记录数。
        """

        return sum(
            partition.row_count
            for partition
            in self._buffers.values()
        )


    @property
    def partition_count(
        self,
    ) -> int:

        return len(
            self._buffers
        )


    @property
    def estimated_bytes(
        self,
    ) -> int:

        return sum(
            partition.estimated_bytes
            for partition
            in self._buffers.values()
        )


    # ========================================================
    # Add
    # ========================================================


    def add(
        self,
        record: CanonicalRecord,
        *,
        scope_token: str | None = None,
    ) -> FlushBatch | None:
        """
        添加一条记录。

        scope_token 可选。

        旧代码：

            add(record)

        新 Runtime：

            add(
                record,
                scope_token="..."
            )

        如果当前 partition 达到 size flush 条件：

            返回 FlushBatch

        否则：

            None
        """

        token = normalize_scope_token(
            scope_token
        )

        key = (
            self.partitioner
            .partition_for(
                record
            )
        )

        partition = (
            self._buffers.get(
                key
            )
        )

        if partition is None:

            partition = (
                BufferedPartition(
                    key=key
                )
            )

            self._buffers[
                key
            ] = partition

        size = estimate_record_bytes(
            record
        )

        partition.records.append(
            record
        )

        partition.estimated_bytes += (
            size
        )

        if token is not None:

            partition.scope_tokens.add(
                token
            )

        partition.updated_monotonic = (
            time.monotonic()
        )

        if self._should_flush_size(
            partition
        ):

            return (
                self._pop_partition(
                    key
                )
            )

        return None


    def add_many(
        self,
        records: Iterable[
            CanonicalRecord
        ],
        *,
        scope_token: str | None = None,
    ) -> list[
        FlushBatch
    ]:
        """
        批量添加同一 scope 的记录。

        如果 scope_token=None，
        行为与旧版本完全相同。
        """

        flushed: list[
            FlushBatch
        ] = []

        for record in records:

            batch = self.add(
                record,
                scope_token=scope_token,
            )

            if batch is not None:

                flushed.append(
                    batch
                )

        return flushed


    # ========================================================
    # Size flush
    # ========================================================


    def _should_flush_size(
        self,
        partition: BufferedPartition,
    ) -> bool:

        if (
            partition.row_count
            >= self.config.max_rows
        ):

            return True

        if (
            partition.row_count
            >= self.config.min_rows
            and
            partition.estimated_bytes
            >= self.config.target_bytes
        ):

            return True

        return False


    # ========================================================
    # Timeout flush
    # ========================================================


    def collect_expired(
        self,
        *,
        now: float | None = None,
    ) -> list[
        FlushBatch
    ]:
        """
        收集超过 flush_seconds 的 partition。

        timeout flush 不要求达到 min_rows。
        """

        if now is None:

            now = time.monotonic()

        expired_keys: list[
            PartitionKey
        ] = []

        for (
            key,
            partition,
        ) in self._buffers.items():

            if (
                partition.age_seconds(
                    now=now
                )
                >= self.config.flush_seconds
            ):

                expired_keys.append(
                    key
                )

        return [
            self._pop_partition(
                key
            )
            for key
            in expired_keys
        ]


    # ========================================================
    # Scope flush
    # ========================================================


    def partition_keys_for_scope(
        self,
        scope_token: str,
    ) -> list[
        PartitionKey
    ]:
        """
        返回当前 Buffer 中包含指定 scope 的 PartitionKey。
        """

        token = normalize_scope_token(
            scope_token
        )

        assert token is not None

        return [
            key
            for (
                key,
                partition,
            )
            in self._buffers.items()
            if partition.contains_scope(
                token
            )
        ]


    def has_scope(
        self,
        scope_token: str,
    ) -> bool:
        """
        Buffer 中是否还有该 scope 的未 flush 数据。
        """

        return bool(
            self.partition_keys_for_scope(
                scope_token
            )
        )


    def flush_scope(
        self,
        scope_token: str,
    ) -> list[
        FlushBatch
    ]:
        """
        强制 flush 包含指定 scope_token 的 partition。

        例如当前：

            partition A:
                scope-005930

            partition B:
                scope-000660

            partition C:
                scope-AAPL

        flush_scope(
            "scope-005930"
        )

        只会 flush partition A。

        ----------------------------

        如果：

            partition A:
                scope-005930
                scope-000660

        flush_scope("scope-005930")

        整个 partition A 都会 flush。

        因为 Parquet batch 仍以 PartitionKey 为边界，
        不按 scope 切小文件。
        """

        keys = (
            self.partition_keys_for_scope(
                scope_token
            )
        )

        return [
            self._pop_partition(
                key
            )
            for key
            in keys
        ]


    # ========================================================
    # Manual flush
    # ========================================================


    def flush_partition(
        self,
        key: PartitionKey,
    ) -> FlushBatch | None:
        """
        强制 flush 指定 partition。
        """

        if key not in self._buffers:

            return None

        return self._pop_partition(
            key
        )


    def flush_all(
        self,
    ) -> list[
        FlushBatch
    ]:
        """
        强制 flush 所有数据。

        主要用于：

            graceful shutdown
            CLI 最终退出
            integration test

        Scope Barrier 后续不应该再使用这个方法。
        """

        keys = list(
            self._buffers.keys()
        )

        return [
            self._pop_partition(
                key
            )
            for key
            in keys
        ]


    # ========================================================
    # Pop
    # ========================================================


    def _pop_partition(
        self,
        key: PartitionKey,
    ) -> FlushBatch:

        partition = (
            self._buffers.pop(
                key
            )
        )

        return FlushBatch(
            key=partition.key,
            records=tuple(
                partition.records
            ),
            estimated_bytes=(
                partition.estimated_bytes
            ),
            scope_tokens=frozenset(
                partition.scope_tokens
            ),
        )