from __future__ import annotations

import hashlib
import os
import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pyarrow as pa
import pyarrow.parquet as pq

from crawl_framework.core.models import (
    CanonicalRecord,
)

from crawl_framework.core.registry import (
    DatasetSchemaRegistry,
    DEFAULT_SCHEMA_REGISTRY,
)

from crawl_framework.storage.buffer import (
    FlushBatch,
)
from crawl_framework.storage.partition import (
    PartitionKey,
)


@dataclass(
    frozen=True,
    slots=True,
)
class ParquetFileInfo:
    """
    一个已经成功写入本地 warehouse 的 Parquet 文件。

    后续 PostgreSQL Catalog / Uploader 都使用这个对象。

    注意：

    这里只代表：

        本地 Parquet 已安全落盘

    不代表：

        已上传远端
        已注册 PostgreSQL
        SeenStore 已 commit
    """

    file_path: Path

    relative_path: Path

    partition: PartitionKey

    row_count: int

    file_size: int

    sha256: str

    min_event_time: datetime | None

    max_event_time: datetime | None

    schema_version: int


def file_sha256(
    path: str | Path,
    *,
    chunk_size: int = 1024 * 1024,
) -> str:
    """
    流式计算文件 SHA256。

    避免整个 Parquet 文件读入内存。
    """

    path = Path(
        path
    )

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


def extract_event_time_range(
    records: Iterable[
        CanonicalRecord
    ],
) -> tuple[
    datetime | None,
    datetime | None,
]:
    """
    提取一批记录 event_time 的最小值和最大值。

    没有 event_time 的记录忽略。

    如果整批都没有 event_time：

        (None, None)
    """

    values = [
        record.event_time
        for record in records
        if record.event_time is not None
    ]

    if not values:
        return (
            None,
            None,
        )

    return (
        min(values),
        max(values),
    )


def records_to_arrow_table(
    records: Iterable[
        CanonicalRecord
    ],
    *,
    registry: DatasetSchemaRegistry | None = None,
) -> pa.Table:
    """
    CanonicalRecord -> PyArrow Table。

    不再让 PyArrow 自动推断物理 Schema。

    统一流程：

        CanonicalRecord
            ↓
        DatasetSchemaRegistry
            ↓
        固定 pa.Schema
            ↓
        pa.Table.from_pylist(..., schema=schema)

    这样即使整批：

        event_time = None
        instrument_id = None

    物理类型仍然保持：

        timestamp[us, UTC]
        string

    不会被推断成 null 类型。
    """

    records = list(
        records
    )

    if not records:
        raise ValueError(
            "cannot create parquet "
            "from empty records"
        )

    if registry is None:
        registry = (
            DEFAULT_SCHEMA_REGISTRY
        )

    # --------------------------------------------------------
    # 一个 FlushBatch 原则上必须只有一个 dataset。
    # --------------------------------------------------------

    datasets = {
        record.dataset
        for record in records
    }

    if len(
        datasets
    ) != 1:
        raise ValueError(
            "multiple datasets inside "
            "one parquet batch: "
            f"{sorted(datasets)!r}"
        )

    dataset = next(
        iter(
            datasets
        )
    )

    dataset_schema = (
        registry.get(
            dataset
        )
    )

    rows: list[
        dict
    ] = []

    for record in records:

        raw = record.to_dict()

        payload = raw.pop(
            "payload",
            {},
        )

        relations = raw.pop(
            "relations",
            [],
        )

        # ----------------------------------------------------
        # CanonicalRecord.to_dict() 当前把 datetime 转成
        # ISO string。
        #
        # Parquet physical schema 需要真正 datetime。
        # 因此这里直接重新使用 record 上的 datetime。
        # ----------------------------------------------------

        raw[
            "event_time"
        ] = record.event_time

        raw[
            "updated_at"
        ] = record.updated_at

        raw[
            "crawled_at"
        ] = record.crawled_at

        # ----------------------------------------------------
        # 动态字段统一 JSON 化。
        # ----------------------------------------------------

        raw[
            "payload_json"
        ] = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(
                ",",
                ":",
            ),
            sort_keys=True,
            default=str,
        )

        raw[
            "relations_json"
        ] = json.dumps(
            relations,
            ensure_ascii=False,
            separators=(
                ",",
                ":",
            ),
            sort_keys=True,
            default=str,
        )

        rows.append(
            raw
        )

    return pa.Table.from_pylist(
        rows,
        schema=(
            dataset_schema
            .arrow_schema
        ),
    )


class ParquetWriter:
    """
    FlushBatch
        ↓
    本地 Parquet

    文件目录：

        warehouse/
          site=...
          country=...
          dataset=...
          year=...
          month=...
          day=...
          bucket=...
          part-<uuid>.parquet

    文件写入流程：

        .tmp
          ↓
        pyarrow write
          ↓
        fsync
          ↓
        os.replace
          ↓
        sha256
          ↓
        ParquetFileInfo
    """

    def __init__(
        self,
        root: str | Path,
        *,
        compression: str = "zstd",
        use_dictionary: bool = True,
        write_statistics: bool = True,
        registry: DatasetSchemaRegistry | None = None,
    ) -> None:

        self.root = Path(
            root
        )

        self.root.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.compression = str(
            compression
        ).strip()

        self.use_dictionary = bool(
            use_dictionary
        )

        self.write_statistics = bool(
            write_statistics
        )

        self.registry = (
            registry
            or DEFAULT_SCHEMA_REGISTRY
        )


    def _make_filename(
        self,
    ) -> str:
        """
        使用 UUID4 保证并发写入时文件名不冲突。
        """

        token = uuid.uuid4().hex

        return (
            f"part-{token}.parquet"
        )


    def write_batch(
        self,
        batch: FlushBatch,
    ) -> ParquetFileInfo:
        """
        写一个 FlushBatch。

        成功：
            返回 ParquetFileInfo

        失败：
            删除 tmp
            抛异常
        """

        if batch.row_count < 1:
            raise ValueError(
                "cannot write empty batch"
            )

        relative_dir = (
            batch.key.relative_path()
        )

        target_dir = (
            self.root
            / relative_dir
        )

        target_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        filename = (
            self._make_filename()
        )

        final_path = (
            target_dir
            / filename
        )

        tmp_path = final_path.with_name(
            final_path.name
            + ".tmp"
        )

        records = list(
            batch.records
        )

        table = (
            records_to_arrow_table(
                records,
                registry=self.registry,
            )
        )

        try:

            pq.write_table(
                table,
                tmp_path,
                compression=(
                    self.compression
                ),
                use_dictionary=(
                    self.use_dictionary
                ),
                write_statistics=(
                    self.write_statistics
                ),
            )

            # ------------------------------------------------
            # 文件内容已经由 pyarrow 写完。
            #
            # 再显式 fsync 一次，尽量保证 crash safety。
            # ------------------------------------------------

            with tmp_path.open(
                "rb"
            ) as file:

                os.fsync(
                    file.fileno()
                )

            # ------------------------------------------------
            # 原子 rename
            # ------------------------------------------------

            os.replace(
                tmp_path,
                final_path,
            )

        finally:

            if tmp_path.exists():

                try:
                    tmp_path.unlink()

                except OSError:
                    pass

        row_count = (
            table.num_rows
        )

        file_size = (
            final_path.stat().st_size
        )

        digest = file_sha256(
            final_path
        )

        (
            min_event_time,
            max_event_time,
        ) = extract_event_time_range(
            records
        )

        schema_versions = {
            record.schema_version
            for record in records
        }

        if len(
            schema_versions
        ) != 1:
            raise ValueError(
                "multiple schema_version values "
                "inside one FlushBatch"
            )

        schema_version = (
            next(
                iter(
                    schema_versions
                )
            )
        )

        relative_path = (
            final_path.relative_to(
                self.root
            )
        )

        return ParquetFileInfo(
            file_path=final_path,
            relative_path=relative_path,
            partition=batch.key,
            row_count=row_count,
            file_size=file_size,
            sha256=digest,
            min_event_time=min_event_time,
            max_event_time=max_event_time,
            schema_version=schema_version,
        )


    def write_batches(
        self,
        batches: Iterable[
            FlushBatch
        ],
    ) -> list[
        ParquetFileInfo
    ]:
        """
        顺序写多个 batch。

        后面 Storage Worker 层负责并发；
        Writer 本身保持简单。
        """

        return [
            self.write_batch(
                batch
            )
            for batch in batches
        ]