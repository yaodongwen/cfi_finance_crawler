from __future__ import annotations

import os
import uuid

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, Protocol

import pyarrow as pa
import pyarrow.parquet as pq

from crawl_framework.storage.parquet_writer import (
    ParquetFileInfo,
    file_sha256,
)
from crawl_framework.storage.partition import (
    PartitionKey,
)
from crawl_framework.storage.postgres import (
    CatalogDataFile,
    DataFileRecord,
)


@dataclass(
    frozen=True,
    slots=True,
)
class CompactionGroup:
    """
    一组可被同一个 replacement 文件替代的 active files。
    """

    site_id: str

    country: str

    dataset: str

    partition_date: date

    bucket: str

    files: tuple[CatalogDataFile, ...]


@dataclass(
    frozen=True,
    slots=True,
)
class CompactionPlan:
    """
    Generic compaction dry-run / apply 共享的计划对象。
    """

    groups: tuple[CompactionGroup, ...]

    source_file_count: int

    source_row_count: int


@dataclass(
    frozen=True,
    slots=True,
)
class CompactionResult:
    """
    单个 compaction group 生成的 replacement。
    """

    group: CompactionGroup

    output: ParquetFileInfo | None

    source_rows: int

    replacement_rows: int

    duplicate_rows_removed: int

    dry_run: bool


class CompactionUploader(Protocol):

    def upload_file(
        self,
        local_path: Path,
        relative_path: Path,
    ) -> str:
        ...


class CompactionCatalog(Protocol):

    def register_data_file(
        self,
        record: DataFileRecord,
    ) -> None:
        ...

    def mark_uploaded(
        self,
        *,
        file_path: str | Path,
        remote_path: str,
    ) -> None:
        ...

    def mark_superseded(
        self,
        *,
        file_path: str | Path,
    ) -> None:
        ...


def _parse_partition_date(
    value,
) -> date:

    if isinstance(
        value,
        date,
    ):

        return value

    return date.fromisoformat(
        str(
            value
        )
    )


def _parse_bucket(
    value: str,
) -> int:

    return int(
        str(
            value
        ),
        16,
    )


def build_compaction_plan(
    files: Iterable[CatalogDataFile],
    *,
    min_file_count: int = 2,
) -> CompactionPlan:
    """
    按 site/country/dataset/partition_date/bucket 分组。

    只接受 active + uploaded 文件作为 compaction 输入。
    """

    if min_file_count < 1:

        raise ValueError(
            "min_file_count must be >= 1"
        )

    grouped: dict[
        tuple[str, str, str, date, str],
        list[CatalogDataFile],
    ] = {}

    for item in files:

        if item.storage_status != "uploaded":

            continue

        if item.lifecycle_status != "active":

            continue

        key = (
            item.site_id,
            item.country,
            item.dataset,
            _parse_partition_date(
                item.partition_date
            ),
            item.bucket,
        )

        grouped.setdefault(
            key,
            [],
        ).append(
            item
        )

    groups = []

    for key, group_files in grouped.items():

        if len(
            group_files
        ) < min_file_count:

            continue

        (
            site_id,
            country,
            dataset,
            partition_date,
            bucket,
        ) = key

        groups.append(
            CompactionGroup(
                site_id=site_id,
                country=country,
                dataset=dataset,
                partition_date=partition_date,
                bucket=bucket,
                files=tuple(
                    group_files
                ),
            )
        )

    groups.sort(
        key=lambda group: (
            group.site_id,
            group.country,
            group.dataset,
            group.partition_date,
            group.bucket,
        )
    )

    return CompactionPlan(
        groups=tuple(
            groups
        ),
        source_file_count=sum(
            len(
                group.files
            )
            for group in groups
        ),
        source_row_count=sum(
            int(
                item.row_count
            )
            for group in groups
            for item in group.files
        ),
    )


def _table_sort_keys(
    table: pa.Table,
) -> list[tuple[str, str]]:

    keys = []

    if "event_time" in table.column_names:

        keys.append(
            (
                "event_time",
                "ascending",
            )
        )

    if "record_uid" in table.column_names:

        keys.append(
            (
                "record_uid",
                "ascending",
            )
        )

    return keys


def deduplicate_table(
    table: pa.Table,
    *,
    identity_column: str = "record_uid",
) -> pa.Table:
    """
    去重逻辑记录，保留排序后最后一次出现的版本。
    """

    if identity_column not in table.column_names:

        raise ValueError(
            f"missing identity column: {identity_column}"
        )

    if table.num_rows < 2:

        return table

    sort_keys = _table_sort_keys(
        table
    )

    if sort_keys:

        table = table.sort_by(
            sort_keys
        )

    rows = table.to_pylist()

    positions: dict[str, int] = {}

    for index, row in enumerate(
        rows
    ):

        positions[
            str(
                row[
                    identity_column
                ]
            )
        ] = index

    keep_positions = set(
        positions.values()
    )

    kept_rows = [
        row
        for index, row in enumerate(
            rows
        )
        if index in keep_positions
    ]

    return pa.Table.from_pylist(
        kept_rows,
        schema=table.schema,
    )


def _read_group_table(
    group: CompactionGroup,
) -> pa.Table:

    tables = []

    for item in group.files:

        if not item.remote_path:

            raise ValueError(
                "active uploaded file has empty remote_path: "
                f"{item.file_path}"
            )

        tables.append(
            pq.read_table(
                item.remote_path
            )
        )

    if not tables:

        raise ValueError(
            "cannot compact empty group"
        )

    if len(
        tables
    ) == 1:

        return tables[0]

    return pa.concat_tables(
        tables,
        promote_options="default",
    )


def _event_time_range(
    table: pa.Table,
):

    if (
        "event_time"
        not in table.column_names
        or
        table.num_rows < 1
    ):

        return (
            None,
            None,
        )

    values = [
        value.as_py()
        for value in table[
            "event_time"
        ]
        if value.as_py() is not None
    ]

    if not values:

        return (
            None,
            None,
        )

    return (
        min(
            values
        ),
        max(
            values
        ),
    )


def _schema_version(
    table: pa.Table,
) -> int:

    if "schema_version" not in table.column_names:

        raise ValueError(
            "compacted table missing schema_version"
        )

    values = {
        int(
            value.as_py()
        )
        for value in table[
            "schema_version"
        ]
        if value.as_py() is not None
    }

    if len(
        values
    ) != 1:

        raise ValueError(
            "compacted table must contain one schema_version"
        )

    return next(
        iter(
            values
        )
    )


def write_compacted_group(
    group: CompactionGroup,
    *,
    output_root: str | Path,
    dry_run: bool = False,
    compression: str = "zstd",
) -> CompactionResult:
    """
    读取 group 中 active files，生成同分区 replacement。
    """

    source_rows = sum(
        int(
            item.row_count
        )
        for item in group.files
    )

    table = _read_group_table(
        group
    )

    compacted = deduplicate_table(
        table
    )

    if dry_run:

        return CompactionResult(
            group=group,
            output=None,
            source_rows=source_rows,
            replacement_rows=(
                compacted.num_rows
            ),
            duplicate_rows_removed=(
                table.num_rows
                -
                compacted.num_rows
            ),
            dry_run=True,
        )

    partition = PartitionKey(
        site_id=group.site_id,
        country=group.country,
        dataset=group.dataset,
        partition_date=group.partition_date,
        bucket=_parse_bucket(
            group.bucket
        ),
    )

    relative_dir = (
        partition.relative_path()
    )

    target_dir = (
        Path(
            output_root
        )
        /
        relative_dir
    )

    target_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    final_path = (
        target_dir
        /
        f"part-compacted-{uuid.uuid4().hex}.parquet"
    )

    tmp_path = final_path.with_name(
        final_path.name
        +
        ".tmp"
    )

    try:

        pq.write_table(
            compacted,
            tmp_path,
            compression=compression,
        )

        with tmp_path.open(
            "rb"
        ) as handle:

            os.fsync(
                handle.fileno()
            )

        os.replace(
            tmp_path,
            final_path,
        )

    finally:

        if tmp_path.exists():

            tmp_path.unlink()

    (
        min_event_time,
        max_event_time,
    ) = _event_time_range(
        compacted
    )

    info = ParquetFileInfo(
        file_path=final_path,
        relative_path=(
            final_path.relative_to(
                output_root
            )
        ),
        partition=partition,
        row_count=(
            compacted.num_rows
        ),
        file_size=(
            final_path
            .stat()
            .st_size
        ),
        sha256=file_sha256(
            final_path
        ),
        min_event_time=min_event_time,
        max_event_time=max_event_time,
        schema_version=_schema_version(
            compacted
        ),
    )

    return CompactionResult(
        group=group,
        output=info,
        source_rows=source_rows,
        replacement_rows=(
            compacted.num_rows
        ),
        duplicate_rows_removed=(
            table.num_rows
            -
            compacted.num_rows
        ),
        dry_run=False,
    )


def publish_compaction_result(
    result: CompactionResult,
    *,
    catalog: CompactionCatalog,
    uploader: CompactionUploader,
    dry_run: bool = False,
) -> str | None:
    """
    安全发布顺序：

        upload replacement
        register replacement
        mark replacement uploaded
        mark sources superseded
    """

    if result.output is None:

        return None

    if dry_run:

        return None

    remote_path = uploader.upload_file(
        result.output.file_path,
        result.output.relative_path,
    )

    record = DataFileRecord.from_parquet_info(
        result.output
    )

    catalog.register_data_file(
        record
    )

    catalog.mark_uploaded(
        file_path=(
            result.output
            .relative_path
        ),
        remote_path=remote_path,
    )

    for item in result.group.files:

        catalog.mark_superseded(
            file_path=(
                item.file_path
            ),
        )

    return remote_path
