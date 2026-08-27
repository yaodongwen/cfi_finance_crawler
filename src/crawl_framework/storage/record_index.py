from __future__ import annotations

import json
import os

from dataclasses import (
    asdict,
    dataclass,
)
from pathlib import Path
from typing import Iterable

from crawl_framework.core.models import (
    CanonicalRecord,
)


# ============================================================
# Record index row
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class RecordIndexEntry:
    """
    一个 durable record 的最小恢复信息。

    record_uid:
        逻辑记录唯一 ID。

    version_hash:
        当前版本内容 hash。

    RecoveryManager 后续只需要这两个字段就可以：

        SeenStore.commit_many(...)
    """

    record_uid: str

    version_hash: str


    def __post_init__(
        self,
    ) -> None:

        if not self.record_uid.strip():

            raise ValueError(
                "record_uid cannot be empty"
            )

        if not self.version_hash.strip():

            raise ValueError(
                "version_hash cannot be empty"
            )


# ============================================================
# Record index file
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class RecordIndexInfo:
    """
    一份 sidecar record index 的元信息。
    """

    file_path: Path

    row_count: int

    file_size: int


# ============================================================
# Helpers
# ============================================================


def record_to_index_entry(
    record: CanonicalRecord,
) -> RecordIndexEntry:

    return RecordIndexEntry(
        record_uid=(
            record.record_uid
        ),
        version_hash=(
            record.version_hash
        ),
    )


def records_to_index_entries(
    records: Iterable[
        CanonicalRecord
    ],
) -> list[
    RecordIndexEntry
]:

    return [
        record_to_index_entry(
            record
        )
        for record
        in records
    ]


def default_index_path(
    parquet_path: str | Path,
) -> Path:
    """
    Parquet:

        part-abc.parquet

    Sidecar:

        part-abc.records.jsonl

    两个文件放在同一目录。
    """

    parquet_path = Path(
        parquet_path
    )

    return parquet_path.with_name(
        parquet_path.stem
        + ".records.jsonl"
    )


# ============================================================
# Writer
# ============================================================


class RecordIndexWriter:
    """
    Crash-safe record index writer。

    写入流程：

        .tmp
          ↓
        flush
          ↓
        fsync
          ↓
        os.replace

    JSONL 每行：

        {
            "record_uid": "...",
            "version_hash": "..."
        }

    这里不用 Parquet，
    因为 sidecar 的目标不是分析，而是 recovery。
    """

    def write(
        self,
        parquet_path: str | Path,
        records: Iterable[
            CanonicalRecord
        ],
    ) -> RecordIndexInfo:

        entries = (
            records_to_index_entries(
                records
            )
        )

        if not entries:

            raise ValueError(
                "cannot write empty record index"
            )

        path = default_index_path(
            parquet_path
        )

        path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        tmp = path.with_name(
            path.name
            + ".tmp"
        )

        try:

            with tmp.open(
                "w",
                encoding="utf-8",
            ) as file:

                for entry in entries:

                    json.dump(
                        asdict(
                            entry
                        ),
                        file,
                        ensure_ascii=False,
                        separators=(
                            ",",
                            ":",
                        ),
                        sort_keys=True,
                    )

                    file.write(
                        "\n"
                    )

                file.flush()

                os.fsync(
                    file.fileno()
                )

            os.replace(
                tmp,
                path,
            )

        finally:

            if tmp.exists():

                try:

                    tmp.unlink()

                except OSError:
                    pass

        stat = path.stat()

        return RecordIndexInfo(
            file_path=path,
            row_count=len(
                entries
            ),
            file_size=stat.st_size,
        )


# ============================================================
# Reader
# ============================================================


class RecordIndexReader:
    """
    Record index reader。
    """

    def read(
        self,
        path: str | Path,
    ) -> list[
        RecordIndexEntry
    ]:

        path = Path(
            path
        )

        if not path.exists():

            raise FileNotFoundError(
                path
            )

        entries: list[
            RecordIndexEntry
        ] = []

        with path.open(
            "r",
            encoding="utf-8",
        ) as file:

            for line_number, line in enumerate(
                file,
                start=1,
            ):

                line = line.strip()

                if not line:

                    continue

                try:

                    obj = json.loads(
                        line
                    )

                except json.JSONDecodeError as exc:

                    raise RuntimeError(
                        "invalid record index json: "
                        f"path={path}, "
                        f"line={line_number}"
                    ) from exc

                try:

                    entry = RecordIndexEntry(
                        record_uid=(
                            obj[
                                "record_uid"
                            ]
                        ),
                        version_hash=(
                            obj[
                                "version_hash"
                            ]
                        ),
                    )

                except (
                    KeyError,
                    TypeError,
                    ValueError,
                ) as exc:

                    raise RuntimeError(
                        "invalid record index entry: "
                        f"path={path}, "
                        f"line={line_number}"
                    ) from exc

                entries.append(
                    entry
                )

        return entries


    def read_pairs(
        self,
        path: str | Path,
    ) -> list[
        tuple[
            str,
            str,
        ]
    ]:
        """
        直接返回 SeenStore.commit_many() 所需要的格式。
        """

        return [
            (
                entry.record_uid,
                entry.version_hash,
            )
            for entry
            in self.read(
                path
            )
        ]


# ============================================================
# Store facade
# ============================================================


class RecordIndexStore:
    """
    Writer + Reader 的统一 facade。
    """

    def __init__(
        self,
    ) -> None:

        self.writer = (
            RecordIndexWriter()
        )

        self.reader = (
            RecordIndexReader()
        )


    def write_for_parquet(
        self,
        parquet_path: str | Path,
        records: Iterable[
            CanonicalRecord
        ],
    ) -> RecordIndexInfo:

        return self.writer.write(
            parquet_path,
            records,
        )


    def read_for_parquet(
        self,
        parquet_path: str | Path,
    ) -> list[
        RecordIndexEntry
    ]:

        return self.reader.read(
            default_index_path(
                parquet_path
            )
        )


    def read_pairs_for_parquet(
        self,
        parquet_path: str | Path,
    ) -> list[
        tuple[
            str,
            str,
        ]
    ]:

        return self.reader.read_pairs(
            default_index_path(
                parquet_path
            )
        )


    def exists_for_parquet(
        self,
        parquet_path: str | Path,
    ) -> bool:

        return default_index_path(
            parquet_path
        ).exists()


    def delete_for_parquet(
        self,
        parquet_path: str | Path,
    ) -> bool:
        """
        删除 sidecar。

        注意：

        当前只是提供能力。

        真正什么时候允许删除，
        后续由 Cleaner / RecoveryManager 决定。
        """

        path = default_index_path(
            parquet_path
        )

        if not path.exists():

            return False

        path.unlink()

        return True