from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Literal


SeenDecision = Literal[
    "new",
    "unchanged",
    "updated",
]


@dataclass(
    frozen=True,
    slots=True,
)
class SeenResult:
    """
    SeenStore 对一条记录的判断结果。

    decision:
        new
            第一次见到 record_uid

        unchanged
            record_uid 已存在，并且 version_hash 没变化

        updated
            record_uid 已存在，但 version_hash 发生变化
    """

    record_uid: str

    version_hash: str

    decision: SeenDecision


class SeenStore:
    """
    SeenStore 抽象接口。

    后续可以实现：

        SQLiteSeenStore
        LMDBSeenStore
        PostgresSeenStore
        RocksDBSeenStore

    Pipeline 不应该知道具体使用哪种后端。
    """

    def inspect(
        self,
        record_uid: str,
        version_hash: str,
    ) -> SeenResult:
        raise NotImplementedError

    def commit(
        self,
        record_uid: str,
        version_hash: str,
    ) -> None:
        raise NotImplementedError

    def inspect_many(
        self,
        records: Iterable[
            tuple[str, str]
        ],
    ) -> list[SeenResult]:
        return [
            self.inspect(
                record_uid,
                version_hash,
            )
            for (
                record_uid,
                version_hash,
            )
            in records
        ]

    def commit_many(
        self,
        records: Iterable[
            tuple[str, str]
        ],
    ) -> int:
        count = 0

        for (
            record_uid,
            version_hash,
        ) in records:

            self.commit(
                record_uid,
                version_hash,
            )

            count += 1

        return count

    def close(
        self,
    ) -> None:
        pass


class SQLiteSeenStore(
    SeenStore
):
    """
    默认本地 SeenStore。

    SQLite 配置：

        WAL
        synchronous=NORMAL
        busy_timeout

    当前只保存：

        record_uid
        version_hash

    不保存正文。

    这意味着 storage spool / parquet 清理后，
    crawler 仍然知道历史记录是否已经处理过。
    """

    def __init__(
        self,
        path: str | Path,
        *,
        busy_timeout_ms: int = 30000,
    ) -> None:

        self.path = Path(
            path
        )

        self.path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.busy_timeout_ms = int(
            busy_timeout_ms
        )

        self._lock = threading.RLock()

        self._conn = sqlite3.connect(
            str(
                self.path
            ),
            timeout=(
                self.busy_timeout_ms
                / 1000
            ),
            check_same_thread=False,
        )

        self._configure()

        self._init_schema()


    def _configure(
        self,
    ) -> None:

        with self._conn:

            self._conn.execute(
                "PRAGMA journal_mode=WAL"
            )

            self._conn.execute(
                "PRAGMA synchronous=NORMAL"
            )

            self._conn.execute(
                "PRAGMA temp_store=MEMORY"
            )

            self._conn.execute(
                (
                    "PRAGMA busy_timeout="
                    f"{self.busy_timeout_ms}"
                )
            )


    def _init_schema(
        self,
    ) -> None:

        with self._conn:

            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS seen_records
                (
                    record_uid TEXT PRIMARY KEY,

                    version_hash TEXT NOT NULL,

                    first_seen_at TEXT
                        NOT NULL
                        DEFAULT CURRENT_TIMESTAMP,

                    updated_at TEXT
                        NOT NULL
                        DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_seen_records_updated_at
                ON seen_records(updated_at)
                """
            )


    @staticmethod
    def _clean_required(
        value: str,
        field_name: str,
    ) -> str:

        text = str(
            value
        ).strip()

        if not text:
            raise ValueError(
                f"{field_name} cannot be empty"
            )

        return text


    def inspect(
        self,
        record_uid: str,
        version_hash: str,
    ) -> SeenResult:

        record_uid = (
            self._clean_required(
                record_uid,
                "record_uid",
            )
        )

        version_hash = (
            self._clean_required(
                version_hash,
                "version_hash",
            )
        )

        with self._lock:

            row = self._conn.execute(
                """
                SELECT version_hash
                FROM seen_records
                WHERE record_uid = ?
                LIMIT 1
                """,
                (
                    record_uid,
                ),
            ).fetchone()

        if row is None:

            decision: SeenDecision = (
                "new"
            )

        elif row[0] == version_hash:

            decision = "unchanged"

        else:

            decision = "updated"

        return SeenResult(
            record_uid=record_uid,
            version_hash=version_hash,
            decision=decision,
        )


    def commit(
        self,
        record_uid: str,
        version_hash: str,
    ) -> None:

        record_uid = (
            self._clean_required(
                record_uid,
                "record_uid",
            )
        )

        version_hash = (
            self._clean_required(
                version_hash,
                "version_hash",
            )
        )

        with self._lock:

            with self._conn:

                self._conn.execute(
                    """
                    INSERT INTO seen_records
                    (
                        record_uid,
                        version_hash
                    )
                    VALUES
                    (
                        ?,
                        ?
                    )

                    ON CONFLICT(record_uid)
                    DO UPDATE SET

                        version_hash =
                            excluded.version_hash,

                        updated_at =
                            CURRENT_TIMESTAMP
                    """,
                    (
                        record_uid,
                        version_hash,
                    ),
                )


    def inspect_many(
        self,
        records: Iterable[
            tuple[str, str]
        ],
    ) -> list[SeenResult]:
        """
        第一版保持接口简单。

        后面我们可以把这里改成真正的批量 SQL，
        Pipeline 无需修改。
        """

        return super().inspect_many(
            records
        )


    def commit_many(
        self,
        records: Iterable[
            tuple[str, str]
        ],
    ) -> int:
        """
        批量提交。

        不做 per-record fsync。

        整批使用一个事务。
        """

        rows: list[
            tuple[str, str]
        ] = []

        for (
            record_uid,
            version_hash,
        ) in records:

            rows.append(
                (
                    self._clean_required(
                        record_uid,
                        "record_uid",
                    ),
                    self._clean_required(
                        version_hash,
                        "version_hash",
                    ),
                )
            )

        if not rows:
            return 0

        with self._lock:

            with self._conn:

                self._conn.executemany(
                    """
                    INSERT INTO seen_records
                    (
                        record_uid,
                        version_hash
                    )
                    VALUES
                    (
                        ?,
                        ?
                    )

                    ON CONFLICT(record_uid)
                    DO UPDATE SET

                        version_hash =
                            excluded.version_hash,

                        updated_at =
                            CURRENT_TIMESTAMP
                    """,
                    rows,
                )

        return len(
            rows
        )


    def contains(
        self,
        record_uid: str,
    ) -> bool:

        record_uid = (
            self._clean_required(
                record_uid,
                "record_uid",
            )
        )

        with self._lock:

            row = self._conn.execute(
                """
                SELECT 1
                FROM seen_records
                WHERE record_uid = ?
                LIMIT 1
                """,
                (
                    record_uid,
                ),
            ).fetchone()

        return (
            row is not None
        )


    def count(
        self,
    ) -> int:

        with self._lock:

            row = self._conn.execute(
                """
                SELECT COUNT(*)
                FROM seen_records
                """
            ).fetchone()

        return int(
            row[0]
        )


    def close(
        self,
    ) -> None:

        with self._lock:

            self._conn.close()


    def __enter__(
        self,
    ) -> "SQLiteSeenStore":

        return self


    def __exit__(
        self,
        exc_type,
        exc,
        tb,
    ) -> None:

        self.close()