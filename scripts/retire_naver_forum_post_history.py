from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Any

from crawl_framework.config import (
    load_default_config,
)

from crawl_framework.storage.postgres_connection import (
    make_config_postgres_connection_factory,
)


SITE_ID = "naver_finance"
DATASET = "forum_post"

OLD_IDS = (
    1,
    2,
    3,
    4,
    5,
    6,
)

NEW_IDS = (
    7,
    8,
    9,
    10,
)

EXPECTED_NEW_ROWS = 123
EXPECTED_NEW_FILES = 4
EXPECTED_OLD_FILES = 6


@dataclass(
    frozen=True,
    slots=True,
)
class CatalogRow:

    id: int
    file_path: str
    row_count: int
    storage_status: str
    remote_path: str | None
    sha256: str
    file_size: int


def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description=(
            "Safely retire superseded historical "
            "Naver Finance forum_post catalog files."
        )
    )

    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Actually mark old catalog rows as "
            "superseded. Without this flag the "
            "script performs dry-run validation only."
        ),
    )

    return parser.parse_args()


def load_rows(
    connection,
    ids: tuple[int, ...],
) -> list[CatalogRow]:

    placeholders = ", ".join(
        ["%s"] * len(ids)
    )

    sql = f"""
        SELECT
            id,
            file_path,
            row_count,
            storage_status,
            remote_path,
            sha256,
            file_size
        FROM marketdata.data_files
        WHERE
            id IN ({placeholders})
        ORDER BY id ASC
    """

    with connection.cursor() as cur:

        cur.execute(
            sql,
            ids,
        )

        raw_rows = cur.fetchall()

    return [
        CatalogRow(
            id=int(row[0]),
            file_path=str(row[1]),
            row_count=int(row[2]),
            storage_status=str(row[3]),
            remote_path=(
                str(row[4])
                if row[4] is not None
                else None
            ),
            sha256=str(row[5]),
            file_size=int(row[6]),
        )
        for row in raw_rows
    ]


def load_all_forum_rows(
    connection,
) -> list[CatalogRow]:

    with connection.cursor() as cur:

        cur.execute(
            """
            SELECT
                id,
                file_path,
                row_count,
                storage_status,
                remote_path,
                sha256,
                file_size
            FROM marketdata.data_files
            WHERE
                site_id = %s
                AND dataset = %s
            ORDER BY id ASC
            """,
            (
                SITE_ID,
                DATASET,
            ),
        )

        rows = cur.fetchall()

    return [
        CatalogRow(
            id=int(row[0]),
            file_path=str(row[1]),
            row_count=int(row[2]),
            storage_status=str(row[3]),
            remote_path=(
                str(row[4])
                if row[4] is not None
                else None
            ),
            sha256=str(row[5]),
            file_size=int(row[6]),
        )
        for row in rows
    ]


def print_rows(
    title: str,
    rows: list[CatalogRow],
) -> None:

    print()
    print(
        "=" * 70
    )

    print(
        title
    )

    print(
        "=" * 70
    )

    for row in rows:

        print()

        print(
            "id =",
            row.id,
        )

        print(
            "rows =",
            row.row_count,
        )

        print(
            "status =",
            row.storage_status,
        )

        print(
            "file_path =",
            row.file_path,
        )

        print(
            "remote_path =",
            row.remote_path,
        )

        print(
            "sha256 =",
            row.sha256,
        )


def validate_old_rows(
    rows: list[CatalogRow],
) -> None:

    print()
    print(
        "===== VALIDATE OLD SET ====="
    )

    actual_ids = {
        row.id
        for row in rows
    }

    expected_ids = set(
        OLD_IDS
    )

    print(
        "expected ids =",
        sorted(
            expected_ids
        ),
    )

    print(
        "actual ids =",
        sorted(
            actual_ids
        ),
    )

    print(
        "files =",
        len(
            rows
        ),
    )

    print(
        "physical rows =",
        sum(
            row.row_count
            for row in rows
        ),
    )

    if (
        actual_ids
        !=
        expected_ids
    ):

        raise RuntimeError(
            "old catalog IDs do not "
            "match expected ids 1..6"
        )

    if (
        len(
            rows
        )
        !=
        EXPECTED_OLD_FILES
    ):

        raise RuntimeError(
            "unexpected old catalog "
            "file count"
        )

    allowed_statuses = {
        "local",
        "uploaded",
        "superseded",
    }

    bad_statuses = [
        (
            row.id,
            row.storage_status,
        )
        for row in rows
        if (
            row.storage_status
            not in allowed_statuses
        )
    ]

    if bad_statuses:

        raise RuntimeError(
            "unexpected old catalog "
            "storage_status: "
            f"{bad_statuses}"
        )


def validate_new_rows(
    rows: list[CatalogRow],
) -> None:

    print()
    print(
        "===== VALIDATE NEW SET ====="
    )

    actual_ids = {
        row.id
        for row in rows
    }

    expected_ids = set(
        NEW_IDS
    )

    total_rows = sum(
        row.row_count
        for row in rows
    )

    print(
        "expected ids =",
        sorted(
            expected_ids
        ),
    )

    print(
        "actual ids =",
        sorted(
            actual_ids
        ),
    )

    print(
        "files =",
        len(
            rows
        ),
    )

    print(
        "logical rows =",
        total_rows,
    )

    if (
        actual_ids
        !=
        expected_ids
    ):

        raise RuntimeError(
            "new compacted catalog IDs "
            "do not match 7..10"
        )

    if (
        len(
            rows
        )
        !=
        EXPECTED_NEW_FILES
    ):

        raise RuntimeError(
            "expected exactly 4 "
            "compacted files"
        )

    if (
        total_rows
        !=
        EXPECTED_NEW_ROWS
    ):

        raise RuntimeError(
            "new compacted row count "
            "is not 123"
        )

    for row in rows:

        if (
            row.storage_status
            !=
            "uploaded"
        ):

            raise RuntimeError(
                "new compacted file "
                "is not uploaded: "
                f"id={row.id}, "
                f"status={row.storage_status}"
            )

        if not row.remote_path:

            raise RuntimeError(
                "new compacted file "
                "has no remote_path: "
                f"id={row.id}"
            )

        if (
            "part-compacted-"
            not in row.file_path
        ):

            raise RuntimeError(
                "new catalog row does not "
                "look like compacted output: "
                f"id={row.id}"
            )


def validate_no_path_overlap(
    old_rows: list[CatalogRow],
    new_rows: list[CatalogRow],
) -> None:

    old_paths = {
        row.file_path
        for row in old_rows
    }

    new_paths = {
        row.file_path
        for row in new_rows
    }

    overlap = (
        old_paths
        &
        new_paths
    )

    print()
    print(
        "===== PATH COLLISION CHECK ====="
    )

    print(
        "overlap =",
        len(
            overlap
        ),
    )

    if overlap:

        raise RuntimeError(
            "old/new catalog file_path "
            "collision detected: "
            f"{sorted(overlap)}"
        )


def print_effective_uploaded_set(
    connection,
) -> None:
    """
    打印当前真正有效的数据集。

    有效文件必须同时满足：

        storage_status = 'uploaded'
        lifecycle_status = 'active'

    注意：

        uploaded 只表示物理上已上传；
        superseded 文件即使仍在 NAS，
        也不能进入有效数据集。
    """

    with connection.cursor() as cur:

        cur.execute(
            """
            SELECT
                id,
                row_count,
                storage_status,
                lifecycle_status,
                file_path
            FROM marketdata.data_files
            WHERE
                site_id = %s
                AND dataset = %s
                AND storage_status = 'uploaded'
                AND lifecycle_status = 'active'
            ORDER BY id ASC
            """,
            (
                SITE_ID,
                DATASET,
            ),
        )

        rows = cur.fetchall()

    print()
    print(
        "========================================"
    )

    print(
        "EFFECTIVE ACTIVE UPLOADED DATASET"
    )

    print(
        "========================================"
    )

    total_rows = 0

    for row in rows:

        (
            file_id,
            row_count,
            storage_status,
            lifecycle_status,
            file_path,
        ) = row

        total_rows += int(
            row_count
        )

        print()

        print(
            "id =",
            file_id,
        )

        print(
            "rows =",
            row_count,
        )

        print(
            "storage_status =",
            storage_status,
        )

        print(
            "lifecycle_status =",
            lifecycle_status,
        )

        print(
            "path =",
            file_path,
        )

    print()

    print(
        "active uploaded files =",
        len(
            rows
        ),
    )

    print(
        "active uploaded rows =",
        total_rows,
    )

    if (
        len(
            rows
        )
        !=
        EXPECTED_NEW_FILES
    ):

        raise RuntimeError(
            "effective active uploaded "
            "dataset does not contain "
            "exactly 4 files"
        )

    ids = {
        int(
            row[0]
        )
        for row in rows
    }

    if (
        ids
        !=
        set(
            NEW_IDS
        )
    ):

        raise RuntimeError(
            "effective active uploaded "
            "dataset is not exactly "
            "ids 7..10"
        )

    if (
        total_rows
        !=
        EXPECTED_NEW_ROWS
    ):

        raise RuntimeError(
            "effective active uploaded "
            "row count is not 123"
        )

def apply_retirement(
    connection,
) -> None:
    """
    将旧历史文件逻辑退役。

    重要：

    本函数只修改 lifecycle_status。

    不修改：

        storage_status
        remote_path
        file_path
        sha256

    正确状态：

        id=1
            storage_status = local
            lifecycle_status = superseded

        id=2..6
            storage_status = uploaded
            lifecycle_status = superseded

    本操作允许安全重复执行。
    """

    print()
    print(
        "===== APPLY RETIREMENT ====="
    )

    # --------------------------------------------------------
    # Lock target rows.
    # --------------------------------------------------------

    with connection.cursor() as cur:

        cur.execute(
            """
            SELECT
                id,
                storage_status,
                lifecycle_status
            FROM marketdata.data_files
            WHERE
                id IN (1, 2, 3, 4, 5, 6)
            ORDER BY id
            FOR UPDATE
            """
        )

        locked = (
            cur.fetchall()
        )

        if (
            len(
                locked
            )
            !=
            EXPECTED_OLD_FILES
        ):

            raise RuntimeError(
                "unable to lock all "
                "old catalog rows"
            )

        # ====================================================
        # Validate physical storage state BEFORE changing
        # lifecycle.
        # ====================================================

        expected_storage = {
            1:
                "local",

            2:
                "uploaded",

            3:
                "uploaded",

            4:
                "uploaded",

            5:
                "uploaded",

            6:
                "uploaded",
        }

        bad_storage = []

        for (
            file_id,
            storage_status,
            lifecycle_status,
        ) in locked:

            file_id = int(
                file_id
            )

            expected = (
                expected_storage[
                    file_id
                ]
            )

            if (
                str(
                    storage_status
                )
                !=
                expected
            ):

                bad_storage.append(
                    (
                        file_id,
                        storage_status,
                        expected,
                    )
                )

        if bad_storage:

            raise RuntimeError(
                "old catalog physical "
                "storage state is unexpected: "
                f"{bad_storage}"
            )

        # ====================================================
        # Logical retirement ONLY.
        #
        # Already-superseded rows remain unchanged.
        #
        # Therefore this is restart-safe / idempotent.
        # ====================================================

        cur.execute(
            """
            UPDATE marketdata.data_files
            SET
                lifecycle_status = 'superseded',
                updated_at = NOW()
            WHERE
                id IN (1, 2, 3, 4, 5, 6)
                AND lifecycle_status
                    <> 'superseded'
            """
        )

        changed = (
            cur.rowcount
        )

    print(
        "rows changed =",
        changed,
    )

def verify_after_apply(
    connection,
) -> None:
    """
    验证 retirement 后的双状态模型。

    old generation:

        id=1
            local + superseded

        id=2..6
            uploaded + superseded

    current generation:

        id=7..10
            uploaded + active
    """

    old_rows = load_rows(
        connection,
        OLD_IDS,
    )

    new_rows = load_rows(
        connection,
        NEW_IDS,
    )

    print_rows(
        "OLD SET AFTER RETIREMENT",
        old_rows,
    )

    print_rows(
        "NEW SET AFTER RETIREMENT",
        new_rows,
    )

    # ========================================================
    # Read lifecycle states directly.
    # ========================================================

    with connection.cursor() as cur:

        cur.execute(
            """
            SELECT
                id,
                storage_status,
                lifecycle_status,
                remote_path
            FROM marketdata.data_files
            WHERE
                id IN (
                    1, 2, 3, 4, 5,
                    6, 7, 8, 9, 10
                )
            ORDER BY id ASC
            """
        )

        state_rows = (
            cur.fetchall()
        )

    if (
        len(
            state_rows
        )
        !=
        10
    ):

        raise RuntimeError(
            "expected exactly 10 "
            "forum_post catalog rows"
        )

    state_by_id = {
        int(
            row[0]
        ):
            (
                str(
                    row[1]
                ),
                str(
                    row[2]
                ),
                (
                    str(
                        row[3]
                    )
                    if row[3] is not None
                    else None
                ),
            )

        for row in state_rows
    }

    print()
    print(
        "===== STORAGE / LIFECYCLE STATE ====="
    )

    for file_id in sorted(
        state_by_id
    ):

        (
            storage_status,
            lifecycle_status,
            remote_path,
        ) = state_by_id[
            file_id
        ]

        print()

        print(
            "id =",
            file_id,
        )

        print(
            "storage_status =",
            storage_status,
        )

        print(
            "lifecycle_status =",
            lifecycle_status,
        )

        print(
            "remote_path =",
            remote_path,
        )

    # ========================================================
    # id=1
    # ========================================================

    (
        storage_status,
        lifecycle_status,
        remote_path,
    ) = state_by_id[
        1
    ]

    if (
        storage_status
        !=
        "local"
    ):

        raise RuntimeError(
            "id=1 storage_status "
            "must remain local"
        )

    if (
        lifecycle_status
        !=
        "superseded"
    ):

        raise RuntimeError(
            "id=1 lifecycle_status "
            "must be superseded"
        )

    if (
        remote_path
        is not None
    ):

        raise RuntimeError(
            "id=1 unexpectedly has "
            "remote_path"
        )

    # ========================================================
    # ids=2..6
    # ========================================================

    for file_id in (
        2,
        3,
        4,
        5,
        6,
    ):

        (
            storage_status,
            lifecycle_status,
            remote_path,
        ) = state_by_id[
            file_id
        ]

        if (
            storage_status
            !=
            "uploaded"
        ):

            raise RuntimeError(
                f"id={file_id} "
                "storage_status must "
                "remain uploaded"
            )

        if (
            lifecycle_status
            !=
            "superseded"
        ):

            raise RuntimeError(
                f"id={file_id} "
                "lifecycle_status must "
                "be superseded"
            )

        if not remote_path:

            raise RuntimeError(
                f"id={file_id} "
                "must retain remote_path"
            )

    # ========================================================
    # ids=7..10
    # ========================================================

    total_new_rows = 0

    rows_by_id = {
        row.id:
            row
        for row in new_rows
    }

    for file_id in (
        7,
        8,
        9,
        10,
    ):

        (
            storage_status,
            lifecycle_status,
            remote_path,
        ) = state_by_id[
            file_id
        ]

        if (
            storage_status
            !=
            "uploaded"
        ):

            raise RuntimeError(
                f"id={file_id} "
                "storage_status must "
                "remain uploaded"
            )

        if (
            lifecycle_status
            !=
            "active"
        ):

            raise RuntimeError(
                f"id={file_id} "
                "lifecycle_status must "
                "remain active"
            )

        if not remote_path:

            raise RuntimeError(
                f"id={file_id} "
                "must retain remote_path"
            )

        total_new_rows += (
            rows_by_id[
                file_id
            ]
            .row_count
        )

    if (
        total_new_rows
        !=
        EXPECTED_NEW_ROWS
    ):

        raise RuntimeError(
            "active compacted dataset "
            "does not contain exactly "
            "123 rows"
        )

    # ========================================================
    # Effective reader semantics
    # ========================================================

    print_effective_uploaded_set(
        connection
    )

def main() -> int:

    args = parse_args()

    mode = (
        "APPLY"
        if args.apply
        else "DRY-RUN"
    )

    print(
        "mode =",
        mode,
    )

    config = (
        load_default_config()
    )

    factory = (
        make_config_postgres_connection_factory(
            config
        )
    )

    connection = factory()

    try:

        old_rows = load_rows(
            connection,
            OLD_IDS,
        )

        new_rows = load_rows(
            connection,
            NEW_IDS,
        )

        print_rows(
            "OLD CATALOG SET",
            old_rows,
        )

        print_rows(
            "NEW COMPACTED SET",
            new_rows,
        )

        # ====================================================
        # Safety validation
        # ====================================================

        validate_old_rows(
            old_rows
        )

        validate_new_rows(
            new_rows
        )

        validate_no_path_overlap(
            old_rows,
            new_rows,
        )

        print()
        print(
            "========================================"
        )

        print(
            "RETIREMENT PLAN"
        )

        print(
            "========================================"
        )

        print(
            "old ids:"
        )

        print(
            "  1, 2, 3, 4, 5, 6"
        )

        print()

        print(
            "old lifecycle target:"
        )

        print(
            "  lifecycle_status = superseded"
        )

        print()

        print(
            "new ids remain:"
        )

        print(
            "  7, 8, 9, 10"
        )

        print()

        print(
            "new status remains:"
        )

        print(
            "  storage_status = uploaded"
        )

        print(
            "  lifecycle_status = active"
        )

        print()

        print(
            "NAS files deleted:"
        )

        print(
            "  NO"
        )

        print()

        print(
            "catalog rows deleted:"
        )

        print(
            "  NO"
        )

        # ====================================================
        # Dry-run
        # ====================================================

        if not args.apply:

            print()
            print(
                "========================================"
            )

            print(
                "RETIREMENT DRY-RUN SUCCESS"
            )

            print(
                "========================================"
            )

            print(
                "No PostgreSQL rows changed."
            )

            print(
                "No NAS files changed."
            )

            return 0

        # ====================================================
        # Transaction
        # ====================================================

        try:

            apply_retirement(
                connection
            )

            # ------------------------------------------------
            # Verify inside same transaction BEFORE COMMIT.
            # ------------------------------------------------

            verify_after_apply(
                connection
            )

            connection.commit()

        except Exception:

            connection.rollback()

            print()
            print(
                "RETIREMENT FAILED"
            )

            print(
                "Transaction rolled back."
            )

            raise

        # ====================================================
        # Verify again AFTER COMMIT
        # ====================================================

        print()
        print(
            "===== POST-COMMIT VERIFICATION ====="
        )

        verify_after_apply(
            connection
        )

        print()
        print(
            "========================================"
        )

        print(
            "RETIREMENT SUCCESS"
        )

        print(
            "========================================"
        )

        print(
            "Old id=1:"
        )

        print(
            "  storage_status = local"
        )

        print(
            "  lifecycle_status = superseded"
        )

        print()

        print(
            "Old id=2..6:"
        )

        print(
            "  storage_status = uploaded"
        )

        print(
            "  lifecycle_status = superseded"
        )

        print()

        print(
            "New id=7..10:"
        )

        print(
            "  storage_status = uploaded"
        )

        print(
            "  lifecycle_status = active"
        )

        print()

        print(
            "Effective uploaded files = 4"
        )

        print(
            "Effective uploaded rows = 123"
        )

        print()

        print(
            "NO NAS files were deleted."
        )

        print(
            "NO catalog rows were deleted."
        )

        print()

        print(
            "Rollback data remains available."
        )

        return 0

    finally:

        connection.close()


if __name__ == "__main__":

    raise SystemExit(
        main()
    )