from __future__ import annotations

import argparse
from dataclasses import dataclass

from crawl_framework.config import (
    load_default_config,
)

from crawl_framework.storage.postgres_connection import (
    make_config_postgres_connection_factory,
)


SCHEMA = "marketdata"

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


@dataclass(
    frozen=True,
    slots=True,
)
class RowState:

    id: int

    storage_status: str

    lifecycle_status: str | None

    remote_path: str | None

    file_path: str

    row_count: int


def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description=(
            "Migrate data_files from mixed storage/lifecycle "
            "status semantics to separate storage_status and "
            "lifecycle_status columns."
        )
    )

    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Actually modify PostgreSQL. "
            "Without this flag only inspect and print the plan."
        ),
    )

    return parser.parse_args()


def column_exists(
    connection,
    column_name: str,
) -> bool:

    with connection.cursor() as cur:

        cur.execute(
            """
            SELECT EXISTS
            (
                SELECT 1
                FROM information_schema.columns
                WHERE
                    table_schema = %s
                    AND table_name = 'data_files'
                    AND column_name = %s
            )
            """,
            (
                SCHEMA,
                column_name,
            ),
        )

        return bool(
            cur.fetchone()[0]
        )


def load_rows_without_lifecycle(
    connection,
) -> list[
    tuple
]:

    with connection.cursor() as cur:

        cur.execute(
            """
            SELECT
                id,
                storage_status,
                remote_path,
                file_path,
                row_count
            FROM marketdata.data_files
            WHERE
                site_id = %s
                AND dataset = %s
            ORDER BY id
            """,
            (
                SITE_ID,
                DATASET,
            ),
        )

        return list(
            cur.fetchall()
        )


def load_rows_with_lifecycle(
    connection,
) -> list[
    RowState
]:

    with connection.cursor() as cur:

        cur.execute(
            """
            SELECT
                id,
                storage_status,
                lifecycle_status,
                remote_path,
                file_path,
                row_count
            FROM marketdata.data_files
            WHERE
                site_id = %s
                AND dataset = %s
            ORDER BY id
            """,
            (
                SITE_ID,
                DATASET,
            ),
        )

        raw = cur.fetchall()

    return [
        RowState(
            id=int(
                row[0]
            ),

            storage_status=str(
                row[1]
            ),

            lifecycle_status=(
                str(
                    row[2]
                )
                if row[2] is not None
                else None
            ),

            remote_path=(
                str(
                    row[3]
                )
                if row[3] is not None
                else None
            ),

            file_path=str(
                row[4]
            ),

            row_count=int(
                row[5]
            ),
        )
        for row in raw
    ]


def print_pre_migration_state(
    connection,
    has_lifecycle: bool,
) -> None:

    print()
    print(
        "========================================"
    )

    print(
        "CURRENT DATA_FILES STATE"
    )

    print(
        "========================================"
    )

    if has_lifecycle:

        rows = load_rows_with_lifecycle(
            connection
        )

        for row in rows:

            print()

            print(
                "id =",
                row.id,
            )

            print(
                "storage_status =",
                row.storage_status,
            )

            print(
                "lifecycle_status =",
                row.lifecycle_status,
            )

            print(
                "remote_path =",
                row.remote_path,
            )

            print(
                "rows =",
                row.row_count,
            )

            print(
                "file_path =",
                row.file_path,
            )

    else:

        rows = (
            load_rows_without_lifecycle(
                connection
            )
        )

        for row in rows:

            (
                file_id,
                storage_status,
                remote_path,
                file_path,
                row_count,
            ) = row

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
                "lifecycle_status = <COLUMN MISSING>"
            )

            print(
                "remote_path =",
                remote_path,
            )

            print(
                "rows =",
                row_count,
            )

            print(
                "file_path =",
                file_path,
            )


def validate_current_history(
    connection,
    has_lifecycle: bool,
) -> None:

    if has_lifecycle:

        rows = (
            load_rows_with_lifecycle(
                connection
            )
        )

        ids = {
            row.id
            for row in rows
        }

    else:

        rows = (
            load_rows_without_lifecycle(
                connection
            )
        )

        ids = {
            int(
                row[0]
            )
            for row in rows
        }

    expected = set(
        OLD_IDS
        +
        NEW_IDS
    )

    print()
    print(
        "===== HISTORY ID CHECK ====="
    )

    print(
        "expected ids =",
        sorted(
            expected
        ),
    )

    print(
        "actual ids =",
        sorted(
            ids
        ),
    )

    if (
        ids
        !=
        expected
    ):

        raise RuntimeError(
            "unexpected Naver forum_post "
            "catalog IDs; refusing migration"
        )


def print_plan() -> None:

    print()
    print(
        "========================================"
    )

    print(
        "MIGRATION PLAN"
    )

    print(
        "========================================"
    )

    print()
    print(
        "1. Add lifecycle_status column"
    )

    print(
        "   default = active"
    )

    print()
    print(
        "2. Restore physical storage semantics"
    )

    print(
        "   id=1:"
    )

    print(
        "       storage_status = local"
    )

    print(
        "       lifecycle_status = superseded"
    )

    print()

    print(
        "   id=2..6:"
    )

    print(
        "       storage_status = uploaded"
    )

    print(
        "       lifecycle_status = superseded"
    )

    print()

    print(
        "   id=7..10:"
    )

    print(
        "       storage_status = uploaded"
    )

    print(
        "       lifecycle_status = active"
    )

    print()

    print(
        "3. Preserve all file_path / remote_path / sha256"
    )

    print()

    print(
        "4. Delete NAS files: NO"
    )

    print(
        "5. Delete catalog rows: NO"
    )


def migrate_schema_and_rows(
    connection,
) -> None:

    with connection.cursor() as cur:

        # ====================================================
        # 1. Schema
        # ====================================================

        cur.execute(
            """
            ALTER TABLE marketdata.data_files

            ADD COLUMN IF NOT EXISTS
                lifecycle_status TEXT
                NOT NULL
                DEFAULT 'active'
            """
        )

        # ====================================================
        # 2. Useful active lookup index
        # ====================================================

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_data_files_active_lookup

            ON marketdata.data_files
            (
                site_id,
                dataset,
                lifecycle_status,
                storage_status,
                partition_date,
                bucket
            )
            """
        )

        # ====================================================
        # 3. Old local-only historical file
        # ====================================================

        cur.execute(
            """
            UPDATE marketdata.data_files
            SET
                storage_status = 'local',
                lifecycle_status = 'superseded',
                updated_at = NOW()
            WHERE
                id = 1
                AND site_id = %s
                AND dataset = %s
            """,
            (
                SITE_ID,
                DATASET,
            ),
        )

        if (
            cur.rowcount
            !=
            1
        ):

            raise RuntimeError(
                "expected exactly one row "
                "for historical id=1"
            )

        # ====================================================
        # 4. Old NAS files
        # ====================================================

        cur.execute(
            """
            UPDATE marketdata.data_files
            SET
                storage_status = 'uploaded',
                lifecycle_status = 'superseded',
                updated_at = NOW()
            WHERE
                id IN (2, 3, 4, 5, 6)
                AND site_id = %s
                AND dataset = %s
            """,
            (
                SITE_ID,
                DATASET,
            ),
        )

        if (
            cur.rowcount
            !=
            5
        ):

            raise RuntimeError(
                "expected exactly five old "
                "uploaded files ids=2..6"
            )

        # ====================================================
        # 5. New compacted active files
        # ====================================================

        cur.execute(
            """
            UPDATE marketdata.data_files
            SET
                storage_status = 'uploaded',
                lifecycle_status = 'active',
                updated_at = NOW()
            WHERE
                id IN (7, 8, 9, 10)
                AND site_id = %s
                AND dataset = %s
            """,
            (
                SITE_ID,
                DATASET,
            ),
        )

        if (
            cur.rowcount
            !=
            4
        ):

            raise RuntimeError(
                "expected exactly four "
                "new compacted files ids=7..10"
            )


def verify_final_state(
    connection,
) -> None:

    rows = (
        load_rows_with_lifecycle(
            connection
        )
    )

    by_id = {
        row.id:
            row
        for row in rows
    }

    print()
    print(
        "========================================"
    )

    print(
        "FINAL STATE"
    )

    print(
        "========================================"
    )

    for row in rows:

        print()

        print(
            "id =",
            row.id,
        )

        print(
            "storage_status =",
            row.storage_status,
        )

        print(
            "lifecycle_status =",
            row.lifecycle_status,
        )

        print(
            "remote_path =",
            row.remote_path,
        )

        print(
            "rows =",
            row.row_count,
        )

    # ========================================================
    # id=1
    # ========================================================

    row = by_id.get(
        1
    )

    if row is None:

        raise RuntimeError(
            "id=1 missing"
        )

    if (
        row.storage_status
        !=
        "local"
    ):

        raise RuntimeError(
            "id=1 storage_status "
            "must be local"
        )

    if (
        row.lifecycle_status
        !=
        "superseded"
    ):

        raise RuntimeError(
            "id=1 lifecycle_status "
            "must be superseded"
        )

    if (
        row.remote_path
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

        row = by_id.get(
            file_id
        )

        if row is None:

            raise RuntimeError(
                f"id={file_id} missing"
            )

        if (
            row.storage_status
            !=
            "uploaded"
        ):

            raise RuntimeError(
                f"id={file_id} must have "
                "storage_status=uploaded"
            )

        if (
            row.lifecycle_status
            !=
            "superseded"
        ):

            raise RuntimeError(
                f"id={file_id} must have "
                "lifecycle_status=superseded"
            )

        if not row.remote_path:

            raise RuntimeError(
                f"id={file_id} missing "
                "remote_path"
            )

    # ========================================================
    # ids=7..10
    # ========================================================

    active_rows = 0

    for file_id in (
        7,
        8,
        9,
        10,
    ):

        row = by_id.get(
            file_id
        )

        if row is None:

            raise RuntimeError(
                f"id={file_id} missing"
            )

        if (
            row.storage_status
            !=
            "uploaded"
        ):

            raise RuntimeError(
                f"id={file_id} must have "
                "storage_status=uploaded"
            )

        if (
            row.lifecycle_status
            !=
            "active"
        ):

            raise RuntimeError(
                f"id={file_id} must have "
                "lifecycle_status=active"
            )

        if not row.remote_path:

            raise RuntimeError(
                f"id={file_id} missing "
                "remote_path"
            )

        active_rows += (
            row.row_count
        )

    if (
        active_rows
        !=
        123
    ):

        raise RuntimeError(
            "active compacted row count "
            f"is {active_rows}, expected 123"
        )

    # ========================================================
    # Active query semantics
    # ========================================================

    with connection.cursor() as cur:

        cur.execute(
            """
            SELECT
                id,
                row_count
            FROM marketdata.data_files
            WHERE
                site_id = %s
                AND dataset = %s
                AND storage_status = 'uploaded'
                AND lifecycle_status = 'active'
            ORDER BY id
            """,
            (
                SITE_ID,
                DATASET,
            ),
        )

        active = (
            cur.fetchall()
        )

    active_ids = {
        int(
            row[0]
        )
        for row in active
    }

    active_count = sum(
        int(
            row[1]
        )
        for row in active
    )

    print()
    print(
        "===== EFFECTIVE ACTIVE DATASET ====="
    )

    print(
        "active ids =",
        sorted(
            active_ids
        ),
    )

    print(
        "active files =",
        len(
            active
        ),
    )

    print(
        "active rows =",
        active_count,
    )

    if (
        active_ids
        !=
        {
            7,
            8,
            9,
            10,
        }
    ):

        raise RuntimeError(
            "active dataset is not "
            "exactly ids 7..10"
        )

    if (
        active_count
        !=
        123
    ):

        raise RuntimeError(
            "active dataset does not "
            "contain exactly 123 rows"
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

        has_lifecycle = (
            column_exists(
                connection,
                "lifecycle_status",
            )
        )

        print(
            "lifecycle_status column exists =",
            has_lifecycle,
        )

        print_pre_migration_state(
            connection,
            has_lifecycle,
        )

        validate_current_history(
            connection,
            has_lifecycle,
        )

        print_plan()

        # ====================================================
        # Dry-run
        # ====================================================

        if not args.apply:

            print()
            print(
                "========================================"
            )

            print(
                "MIGRATION DRY-RUN SUCCESS"
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
        # Apply transaction
        # ====================================================

        try:

            migrate_schema_and_rows(
                connection
            )

            # Verify BEFORE COMMIT.

            verify_final_state(
                connection
            )

            connection.commit()

        except Exception:

            connection.rollback()

            print()
            print(
                "MIGRATION FAILED"
            )

            print(
                "Transaction rolled back."
            )

            raise

        # ====================================================
        # Verify AFTER COMMIT
        # ====================================================

        print()
        print(
            "===== POST-COMMIT VERIFY ====="
        )

        verify_final_state(
            connection
        )

        print()
        print(
            "========================================"
        )

        print(
            "LIFECYCLE MIGRATION SUCCESS"
        )

        print(
            "========================================"
        )

        print()
        print(
            "id=1:"
        )

        print(
            "  storage_status = local"
        )

        print(
            "  lifecycle_status = superseded"
        )

        print()
        print(
            "id=2..6:"
        )

        print(
            "  storage_status = uploaded"
        )

        print(
            "  lifecycle_status = superseded"
        )

        print()
        print(
            "id=7..10:"
        )

        print(
            "  storage_status = uploaded"
        )

        print(
            "  lifecycle_status = active"
        )

        print()
        print(
            "active files = 4"
        )

        print(
            "active rows = 123"
        )

        print()
        print(
            "NO NAS files deleted."
        )

        print(
            "NO catalog rows deleted."
        )

        return 0

    finally:

        connection.close()


if __name__ == "__main__":

    raise SystemExit(
        main()
    )