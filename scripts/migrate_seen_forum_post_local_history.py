from __future__ import annotations

import argparse
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq

from crawl_framework.sites.naver_finance.plugin import (
    NaverFinancePlugin,
)

from migrate_seen_forum_post_hashes import (
    rebuild_record,
)


DEFAULT_SEEN_DB = Path(
    "state/seen.sqlite3"
)

DEFAULT_PARQUET = Path(
    "remote/"
    "site=naver_finance/"
    "country=KR/"
    "dataset=forum_post/"
    "year=2026/"
    "month=08/"
    "day=25/"
    "bucket=5f/"
    "part-064c17daaba742518424d4cf2c890527.parquet"
)


def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description=(
            "Migrate SeenStore hashes for "
            "historical LocalUploader "
            "Naver forum_post records."
        )
    )

    parser.add_argument(
        "--apply",
        action="store_true",
    )

    parser.add_argument(
        "--seen-db",
        default=str(
            DEFAULT_SEEN_DB
        ),
    )

    parser.add_argument(
        "--parquet",
        default=str(
            DEFAULT_PARQUET
        ),
    )

    return parser.parse_args()


def main() -> int:

    args = parse_args()

    seen_db = Path(
        args.seen_db
    ).resolve()

    parquet_path = Path(
        args.parquet
    ).resolve()

    print(
        "mode =",
        (
            "APPLY"
            if args.apply
            else "DRY-RUN"
        ),
    )

    print(
        "seen_db =",
        seen_db,
    )

    print(
        "parquet =",
        parquet_path,
    )

    if not seen_db.exists():

        raise FileNotFoundError(
            seen_db
        )

    if not parquet_path.exists():

        raise FileNotFoundError(
            parquet_path
        )

    # ========================================================
    # Read historical parquet
    # ========================================================

    df = (
        pq.read_table(
            parquet_path
        )
        .to_pandas()
    )

    print()
    print(
        "historical rows =",
        len(
            df
        ),
    )

    # ========================================================
    # SeenStore
    # ========================================================

    conn = sqlite3.connect(
        str(
            seen_db
        )
    )

    try:

        seen_rows = dict(
            conn.execute(
                """
                SELECT
                    record_uid,
                    version_hash
                FROM seen_records
                """
            ).fetchall()
        )

        print(
            "seen_store count before =",
            len(
                seen_rows
            ),
        )

        # ====================================================
        # Rebuild
        # ====================================================

        plugin = (
            NaverFinancePlugin()
        )

        updates: list[
            tuple[
                str,
                str,
            ]
        ] = []

        unchanged = 0
        needs_update = 0
        missing_seen = 0
        uid_mismatch = 0
        rebuild_failed = 0

        for _, source_row in (
            df.iterrows()
        ):

            row = (
                source_row.to_dict()
            )

            old_uid = str(
                row[
                    "record_uid"
                ]
            )

            try:

                record = (
                    rebuild_record(
                        plugin=plugin,
                        row=row,
                    )
                )

            except Exception as exc:

                rebuild_failed += 1

                print(
                    "[REBUILD FAILED]",
                    row.get(
                        "source_id"
                    ),
                    repr(
                        exc
                    ),
                )

                continue

            if (
                record.record_uid
                !=
                old_uid
            ):

                uid_mismatch += 1

                print(
                    "[UID MISMATCH]",
                    row.get(
                        "source_id"
                    ),
                )

                continue

            current_hash = (
                seen_rows.get(
                    old_uid
                )
            )

            if current_hash is None:

                missing_seen += 1

                print(
                    "[MISSING SEEN]",
                    row.get(
                        "source_id"
                    ),
                )

                continue

            if (
                current_hash
                ==
                record.version_hash
            ):

                unchanged += 1

                continue

            needs_update += 1

            updates.append(
                (
                    record.version_hash,
                    old_uid,
                )
            )

        # ====================================================
        # Plan
        # ====================================================

        print()
        print(
            "===== MIGRATION PLAN ====="
        )

        print(
            "unchanged =",
            unchanged,
        )

        print(
            "needs_update =",
            needs_update,
        )

        print(
            "missing_seen =",
            missing_seen,
        )

        print(
            "uid_mismatch =",
            uid_mismatch,
        )

        print(
            "rebuild_failed =",
            rebuild_failed,
        )

        # ====================================================
        # Safety gate
        # ====================================================

        if (
            missing_seen
            or
            uid_mismatch
            or
            rebuild_failed
        ):

            raise RuntimeError(
                "migration validation failed; "
                "refusing to modify SeenStore"
            )

        # ====================================================
        # Dry-run
        # ====================================================

        if not args.apply:

            print()
            print(
                "DRY RUN ONLY"
            )

            print(
                "No SeenStore rows "
                "were modified."
            )

            return 0

        # ====================================================
        # Backup
        # ====================================================

        timestamp = (
            datetime.now()
            .strftime(
                "%Y%m%d-%H%M%S"
            )
        )

        backup_path = (
            seen_db.parent
            /
            (
                f"{seen_db.name}"
                ".before_local_forum_hash_migration."
                f"{timestamp}.bak"
            )
        )

        # sqlite backup API is safer than
        # blindly copying an open WAL database.

        backup_conn = sqlite3.connect(
            str(
                backup_path
            )
        )

        try:

            conn.backup(
                backup_conn
            )

        finally:

            backup_conn.close()

        print()
        print(
            "backup =",
            backup_path,
        )

        # ====================================================
        # Apply transaction
        # ====================================================

        conn.execute(
            "BEGIN"
        )

        try:

            conn.executemany(
                """
                UPDATE seen_records
                SET
                    version_hash = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE
                    record_uid = ?
                """,
                updates,
            )

            conn.commit()

        except Exception:

            conn.rollback()

            raise

        print()
        print(
            "===== APPLY ====="
        )

        print(
            "migrated =",
            len(
                updates
            ),
        )

        count_after = (
            conn.execute(
                """
                SELECT COUNT(*)
                FROM seen_records
                """
            )
            .fetchone()[0]
        )

        print(
            "seen_store count after =",
            count_after,
        )

        return 0

    finally:

        conn.close()


if __name__ == "__main__":

    raise SystemExit(
        main()
    )