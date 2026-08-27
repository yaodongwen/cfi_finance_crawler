from __future__ import annotations

import argparse
import hashlib
import shutil
import sqlite3
import subprocess
import tempfile

from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from crawl_framework.config import (
    load_default_config,
)

from crawl_framework.storage.parquet_writer import (
    ParquetFileInfo,
)

from crawl_framework.storage.partition import (
    PartitionKey,
)

from crawl_framework.storage.postgres import (
    PostgresCatalog,
)

from crawl_framework.storage.postgres_connection import (
    make_config_postgres_connection_factory,
)

from crawl_framework.storage.uploader import (
    RsyncUploader,
)


SITE_ID = "naver_finance"

COUNTRY = "KR"

DATASET = "forum_post"

DEFAULT_INPUT_DIR = Path(
    "tmp/naver_forum_post_compaction"
)

DEFAULT_SEEN_DB = Path(
    "state/seen.sqlite3"
)


# ============================================================
# CLI
# ============================================================


def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description=(
            "Publish validated compacted Naver "
            "forum_post Parquet files."
        )
    )

    parser.add_argument(
        "--input-dir",
        default=str(
            DEFAULT_INPUT_DIR
        ),
    )

    parser.add_argument(
        "--seen-db",
        default=str(
            DEFAULT_SEEN_DB
        ),
    )

    parser.add_argument(
        "--ssh-port",
        type=int,
        default=22,
    )

    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Actually upload files and register "
            "them in PostgreSQL. "
            "Without this flag, run dry-run only."
        ),
    )

    return parser.parse_args()


# ============================================================
# Generic helpers
# ============================================================


def file_sha256(
    path: Path,
) -> str:

    digest = hashlib.sha256()

    with path.open(
        "rb"
    ) as file:

        while True:

            chunk = file.read(
                1024 * 1024
            )

            if not chunk:
                break

            digest.update(
                chunk
            )

    return digest.hexdigest()


def normalize_datetime(
    value: Any,
) -> datetime | None:

    if value is None:
        return None

    try:

        if pd.isna(
            value
        ):

            return None

    except Exception:
        pass

    if isinstance(
        value,
        pd.Timestamp,
    ):

        return value.to_pydatetime()

    if isinstance(
        value,
        datetime,
    ):

        return value

    parsed = pd.to_datetime(
        value,
        errors="coerce",
        utc=True,
    )

    if pd.isna(
        parsed
    ):

        return None

    if isinstance(
        parsed,
        pd.Timestamp,
    ):

        return parsed.to_pydatetime()

    return None


def event_time_range(
    df: pd.DataFrame,
) -> tuple[
    datetime | None,
    datetime | None,
]:

    if (
        "event_time"
        not in df.columns
    ):

        return (
            None,
            None,
        )

    values = [
        normalize_datetime(
            value
        )
        for value
        in df[
            "event_time"
        ].tolist()
    ]

    values = [
        value
        for value
        in values
        if value is not None
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


def unique_schema_version(
    df: pd.DataFrame,
) -> int:

    if (
        "schema_version"
        not in df.columns
    ):

        raise RuntimeError(
            "Parquet missing schema_version"
        )

    values = {
        int(
            value
        )
        for value
        in df[
            "schema_version"
        ].tolist()
        if not pd.isna(
            value
        )
    }

    if len(
        values
    ) != 1:

        raise RuntimeError(
            "expected exactly one "
            "schema_version per Parquet; "
            f"got {sorted(values)}"
        )

    return next(
        iter(
            values
        )
    )


# ============================================================
# Partition parsing
# ============================================================


def parse_partition_from_relative_path(
    relative_path: Path,
) -> PartitionKey:
    """
    Expected:

        site=naver_finance/
        country=KR/
        dataset=forum_post/
        year=2026/
        month=08/
        day=26/
        bucket=20/
        part-compacted-xxx.parquet
    """

    values: dict[
        str,
        str,
    ] = {}

    for part in (
        relative_path.parts[:-1]
    ):

        if "=" not in part:
            continue

        key, value = (
            part.split(
                "=",
                1,
            )
        )

        values[
            key
        ] = value

    required = {
        "site",
        "country",
        "dataset",
        "year",
        "month",
        "day",
        "bucket",
    }

    missing = (
        required
        -
        set(
            values
        )
    )

    if missing:

        raise RuntimeError(
            "invalid compacted partition path; "
            f"missing={sorted(missing)}, "
            f"path={relative_path}"
        )

    if (
        values[
            "site"
        ]
        != SITE_ID
    ):

        raise RuntimeError(
            "unexpected site: "
            f"{values['site']}"
        )

    if (
        values[
            "country"
        ]
        != COUNTRY
    ):

        raise RuntimeError(
            "unexpected country: "
            f"{values['country']}"
        )

    if (
        values[
            "dataset"
        ]
        != DATASET
    ):

        raise RuntimeError(
            "unexpected dataset: "
            f"{values['dataset']}"
        )

    partition_date = date(
        int(
            values[
                "year"
            ]
        ),
        int(
            values[
                "month"
            ]
        ),
        int(
            values[
                "day"
            ]
        ),
    )

    # bucket_label is hexadecimal when bucket_count=256.
    #
    # Examples:
    #
    #     20 -> 0x20 -> 32
    #     5f -> 0x5f -> 95
    #     f1 -> 0xf1 -> 241

    bucket = int(
        values[
            "bucket"
        ],
        16,
    )

    return PartitionKey(
        site_id=SITE_ID,
        country=COUNTRY,
        dataset=DATASET,
        partition_date=(
            partition_date
        ),
        bucket=bucket,
        bucket_count=256,
    )


# ============================================================
# Build ParquetFileInfo
# ============================================================


def build_parquet_info(
    *,
    file_path: Path,
    input_root: Path,
) -> ParquetFileInfo:

    relative_path = (
        file_path.relative_to(
            input_root
        )
    )

    partition = (
        parse_partition_from_relative_path(
            relative_path
        )
    )

    table = pq.read_table(
        file_path
    )

    df = table.to_pandas()

    row_count = len(
        df
    )

    if row_count < 1:

        raise RuntimeError(
            "cannot publish empty Parquet: "
            f"{file_path}"
        )

    (
        min_event_time,
        max_event_time,
    ) = event_time_range(
        df
    )

    schema_version = (
        unique_schema_version(
            df
        )
    )

    return ParquetFileInfo(
        file_path=(
            file_path
        ),
        relative_path=(
            relative_path
        ),
        partition=(
            partition
        ),
        row_count=(
            row_count
        ),
        file_size=(
            file_path
            .stat()
            .st_size
        ),
        sha256=(
            file_sha256(
                file_path
            )
        ),
        min_event_time=(
            min_event_time
        ),
        max_event_time=(
            max_event_time
        ),
        schema_version=(
            schema_version
        ),
    )


# ============================================================
# Local clean-set verification
# ============================================================


def load_seen_hashes(
    seen_db: Path,
) -> dict[
    str,
    str,
]:

    conn = sqlite3.connect(
        str(
            seen_db
        )
    )

    try:

        rows = conn.execute(
            """
            SELECT
                record_uid,
                version_hash
            FROM seen_records
            """
        ).fetchall()

    finally:

        conn.close()

    return {
        str(
            uid
        ): str(
            version_hash
        )
        for (
            uid,
            version_hash,
        )
        in rows
    }


def verify_parquet_set(
    *,
    parquet_paths: list[
        Path
    ],
    seen_hashes: dict[
        str,
        str,
    ],
    expected_total: int = 123,
) -> None:

    frames: list[
        pd.DataFrame
    ] = []

    for path in parquet_paths:

        df = (
            pq.read_table(
                path
            )
            .to_pandas()
        )

        frames.append(
            df
        )

    if not frames:

        raise RuntimeError(
            "no Parquet files to verify"
        )

    combined = pd.concat(
        frames,
        ignore_index=True,
    )

    combined[
        "record_uid"
    ] = (
        combined[
            "record_uid"
        ]
        .astype(str)
    )

    combined[
        "version_hash"
    ] = (
        combined[
            "version_hash"
        ]
        .astype(str)
    )

    row_count = len(
        combined
    )

    unique_uid_count = (
        combined[
            "record_uid"
        ]
        .nunique()
    )

    duplicate_count = (
        row_count
        -
        unique_uid_count
    )

    output_uids = set(
        combined[
            "record_uid"
        ]
    )

    seen_uids = set(
        seen_hashes
    )

    missing_seen = (
        output_uids
        -
        seen_uids
    )

    hash_mismatch = 0

    for _, row in (
        combined.iterrows()
    ):

        uid = str(
            row[
                "record_uid"
            ]
        )

        version_hash = str(
            row[
                "version_hash"
            ]
        )

        expected_hash = (
            seen_hashes.get(
                uid
            )
        )

        if (
            expected_hash
            !=
            version_hash
        ):

            hash_mismatch += 1

    print()
    print(
        "===== CLEAN SET VERIFICATION ====="
    )

    print(
        "files =",
        len(
            parquet_paths
        ),
    )

    print(
        "rows =",
        row_count,
    )

    print(
        "unique uid =",
        unique_uid_count,
    )

    print(
        "duplicate rows =",
        duplicate_count,
    )

    print(
        "missing seen uid =",
        len(
            missing_seen
        ),
    )

    print(
        "hash mismatch =",
        hash_mismatch,
    )

    if (
        row_count
        !=
        expected_total
    ):

        raise RuntimeError(
            "unexpected clean row count: "
            f"{row_count}; "
            f"expected={expected_total}"
        )

    if (
        unique_uid_count
        !=
        expected_total
    ):

        raise RuntimeError(
            "unexpected unique UID count"
        )

    if duplicate_count != 0:

        raise RuntimeError(
            "clean Parquet contains "
            "duplicate record_uid"
        )

    if missing_seen:

        raise RuntimeError(
            "clean Parquet contains "
            "record_uid missing from SeenStore"
        )

    if hash_mismatch != 0:

        raise RuntimeError(
            "clean Parquet version_hash "
            "does not match SeenStore"
        )


# ============================================================
# PostgreSQL helpers
# ============================================================


def find_catalog_row(
    connection,
    *,
    file_path: str,
):
    """
    根据唯一 file_path 查询 Catalog。

    查询统一委托给 PostgresCatalog.get_data_file()。

    为兼容本脚本现有的历史逻辑，
    这里继续返回原来的 tuple 结构：

        (
            id,
            file_path,
            sha256,
            row_count,
            file_size,
            storage_status,
            remote_path,
        )

    注意：

        本函数属于精确 file_path 审计查询，
        因此不限制 lifecycle_status。

        superseded / archived 文件仍然必须能够
        被 publish 幂等检查发现，不能假装不存在。
    """

    catalog = PostgresCatalog(
        connection
    )

    row = catalog.get_data_file(
        file_path=file_path,
    )

    if row is None:

        return None

    return (
        row.id,
        row.file_path,
        row.sha256,
        row.row_count,
        row.file_size,
        row.storage_status,
        row.remote_path,
    )
    

def verify_existing_catalog_row(
    *,
    row,
    info: ParquetFileInfo,
) -> None:

    (
        file_id,
        file_path,
        sha256,
        row_count,
        file_size,
        storage_status,
        remote_path,
    ) = row

    if (
        str(
            sha256
        )
        !=
        info.sha256
    ):

        raise RuntimeError(
            "existing catalog sha256 mismatch: "
            f"id={file_id}, "
            f"path={file_path}"
        )

    if (
        int(
            row_count
        )
        !=
        info.row_count
    ):

        raise RuntimeError(
            "existing catalog row_count mismatch: "
            f"id={file_id}"
        )

    if (
        int(
            file_size
        )
        !=
        info.file_size
    ):

        raise RuntimeError(
            "existing catalog file_size mismatch: "
            f"id={file_id}"
        )


# ============================================================
# Publish one file
# ============================================================


def publish_one(
    *,
    info: ParquetFileInfo,
    uploader: RsyncUploader,
    connection,
    apply: bool,
) -> str:

    relative = (
        info.relative_path
        .as_posix()
    )

    print()
    print(
        "========================================"
    )

    print(
        "FILE"
    )

    print(
        "========================================"
    )

    print(
        "relative_path =",
        relative,
    )

    print(
        "rows =",
        info.row_count,
    )

    print(
        "size =",
        info.file_size,
    )

    print(
        "sha256 =",
        info.sha256,
    )

    print(
        "partition_date =",
        info.partition.partition_date,
    )

    print(
        "bucket =",
        info.partition.bucket_label,
    )

    # ========================================================
    # Existing catalog check
    # ========================================================

    existing = find_catalog_row(
        connection,
        file_path=relative,
    )

    if existing is not None:

        verify_existing_catalog_row(
            row=existing,
            info=info,
        )

        print(
            "catalog existing id =",
            existing[0],
        )

        print(
            "catalog existing status =",
            existing[5],
        )

    # ========================================================
    # Upload
    # ========================================================

    upload_result = uploader.upload(
        info
    )

    print(
        "upload status =",
        upload_result.status,
    )

    print(
        "remote_path =",
        upload_result.remote_path,
    )

    print(
        "local_size =",
        upload_result.local_size,
    )

    print(
        "remote_size =",
        upload_result.remote_size,
    )

    print(
        "local_sha256 =",
        upload_result.local_sha256,
    )

    print(
        "remote_sha256 =",
        upload_result.remote_sha256,
    )

    # ========================================================
    # Dry-run ends here
    # ========================================================

    if not apply:

        if (
            upload_result.status
            !=
            "dry_run"
        ):

            raise RuntimeError(
                "expected dry_run upload status"
            )

        return str(
            upload_result.remote_path
        )

    # ========================================================
    # Production upload MUST be verified
    # ========================================================

    if (
        upload_result.status
        !=
        "verified"
    ):

        raise RuntimeError(
            "production upload was not "
            "fully verified: "
            f"{relative}"
        )

    remote_path = str(
        upload_result.remote_path
    ).strip()

    if not remote_path:

        raise RuntimeError(
            "verified upload returned "
            "empty remote_path"
        )

    # ========================================================
    # Catalog
    # ========================================================

    catalog = PostgresCatalog(
        connection
    )

    existing = find_catalog_row(
        connection,
        file_path=relative,
    )

    # --------------------------------------------------------
    # First publication
    # --------------------------------------------------------

    if existing is None:

        catalog.register_parquet_file(
            info
        )

        print(
            "catalog registration = NEW"
        )

    # --------------------------------------------------------
    # Safe rerun after partial previous publication
    # --------------------------------------------------------

    else:

        verify_existing_catalog_row(
            row=existing,
            info=info,
        )

        print(
            "catalog registration = EXISTING"
        )

    # ========================================================
    # Mark uploaded
    # ========================================================

    catalog.mark_uploaded(
        file_path=relative,
        remote_path=remote_path,
    )

    # ========================================================
    # Read back catalog
    # ========================================================

    final_row = find_catalog_row(
        connection,
        file_path=relative,
    )

    if final_row is None:

        raise RuntimeError(
            "catalog row disappeared "
            "after registration"
        )

    verify_existing_catalog_row(
        row=final_row,
        info=info,
    )

    (
        final_id,
        _,
        _,
        _,
        _,
        final_status,
        final_remote_path,
    ) = final_row

    print(
        "catalog id =",
        final_id,
    )

    print(
        "catalog status =",
        final_status,
    )

    print(
        "catalog remote =",
        final_remote_path,
    )

    if (
        final_status
        !=
        "uploaded"
    ):

        raise RuntimeError(
            "catalog was not marked uploaded: "
            f"id={final_id}"
        )

    if (
        str(
            final_remote_path
        )
        !=
        remote_path
    ):

        raise RuntimeError(
            "catalog remote_path mismatch: "
            f"id={final_id}"
        )

    return remote_path


# ============================================================
# Remote read-back verification
# ============================================================


def verify_remote_readback(
    *,
    infos: list[
        ParquetFileInfo
    ],
    remote_paths: list[
        str
    ],
    remote_host: str,
    remote_user: str | None,
    ssh_port: int,
    seen_hashes: dict[
        str,
        str,
    ],
) -> None:

    if len(
        infos
    ) != len(
        remote_paths
    ):

        raise RuntimeError(
            "remote path count mismatch"
        )

    ssh_target = (
        (
            f"{remote_user}@"
            f"{remote_host}"
        )
        if remote_user
        else remote_host
    )

    with tempfile.TemporaryDirectory(
        prefix=(
            "naver_forum_compaction_verify_"
        )
    ) as temp_text:

        temp_dir = Path(
            temp_text
        )

        downloaded: list[
            Path
        ] = []

        for index, (
            info,
            remote_path,
        ) in enumerate(
            zip(
                infos,
                remote_paths,
            ),
            start=1,
        ):

            destination = (
                temp_dir
                /
                (
                    f"{index:02d}-"
                    f"{info.file_path.name}"
                )
            )

            print()
            print(
                "[READBACK]",
                remote_path,
            )

            subprocess.run(
                [
                    "scp",
                    "-q",
                    "-P",
                    str(
                        ssh_port
                    ),
                    (
                        f"{ssh_target}:"
                        f"{remote_path}"
                    ),
                    str(
                        destination
                    ),
                ],
                check=True,
            )

            # ================================================
            # Byte-level verification again
            # ================================================

            downloaded_sha = (
                file_sha256(
                    destination
                )
            )

            if (
                downloaded_sha
                !=
                info.sha256
            ):

                raise RuntimeError(
                    "readback sha256 mismatch: "
                    f"{remote_path}"
                )

            if (
                destination
                .stat()
                .st_size
                !=
                info.file_size
            ):

                raise RuntimeError(
                    "readback file_size mismatch: "
                    f"{remote_path}"
                )

            downloaded.append(
                destination
            )

        # ====================================================
        # Logical verification
        # ====================================================

        verify_parquet_set(
            parquet_paths=downloaded,
            seen_hashes=seen_hashes,
            expected_total=123,
        )


# ============================================================
# Final catalog summary
# ============================================================


def print_new_catalog_rows(
    connection,
    *,
    infos: list[
        ParquetFileInfo
    ],
) -> None:

    print()
    print(
        "========================================"
    )

    print(
        "PUBLISHED CATALOG ROWS"
    )

    print(
        "========================================"
    )

    for info in infos:

        relative = (
            info.relative_path
            .as_posix()
        )

        row = find_catalog_row(
            connection,
            file_path=relative,
        )

        if row is None:

            raise RuntimeError(
                "published catalog row missing: "
                f"{relative}"
            )

        print()
        print(
            "id =",
            row[0],
        )

        print(
            "path =",
            row[1],
        )

        print(
            "rows =",
            row[3],
        )

        print(
            "status =",
            row[5],
        )

        print(
            "remote =",
            row[6],
        )


# ============================================================
# Main
# ============================================================


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

    input_dir = Path(
        args.input_dir
    ).resolve()

    seen_db = Path(
        args.seen_db
    ).resolve()

    if not input_dir.exists():

        raise FileNotFoundError(
            input_dir
        )

    if not seen_db.exists():

        raise FileNotFoundError(
            seen_db
        )

    # ========================================================
    # Discover compacted Parquet
    # ========================================================

    parquet_paths = sorted(
        path
        for path
        in input_dir.rglob(
            "*.parquet"
        )
        if path.is_file()
    )

    print(
        "input_dir =",
        input_dir,
    )

    print(
        "compacted files =",
        len(
            parquet_paths
        ),
    )

    if len(
        parquet_paths
    ) != 4:

        raise RuntimeError(
            "expected exactly 4 compacted "
            "Parquet files; "
            f"found={len(parquet_paths)}"
        )

    # ========================================================
    # SeenStore
    # ========================================================

    seen_hashes = (
        load_seen_hashes(
            seen_db
        )
    )

    print(
        "seen_store rows =",
        len(
            seen_hashes
        ),
    )

    # ========================================================
    # Verify complete clean set BEFORE anything else
    # ========================================================

    verify_parquet_set(
        parquet_paths=(
            parquet_paths
        ),
        seen_hashes=(
            seen_hashes
        ),
        expected_total=123,
    )

    # ========================================================
    # Build ParquetFileInfo
    # ========================================================

    infos = [
        build_parquet_info(
            file_path=path,
            input_root=input_dir,
        )
        for path
        in parquet_paths
    ]

    print()
    print(
        "===== PUBLICATION PLAN ====="
    )

    print(
        "files =",
        len(
            infos
        ),
    )

    print(
        "rows =",
        sum(
            info.row_count
            for info
            in infos
        ),
    )

    for info in infos:

        print()

        print(
            info.relative_path
            .as_posix()
        )

        print(
            "  rows =",
            info.row_count,
        )

        print(
            "  sha256 =",
            info.sha256,
        )

    # ========================================================
    # Config
    # ========================================================

    config = (
        load_default_config()
    )

    remote_host = str(
        config.server.host
    ).strip()

    remote_user_raw = getattr(
        config.server,
        "user",
        None,
    )

    remote_user = (
        str(
            remote_user_raw
        ).strip()
        if remote_user_raw
        else None
    )

    remote_root = str(
        config.server.data_dir
    ).strip()

    # ========================================================
    # Uploader
    # ========================================================

    uploader = RsyncUploader(
        remote_host=remote_host,
        remote_root=remote_root,
        remote_user=remote_user,
        ssh_port=(
            args.ssh_port
        ),
        dry_run=(
            not args.apply
        ),

        # Strong publication verification.
        verify_size=True,
        verify_sha256=True,
    )

    # ========================================================
    # PostgreSQL
    # ========================================================

    factory = (
        make_config_postgres_connection_factory(
            config
        )
    )

    connection = factory()

    try:

        remote_paths: list[
            str
        ] = []

        for info in infos:

            remote_path = publish_one(
                info=info,
                uploader=uploader,
                connection=connection,
                apply=args.apply,
            )

            remote_paths.append(
                remote_path
            )

        # ====================================================
        # Dry-run finished
        # ====================================================

        if not args.apply:

            print()
            print(
                "========================================"
            )

            print(
                "PUBLICATION DRY-RUN SUCCESS"
            )

            print(
                "========================================"
            )

            print(
                "files planned =",
                len(
                    infos
                ),
            )

            print(
                "rows planned =",
                sum(
                    info.row_count
                    for info
                    in infos
                ),
            )

            print()
            print(
                "NO NAS files changed."
            )

            print(
                "NO PostgreSQL rows changed."
            )

            print(
                "Old catalog id=1..6 "
                "remain untouched."
            )

            return 0

        # ====================================================
        # Remote byte + logical read-back verification
        # ====================================================

        print()
        print(
            "========================================"
        )

        print(
            "REMOTE READBACK VERIFICATION"
        )

        print(
            "========================================"
        )

        verify_remote_readback(
            infos=infos,
            remote_paths=remote_paths,
            remote_host=remote_host,
            remote_user=remote_user,
            ssh_port=(
                args.ssh_port
            ),
            seen_hashes=(
                seen_hashes
            ),
        )

        # ====================================================
        # Catalog summary
        # ====================================================

        print_new_catalog_rows(
            connection,
            infos=infos,
        )

        # ====================================================
        # Final success
        # ====================================================

        print()
        print(
            "========================================"
        )

        print(
            "COMPACTION PUBLICATION SUCCESS"
        )

        print(
            "========================================"
        )

        print(
            "published files =",
            len(
                infos
            ),
        )

        print(
            "published logical rows =",
            sum(
                info.row_count
                for info
                in infos
            ),
        )

        print(
            "expected logical rows = 123"
        )

        print()
        print(
            "Old catalog id=1..6 "
            "were NOT deleted."
        )

        print(
            "Old NAS/local historical "
            "files were NOT deleted."
        )

        print()
        print(
            "Safe rollback copies remain."
        )

        return 0

    finally:

        connection.close()


if __name__ == "__main__":

    raise SystemExit(
        main()
    )