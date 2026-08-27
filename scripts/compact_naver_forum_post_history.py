from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import subprocess
import tempfile
import uuid

from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from crawl_framework.config import (
    load_default_config,
)

from crawl_framework.storage.postgres_connection import (
    make_config_postgres_connection_factory,
)

from crawl_framework.storage.postgres import (
    PostgresCatalog,
)

from migrate_seen_forum_post_hashes import (
    rebuild_record,
)


SITE_ID = "naver_finance"

DATASET = "forum_post"


# ============================================================
# CLI
# ============================================================


def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description=(
            "Dry-run compaction of historical "
            "Naver Finance forum_post Parquet files."
        )
    )

    parser.add_argument(
        "--output-dir",
        default=(
            "tmp/"
            "naver_forum_post_compaction"
        ),
        help=(
            "Local output directory for clean "
            "Parquet files."
        ),
    )

    parser.add_argument(
        "--seen-db",
        default=(
            "state/seen.sqlite3"
        ),
    )

    parser.add_argument(
        "--ssh-port",
        type=int,
        default=22,
    )

    parser.add_argument(
        "--overwrite-output",
        action="store_true",
        help=(
            "Remove existing output directory "
            "before generating clean files."
        ),
    )

    return parser.parse_args()


# ============================================================
# Helpers
# ============================================================


def optional_text(
    value: Any,
) -> str | None:

    if value is None:
        return None

    try:

        if pd.isna(
            value
        ):

            return None

    except Exception:
        pass

    text = str(
        value
    ).strip()

    if not text:
        return None

    return text


def compact_json(
    value: Any,
) -> str:

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(
            ",",
            ":",
        ),
    )


def mutation_policy_text(
    value: Any,
) -> str:

    if value is None:

        return ""

    enum_value = getattr(
        value,
        "value",
        None,
    )

    if enum_value is not None:

        return str(
            enum_value
        )

    return str(
        value
    )


def crawled_sort_value(
    value: Any,
) -> str:

    if value is None:

        return ""

    try:

        if pd.isna(
            value
        ):

            return ""

    except Exception:
        pass

    if hasattr(
        value,
        "isoformat",
    ):

        try:

            return value.isoformat()

        except Exception:
            pass

    return str(
        value
    )


def partition_directory(
    relative_path: str,
) -> str:
    """
    例如：

        site=naver_finance/
        country=KR/
        dataset=forum_post/
        year=2026/
        month=08/
        day=26/
        bucket=20/
        part-xxx.parquet

    返回：

        site=naver_finance/
        country=KR/
        dataset=forum_post/
        year=2026/
        month=08/
        day=26/
        bucket=20
    """

    path = Path(
        relative_path
    )

    return str(
        path.parent
    )


# ============================================================
# PostgreSQL Catalog
# ============================================================


def load_catalog_rows(
    config,
) -> list[
    tuple[
        int,
        str,
        int,
        str,
        str | None,
        Any,
    ]
]:
    """
    获取当前真正有效的 Naver forum_post
    Parquet Catalog 文件。

    正常 compaction 的输入统一定义为：

        storage_status = 'uploaded'
        lifecycle_status = 'active'

    不再自动包含：

        local
        superseded
        archived

    历史修复场景如果未来仍有需要，
    应单独提供显式 legacy / recovery 模式，
    不能混入正常 compaction。
    """

    factory = (
        make_config_postgres_connection_factory(
            config
        )
    )

    conn = factory()

    try:

        catalog = PostgresCatalog(
            conn
        )

        files = (
            catalog.list_active_data_files(
                site_id=SITE_ID,
                dataset=DATASET,
                country="KR",
            )
        )

        rows: list[
            tuple[
                int,
                str,
                int,
                str,
                str | None,
                Any,
            ]
        ] = []

        for item in files:

            rows.append(
                (
                    item.id,
                    item.file_path,
                    item.row_count,
                    item.storage_status,
                    item.remote_path,

                    # 旧 compactor tuple 中包含
                    # catalog_created_at。
                    #
                    # 当前 CatalogDataFile 暂未暴露
                    # created_at，而 choose_latest_rows()
                    # 实际排序使用的是：
                    #
                    #     crawled_at
                    #     _catalog_id
                    #
                    # 因此这里保留兼容槽位，
                    # 不影响去重选择逻辑。
                    None,
                )
            )

        return rows

    finally:

        conn.close()

# ============================================================
# Load historical files
# ============================================================


def load_historical_frames(
    *,
    catalog_rows,
    remote_host: str,
    remote_user: str,
    ssh_port: int,
) -> tuple[
    list[pd.DataFrame],
    pa.Schema,
]:
    """
    加载：

        uploaded -> NAS
        historical local -> ./remote/

    不修改任何源文件。
    """

    frames: list[
        pd.DataFrame
    ] = []

    reference_schema: (
        pa.Schema
        | None
    ) = None

    with tempfile.TemporaryDirectory(
        prefix=(
            "naver_forum_compact_source_"
        )
    ) as temp_dir_text:

        temp_dir = Path(
            temp_dir_text
        )

        total = len(
            catalog_rows
        )

        for position, row in enumerate(
            catalog_rows,
            start=1,
        ):

            (
                file_id,
                file_path,
                expected_rows,
                storage_status,
                remote_path,
                catalog_created_at,
            ) = row

            print()
            print(
                f"[load {position}/{total}] "
                f"id={file_id}"
            )

            # ================================================
            # NAS uploaded
            # ================================================

            if (
                storage_status == "uploaded"
                and remote_path
            ):

                local_path = (
                    temp_dir
                    / (
                        f"id-{file_id}-"
                        f"{Path(file_path).name}"
                    )
                )

                remote_spec = (
                    f"{remote_user}@"
                    f"{remote_host}:"
                    f"{remote_path}"
                )

                subprocess.run(
                    [
                        "scp",
                        "-q",
                        "-P",
                        str(
                            ssh_port
                        ),
                        remote_spec,
                        str(
                            local_path
                        ),
                    ],
                    check=True,
                )

            # ================================================
            # Historical LocalUploader
            # ================================================

            else:

                local_path = (
                    Path("remote")
                    / Path(
                        file_path
                    )
                ).resolve()

                if not local_path.exists():

                    raise FileNotFoundError(
                        "historical local file "
                        "not found: "
                        f"id={file_id}, "
                        f"path={local_path}"
                    )

            table = pq.read_table(
                local_path
            )

            if reference_schema is None:

                reference_schema = (
                    table.schema
                )

            df = table.to_pandas()

            actual_rows = len(
                df
            )

            print(
                "  expected rows =",
                expected_rows,
            )

            print(
                "  actual rows   =",
                actual_rows,
            )

            if (
                actual_rows
                !=
                int(
                    expected_rows
                )
            ):

                raise RuntimeError(
                    "catalog/parquet row count "
                    "mismatch: "
                    f"id={file_id}"
                )

            df[
                "_catalog_id"
            ] = int(
                file_id
            )

            df[
                "_catalog_created_at"
            ] = (
                catalog_created_at
            )

            df[
                "_source_file_path"
            ] = str(
                file_path
            )

            df[
                "_partition_directory"
            ] = (
                partition_directory(
                    file_path
                )
            )

            frames.append(
                df
            )

    if reference_schema is None:

        raise RuntimeError(
            "no historical parquet schema found"
        )

    return (
        frames,
        reference_schema,
    )


# ============================================================
# Latest physical row per record_uid
# ============================================================


def choose_latest_rows(
    all_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    同一个 record_uid 可能有 1~3 个历史副本。

    使用：

        crawled_at
        catalog id

    选择最后一个物理版本。

    注意：

    这只是为了选择“最完整的原材料”。

    最终 version_hash 会重新通过当前
    Naver plugin normalize() 计算。
    """

    working = (
        all_df.copy()
    )

    working[
        "_crawl_sort"
    ] = (
        working[
            "crawled_at"
        ]
        .map(
            crawled_sort_value
        )
    )

    working = (
        working.sort_values(
            [
                "record_uid",
                "_crawl_sort",
                "_catalog_id",
            ]
        )
        .drop_duplicates(
            subset=[
                "record_uid",
            ],
            keep="last",
        )
        .reset_index(
            drop=True
        )
    )

    return working


# ============================================================
# SeenStore
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


# ============================================================
# Canonical reconstruction
# ============================================================


def rebuild_clean_dataframe(
    *,
    latest_df: pd.DataFrame,
    seen_hashes: dict[
        str,
        str,
    ],
) -> pd.DataFrame:

    rows: list[
        dict[str, Any]
    ] = []

    uid_mismatch = 0
    hash_mismatch = 0
    missing_seen = 0
    rebuild_failed = 0

    # ========================================================
    # Current production Naver normalizer
    # ========================================================

    from crawl_framework.sites.naver_finance.plugin import (
        NaverFinancePlugin,
    )

    plugin = NaverFinancePlugin()

    # ========================================================
    # Rebuild every logical record
    # ========================================================

    for _, source_row in latest_df.iterrows():

        row = source_row.to_dict()

        old_uid = str(
            row[
                "record_uid"
            ]
        )

        try:

            record = rebuild_record(
                plugin=plugin,
                row=row,
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

        # ====================================================
        # 1. record_uid must remain stable
        # ====================================================

        if (
            record.record_uid
            != old_uid
        ):

            uid_mismatch += 1

            print(
                "[UID MISMATCH]",
                row.get(
                    "source_id"
                ),
                "old=",
                old_uid,
                "new=",
                record.record_uid,
            )

            continue

        # ====================================================
        # 2. SeenStore must contain this record
        # ====================================================

        seen_hash = seen_hashes.get(
            record.record_uid
        )

        if seen_hash is None:

            missing_seen += 1

            print(
                "[MISSING SEEN]",
                row.get(
                    "source_id"
                ),
                record.record_uid,
            )

            continue

        # ====================================================
        # 3. Current canonical hash must equal SeenStore
        # ====================================================

        if (
            seen_hash
            != record.version_hash
        ):

            hash_mismatch += 1

            print(
                "[HASH MISMATCH]",
                row.get(
                    "source_id"
                ),
            )

            print(
                "  seen    =",
                seen_hash,
            )

            print(
                "  rebuilt =",
                record.version_hash,
            )

            continue

        # ====================================================
        # 4. Start from historical physical row
        # ====================================================
        #
        # This deliberately preserves:
        #
        #     event_time
        #     updated_at
        #     crawled_at
        #
        # in their original Arrow-compatible representation.
        #
        # Then all semantic/canonical fields are overwritten
        # using the CURRENT normalizer.
        # ====================================================

        clean = dict(
            row
        )

        clean[
            "schema_version"
        ] = (
            record.schema_version
        )

        clean[
            "record_uid"
        ] = (
            record.record_uid
        )

        clean[
            "version_hash"
        ] = (
            record.version_hash
        )

        clean[
            "mutation_policy"
        ] = mutation_policy_text(
            record.mutation_policy
        )

        clean[
            "site_id"
        ] = (
            record.site_id
        )

        clean[
            "country"
        ] = (
            record.country
        )

        clean[
            "dataset"
        ] = (
            record.dataset
        )

        clean[
            "source_id"
        ] = (
            record.source_id
        )

        clean[
            "scope_type"
        ] = (
            record.scope_type
        )

        clean[
            "scope_id"
        ] = (
            record.scope_id
        )

        clean[
            "instrument_id"
        ] = (
            record.instrument_id
        )

        clean[
            "title"
        ] = (
            record.title
        )

        clean[
            "content"
        ] = (
            record.content
        )

        clean[
            "author_id"
        ] = (
            record.author_id
        )

        clean[
            "author_name"
        ] = (
            record.author_name
        )

        clean[
            "source_url"
        ] = (
            record.source_url
        )

        clean[
            "payload_json"
        ] = compact_json(
            record.payload
        )

        clean[
            "relations_json"
        ] = compact_json(
            [
                {
                    "instrument_id": (
                        relation.instrument_id
                    ),
                    "relation_type": (
                        relation.relation_type
                    ),
                    "confidence": (
                        relation.confidence
                    ),
                }
                for relation
                in record.relations
            ]
        )

        rows.append(
            clean
        )

    # ========================================================
    # Validation summary
    # ========================================================

    print()
    print(
        "===== REBUILD VALIDATION ====="
    )

    print(
        "input unique rows =",
        len(
            latest_df
        ),
    )

    print(
        "clean rows =",
        len(
            rows
        ),
    )

    print(
        "uid_mismatch =",
        uid_mismatch,
    )

    print(
        "hash_mismatch =",
        hash_mismatch,
    )

    print(
        "missing_seen =",
        missing_seen,
    )

    print(
        "rebuild_failed =",
        rebuild_failed,
    )

    # ========================================================
    # Safety gate
    # ========================================================

    if (
        uid_mismatch
        or hash_mismatch
        or missing_seen
        or rebuild_failed
    ):

        raise RuntimeError(
            "compaction validation failed; "
            "refusing to write clean parquet"
        )

    return pd.DataFrame(
        rows
    )

# ============================================================
# Write clean Parquet
# ============================================================


def write_clean_partitions(
    *,
    clean_df: pd.DataFrame,
    reference_schema: pa.Schema,
    output_dir: Path,
) -> list[
    Path
]:
    """
    不把所有股票/日期塞进一个大文件。

    保留原 partition directory：

        site
        country
        dataset
        year
        month
        day
        bucket

    每个 partition 生成一个 compacted parquet。
    """

    output_paths: list[
        Path
    ] = []

    schema_names = list(
        reference_schema.names
    )

    grouped = clean_df.groupby(
        "_partition_directory",
        sort=True,
    )

    for (
        partition_dir,
        group,
    ) in grouped:

        group = (
            group.copy()
        )

        # ================================================
        # Only physical Parquet schema columns
        # ================================================

        missing_columns = [
            column
            for column
            in schema_names
            if column
            not in group.columns
        ]

        if missing_columns:

            raise RuntimeError(
                "clean dataframe missing "
                "schema columns: "
                f"{missing_columns}"
            )

        physical = group[
            schema_names
        ].copy()

        # ================================================
        # Stable local output
        # ================================================

        destination_dir = (
            output_dir
            / Path(
                partition_dir
            )
        )

        destination_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        file_name = (
            "part-compacted-"
            + uuid.uuid4().hex
            + ".parquet"
        )

        output_path = (
            destination_dir
            / file_name
        )

        table = (
            pa.Table.from_pandas(
                physical,
                schema=reference_schema,
                preserve_index=False,
                safe=True,
            )
        )

        pq.write_table(
            table,
            output_path,
            compression="zstd",
        )

        print()
        print(
            "[WRITE]",
            output_path,
        )

        print(
            "  rows =",
            table.num_rows,
        )

        output_paths.append(
            output_path
        )

    return output_paths


# ============================================================
# Verify written output
# ============================================================


def verify_output(
    *,
    output_paths: list[
        Path
    ],
    expected_uids: set[
        str
    ],
    seen_hashes: dict[
        str,
        str,
    ],
) -> None:

    frames = []

    for path in output_paths:

        table = pq.read_table(
            path
        )

        df = table.to_pandas()

        frames.append(
            df
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

    output_uids = set(
        combined[
            "record_uid"
        ]
    )

    duplicate_rows = (
        len(
            combined
        )
        -
        combined[
            "record_uid"
        ].nunique()
    )

    hash_mismatch = 0

    for _, row in (
        combined.iterrows()
    ):

        expected_hash = (
            seen_hashes.get(
                str(
                    row[
                        "record_uid"
                    ]
                )
            )
        )

        if (
            expected_hash
            !=
            str(
                row[
                    "version_hash"
                ]
            )
        ):

            hash_mismatch += 1

    print()
    print(
        "========================================"
    )

    print(
        "OUTPUT VERIFICATION"
    )

    print(
        "========================================"
    )

    print(
        "output files =",
        len(
            output_paths
        ),
    )

    print(
        "output rows =",
        len(
            combined
        ),
    )

    print(
        "unique record_uid =",
        combined[
            "record_uid"
        ].nunique(),
    )

    print(
        "duplicate rows =",
        duplicate_rows,
    )

    print(
        "missing uid =",
        len(
            expected_uids
            -
            output_uids
        ),
    )

    print(
        "unexpected uid =",
        len(
            output_uids
            -
            expected_uids
        ),
    )

    print(
        "hash mismatch =",
        hash_mismatch,
    )

    if (
        duplicate_rows != 0
        or
        output_uids
        !=
        expected_uids
        or
        hash_mismatch != 0
    ):

        raise RuntimeError(
            "written compacted parquet "
            "verification failed"
        )


# ============================================================
# Main
# ============================================================


def main() -> int:

    args = parse_args()

    config = (
        load_default_config()
    )

    output_dir = Path(
        args.output_dir
    ).resolve()

    seen_db = Path(
        args.seen_db
    ).resolve()

    # ========================================================
    # Output safety
    # ========================================================

    if output_dir.exists():

        if not (
            args.overwrite_output
        ):

            raise RuntimeError(
                "output directory already exists: "
                f"{output_dir}\n"
                "Use --overwrite-output "
                "to replace it."
            )

        shutil.rmtree(
            output_dir
        )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    # ========================================================
    # Catalog
    # ========================================================

    catalog_rows = (
        load_catalog_rows(
            config
        )
    )

    if not catalog_rows:

        raise RuntimeError(
            "no Naver forum_post "
            "catalog files found"
        )

    print(
        "catalog files =",
        len(
            catalog_rows
        ),
    )

    print(
        "catalog physical rows =",
        sum(
            int(
                row[2]
            )
            for row
            in catalog_rows
        ),
    )

    # ========================================================
    # Load historical
    # ========================================================

    (
        frames,
        reference_schema,
    ) = load_historical_frames(
        catalog_rows=catalog_rows,
        remote_host=str(
            config.server.host
        ).strip(),
        remote_user=str(
            config.server.user
        ).strip(),
        ssh_port=args.ssh_port,
    )

    all_df = pd.concat(
        frames,
        ignore_index=True,
    )

    all_df[
        "record_uid"
    ] = (
        all_df[
            "record_uid"
        ]
        .astype(str)
    )

    print()
    print(
        "===== SOURCE SUMMARY ====="
    )

    print(
        "physical rows =",
        len(
            all_df
        ),
    )

    print(
        "unique uid =",
        all_df[
            "record_uid"
        ].nunique(),
    )

    print(
        "duplicate physical rows =",
        (
            len(
                all_df
            )
            -
            all_df[
                "record_uid"
            ].nunique()
        ),
    )

    # ========================================================
    # Latest per UID
    # ========================================================

    latest_df = (
        choose_latest_rows(
            all_df
        )
    )

    print(
        "selected logical rows =",
        len(
            latest_df
        ),
    )

    # ========================================================
    # SeenStore
    # ========================================================

    seen_hashes = (
        load_seen_hashes(
            seen_db
        )
    )

    source_uids = set(
        latest_df[
            "record_uid"
        ]
    )

    seen_uids = set(
        seen_hashes
    )

    print()
    print(
        "===== PRE-WRITE UID CHECK ====="
    )

    print(
        "source uid =",
        len(
            source_uids
        ),
    )

    print(
        "seen uid =",
        len(
            seen_uids
        ),
    )

    print(
        "source only =",
        len(
            source_uids
            -
            seen_uids
        ),
    )

    print(
        "seen only =",
        len(
            seen_uids
            -
            source_uids
        ),
    )

    if (
        source_uids
        !=
        seen_uids
    ):

        raise RuntimeError(
            "SeenStore/Parquet UID sets "
            "do not match"
        )

    # ========================================================
    # Rebuild canonical
    # ========================================================

    clean_df = (
        rebuild_clean_dataframe(
            latest_df=latest_df,
            seen_hashes=seen_hashes,
        )
    )

    # ========================================================
    # Write
    # ========================================================

    output_paths = (
        write_clean_partitions(
            clean_df=clean_df,
            reference_schema=reference_schema,
            output_dir=output_dir,
        )
    )

    # ========================================================
    # Verify
    # ========================================================

    verify_output(
        output_paths=output_paths,
        expected_uids=source_uids,
        seen_hashes=seen_hashes,
    )

    # ========================================================
    # Final
    # ========================================================

    print()
    print(
        "========================================"
    )

    print(
        "COMPACTION DRY-RUN SUCCESS"
    )

    print(
        "========================================"
    )

    print(
        "source physical rows =",
        len(
            all_df
        ),
    )

    print(
        "clean logical rows =",
        len(
            clean_df
        ),
    )

    print(
        "removed duplicates =",
        (
            len(
                all_df
            )
            -
            len(
                clean_df
            )
        ),
    )

    print(
        "output directory =",
        output_dir,
    )

    print()
    print(
        "NO PostgreSQL rows changed."
    )

    print(
        "NO NAS files changed."
    )

    print(
        "NO historical files deleted."
    )

    return 0


if __name__ == "__main__":

    raise SystemExit(
        main()
    )