from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import subprocess
import tempfile

from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from crawl_framework.config import (
    load_default_config,
)

from crawl_framework.core.plugin import (
    CrawlScope,
)

from crawl_framework.storage.postgres import (
    PostgresCatalog,
)

from crawl_framework.sites.naver_finance.plugin import (
    NaverFinancePlugin,
)

from crawl_framework.storage.postgres_connection import (
    make_config_postgres_connection_factory,
)

from crawl_framework.storage.seen_store import (
    SQLiteSeenStore,
)


# ============================================================
# Constants
# ============================================================

SITE_ID = "naver_finance"
DATASET = "forum_post"


# ============================================================
# Helpers
# ============================================================


def parse_args() -> argparse.Namespace:
    """
    默认 dry-run。

    真正执行迁移时：

        --apply
    """

    parser = argparse.ArgumentParser(
        description=(
            "Rebuild Naver forum_post SeenStore "
            "version hashes using the current "
            "canonical normalization rules."
        )
    )

    parser.add_argument(
        "--seen-db",
        default="state/seen.sqlite3",
        help=(
            "Path to SQLite SeenStore database. "
            "Default: state/seen.sqlite3"
        ),
    )

    parser.add_argument(
        "--ssh-port",
        type=int,
        default=22,
        help=(
            "SSH port used to download Parquet "
            "files from remote storage."
        ),
    )

    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Actually update SeenStore. "
            "Without this flag the script is dry-run."
        ),
    )

    return parser.parse_args()


def parse_payload(
    value: Any,
) -> dict[str, Any]:
    """
    解析历史 parquet payload_json。
    """

    if value is None:
        return {}

    if isinstance(
        value,
        dict,
    ):
        return value

    if not isinstance(
        value,
        str,
    ):
        return {}

    try:
        result = json.loads(
            value
        )
    except Exception:
        return {}

    if not isinstance(
        result,
        dict,
    ):
        return {}

    return result


def optional_text(
    value: Any,
) -> str | None:
    """
    将 parquet / pandas 中的值安全转成文本。
    """

    if value is None:
        return None

    # pandas NaN
    try:
        if value != value:
            return None
    except Exception:
        pass

    text = str(
        value
    ).strip()

    if not text:
        return None

    return text


def extract_code(
    row: dict[str, Any],
) -> str:
    """
    从历史记录恢复 Naver 股票代码。

    优先顺序：

        payload_json.code
        instrument_id = XKRX:042700
        scope_id      = XKRX:042700
    """

    payload = parse_payload(
        row.get(
            "payload_json"
        )
    )

    code = optional_text(
        payload.get(
            "code"
        )
    )

    if code:
        return code

    for field in (
        "instrument_id",
        "scope_id",
    ):

        value = optional_text(
            row.get(
                field
            )
        )

        if not value:
            continue

        if ":" in value:

            prefix, symbol = (
                value.split(
                    ":",
                    1,
                )
            )

            if (
                prefix == "XKRX"
                and symbol
            ):
                return symbol

    raise ValueError(
        "cannot recover Naver instrument code "
        f"for source_id={row.get('source_id')!r}"
    )


def crawled_sort_value(
    value: Any,
) -> str:
    """
    用于同一个 record_uid 多个历史版本之间
    选择最后一次抓取的记录。

    ISO / pandas timestamp 转成字符串后，
    对当前数据已经足够稳定。
    """

    if value is None:
        return ""

    try:
        if value != value:
            return ""
    except Exception:
        pass

    if isinstance(
        value,
        datetime,
    ):
        return value.isoformat()

    return str(
        value
    )


def backup_sqlite(
    source_path: Path,
) -> Path:
    """
    使用 SQLite backup API 创建一致性备份。

    比直接 shutil.copy seen.sqlite3 更安全，
    因为数据库使用 WAL。
    """

    timestamp = (
        datetime.now()
        .strftime(
            "%Y%m%d-%H%M%S"
        )
    )

    backup_path = (
        source_path.parent
        / (
            source_path.name
            + ".before_forum_hash_migration."
            + timestamp
            + ".bak"
        )
    )

    source_conn = sqlite3.connect(
        str(
            source_path
        )
    )

    try:

        backup_conn = sqlite3.connect(
            str(
                backup_path
            )
        )

        try:

            source_conn.backup(
                backup_conn
            )

        finally:

            backup_conn.close()

    finally:

        source_conn.close()

    return backup_path


# ============================================================
# PostgreSQL catalog
# ============================================================

def load_remote_parquet_paths(
    config,
) -> list[str]:
    """
    从 PostgreSQL Catalog 获取当前有效的
    Naver forum_post Parquet。

    有效文件统一定义为：

        storage_status = 'uploaded'
        lifecycle_status = 'active'

    不处理：

        local
        superseded
        archived
        remote_path IS NULL
        其他 site
        其他 dataset
    """

    connection_factory = (
        make_config_postgres_connection_factory(
            config
        )
    )

    conn = (
        connection_factory()
    )

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

    finally:

        conn.close()

    result: list[str] = []

    seen: set[str] = set()

    for item in files:

        remote_path = (
            str(
                item.remote_path
            ).strip()
            if item.remote_path
            else ""
        )

        if not remote_path:
            continue

        if remote_path in seen:
            continue

        seen.add(
            remote_path
        )

        result.append(
            remote_path
        )

    return result

# ============================================================
# Remote download
# ============================================================


def download_remote_file(
    *,
    remote_host: str,
    remote_user: str,
    ssh_port: int,
    remote_path: str,
    destination: Path,
) -> None:
    """
    将一个 NAS parquet 临时拉到本机。
    """

    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    remote_spec = (
        f"{remote_user}@"
        f"{remote_host}:"
        f"{remote_path}"
    )

    command = [
        "scp",
        "-q",
        "-P",
        str(
            ssh_port
        ),
        remote_spec,
        str(
            destination
        ),
    ]

    subprocess.run(
        command,
        check=True,
    )


# ============================================================
# Historical row collection
# ============================================================


def collect_latest_rows(
    *,
    remote_paths: list[str],
    remote_host: str,
    remote_user: str,
    ssh_port: int,
) -> tuple[
    dict[str, dict[str, Any]],
    int,
]:
    """
    扫描所有历史 Naver forum_post parquet。

    同一个 record_uid 可能由于旧 version_hash 规则
    出现在多个 parquet 中。

    我们只保留 crawled_at 最新的一份，
    再按当前 normalize 规则重算版本。
    """

    latest_by_uid: dict[
        str,
        dict[str, Any],
    ] = {}

    scanned_rows = 0

    with tempfile.TemporaryDirectory(
        prefix="crawl_seen_migration_"
    ) as temp_dir_text:

        temp_dir = Path(
            temp_dir_text
        )

        for index, remote_path in enumerate(
            remote_paths,
            start=1,
        ):

            file_name = Path(
                remote_path
            ).name

            local_path = (
                temp_dir
                / f"{index:06d}-{file_name}"
            )

            print(
                f"[download {index}/{len(remote_paths)}] "
                f"{remote_path}"
            )

            download_remote_file(
                remote_host=remote_host,
                remote_user=remote_user,
                ssh_port=ssh_port,
                remote_path=remote_path,
                destination=local_path,
            )

            table = pq.read_table(
                local_path
            )

            df = table.to_pandas()

            for row in df.to_dict(
                orient="records"
            ):

                if (
                    optional_text(
                        row.get(
                            "site_id"
                        )
                    )
                    != SITE_ID
                ):
                    continue

                if (
                    optional_text(
                        row.get(
                            "dataset"
                        )
                    )
                    != DATASET
                ):
                    continue

                scanned_rows += 1

                record_uid = (
                    optional_text(
                        row.get(
                            "record_uid"
                        )
                    )
                )

                if not record_uid:

                    raise ValueError(
                        "historical parquet row "
                        "has no record_uid"
                    )

                current = (
                    latest_by_uid.get(
                        record_uid
                    )
                )

                if current is None:

                    latest_by_uid[
                        record_uid
                    ] = row

                    continue

                old_sort = (
                    crawled_sort_value(
                        current.get(
                            "crawled_at"
                        )
                    )
                )

                new_sort = (
                    crawled_sort_value(
                        row.get(
                            "crawled_at"
                        )
                    )
                )

                if (
                    new_sort
                    >=
                    old_sort
                ):

                    latest_by_uid[
                        record_uid
                    ] = row

    return (
        latest_by_uid,
        scanned_rows,
    )


# ============================================================
# Canonical reconstruction
# ============================================================

def rebuild_record(
    *,
    plugin: NaverFinancePlugin,
    row: dict[str, Any],
):
    """
    将历史 parquet 行重新送入当前
    NaverFinancePlugin.normalize()。

    兼容两种历史结构：

    旧结构：
        content / nickname / detail_url
        主要保存在 payload_json 中。

    新结构：
        content
        author_name
        source_url
        已经进入 CanonicalRecord 顶层字段。

    迁移时必须同时兼容两者，否则旧记录会因为
    顶层字段为空而计算出错误的 version_hash。
    """

    # ========================================================
    # Identity
    # ========================================================

    source_id = optional_text(
        row.get(
            "source_id"
        )
    )

    if not source_id:

        raise ValueError(
            "historical row has no source_id"
        )

    # ========================================================
    # Historical payload
    # ========================================================

    payload = parse_payload(
        row.get(
            "payload_json"
        )
    )

    # ========================================================
    # Instrument
    # ========================================================

    code = extract_code(
        row
    )

    instrument_id = (
        optional_text(
            row.get(
                "instrument_id"
            )
        )
        or
        f"XKRX:{code}"
    )

    scope_id = (
        optional_text(
            row.get(
                "scope_id"
            )
        )
        or
        instrument_id
    )

    scope_type = (
        optional_text(
            row.get(
                "scope_type"
            )
        )
        or
        "instrument"
    )

    scope = CrawlScope(
        scope_type=scope_type,
        source_key=code,
        scope_id=scope_id,
        metadata={
            "code":
                code,

            "instrument_id":
                instrument_id,
        },
    )

    # ========================================================
    # Stable content
    #
    # 新 schema：
    #     顶层字段
    #
    # 旧 schema：
    #     payload_json
    #
    # 必须优先使用顶层，新字段不存在时再 fallback。
    # ========================================================

    title = (
        optional_text(
            row.get(
                "title"
            )
        )
        or
        optional_text(
            payload.get(
                "title"
            )
        )
    )

    content = (
        optional_text(
            row.get(
                "content"
            )
        )
        or
        optional_text(
            payload.get(
                "content"
            )
        )
    )

    nickname = (
        optional_text(
            row.get(
                "author_name"
            )
        )
        or
        optional_text(
            payload.get(
                "nickname"
            )
        )
    )

    # ========================================================
    # Event time
    # ========================================================

    written_at = (
        row.get(
            "event_time"
        )
    )

    # ========================================================
    # Build raw in the SAME semantic shape
    # used by today's crawler.
    #
    # 注意：
    #
    # view_count / recommend / dislike
    # 不需要恢复。
    #
    # detail_url 也不需要恢复。
    #
    # 当前 normalize() 会自己生成稳定 canonical URL：
    #
    #   ?code=...&nid=...
    #
    # 这样 page=1/page=2 不会影响 version_hash。
    # ========================================================

    raw = {
        "nid":
            source_id,

        "code":
            code,

        "title":
            title,

        "content":
            content,

        "nickname":
            nickname,

        "written_at":
            written_at,
    }

    record = plugin.normalize(
        DATASET,
        raw,
        scope,
    )

    if record is None:

        raise RuntimeError(
            "plugin returned None "
            f"for source_id={source_id}"
        )

    return record

# ============================================================
# Migration
# ============================================================


def run_migration(
    *,
    seen_db: Path,
    remote_paths: list[str],
    remote_host: str,
    remote_user: str,
    ssh_port: int,
    apply: bool,
) -> None:

    plugin = (
        NaverFinancePlugin()
    )

    print()
    print(
        "===== COLLECT HISTORICAL PARQUET ====="
    )

    (
        latest_rows,
        scanned_rows,
    ) = collect_latest_rows(
        remote_paths=remote_paths,
        remote_host=remote_host,
        remote_user=remote_user,
        ssh_port=ssh_port,
    )

    print()
    print(
        "historical rows scanned =",
        scanned_rows,
    )

    print(
        "unique record_uid =",
        len(
            latest_rows
        ),
    )

    # ========================================================
    # Rebuild current hashes
    # ========================================================

    rebuilt: dict[
        str,
        str,
    ] = {}

    uid_mismatch = 0
    rebuild_failed = 0

    print()
    print(
        "===== REBUILD CURRENT HASHES ====="
    )

    for old_uid, row in (
        latest_rows.items()
    ):

        try:

            record = rebuild_record(
                plugin=plugin,
                row=row,
            )

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
                    old_uid,
                    record.record_uid,
                )

                continue

            rebuilt[
                record.record_uid
            ] = (
                record.version_hash
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

    print()
    print(
        "rebuilt =",
        len(
            rebuilt
        ),
    )

    print(
        "uid_mismatch =",
        uid_mismatch,
    )

    print(
        "rebuild_failed =",
        rebuild_failed,
    )

    # ========================================================
    # Compare SeenStore
    # ========================================================

    unchanged = 0
    needs_update = 0
    missing_seen = 0

    updates: list[
        tuple[str, str]
    ] = []

    with SQLiteSeenStore(
        seen_db
    ) as store:

        print()
        print(
            "seen_store count before =",
            store.count(),
        )

        for (
            record_uid,
            version_hash,
        ) in rebuilt.items():

            if not store.contains(
                record_uid
            ):

                missing_seen += 1

                continue

            result = store.inspect(
                record_uid,
                version_hash,
            )

            if (
                result.decision
                == "unchanged"
            ):

                unchanged += 1

                continue

            if (
                result.decision
                == "updated"
            ):

                needs_update += 1

                updates.append(
                    (
                        record_uid,
                        version_hash,
                    )
                )

                continue

            # 理论上 contains=True 时不应该出现 new。
            raise RuntimeError(
                "unexpected SeenStore decision "
                f"{result.decision!r} "
                f"for {record_uid}"
            )

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

        if not apply:

            print()
            print(
                "DRY RUN ONLY"
            )

            print(
                "No SeenStore rows were modified."
            )

            return

        # ====================================================
        # Apply
        # ====================================================

        if (
            uid_mismatch
            or rebuild_failed
        ):

            raise RuntimeError(
                "refusing migration because "
                "uid_mismatch or rebuild_failed "
                "is non-zero"
            )

        print()
        print(
            "===== APPLY ====="
        )

        migrated = (
            store.commit_many(
                updates
            )
        )

        print(
            "migrated =",
            migrated,
        )

        print(
            "seen_store count after =",
            store.count(),
        )


# ============================================================
# Main
# ============================================================


def main() -> int:

    args = parse_args()

    config = (
        load_default_config()
    )

    seen_db = Path(
        args.seen_db
    ).resolve()

    if not seen_db.exists():

        raise FileNotFoundError(
            f"SeenStore not found: {seen_db}"
        )

    remote_host = str(
        config.server.host
    ).strip()

    remote_user = str(
        config.server.user
    ).strip()

    if not remote_host:

        raise ValueError(
            "config.server.host is empty"
        )

    if not remote_user:

        raise ValueError(
            "config.server.user is empty"
        )

    remote_paths = (
        load_remote_parquet_paths(
            config
        )
    )

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
        "remote parquet files =",
        len(
            remote_paths
        ),
    )

    if not remote_paths:

        print(
            "No uploaded Naver forum_post "
            "Parquet files found."
        )

        return 0

    # ========================================================
    # Apply 前做 SQLite 一致性备份
    # ========================================================

    if args.apply:

        backup_path = (
            backup_sqlite(
                seen_db
            )
        )

        print(
            "backup =",
            backup_path,
        )

    run_migration(
        seen_db=seen_db,
        remote_paths=remote_paths,
        remote_host=remote_host,
        remote_user=remote_user,
        ssh_port=args.ssh_port,
        apply=args.apply,
    )

    return 0


if __name__ == "__main__":

    raise SystemExit(
        main()
    )