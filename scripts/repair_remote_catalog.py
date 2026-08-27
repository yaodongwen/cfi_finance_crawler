from __future__ import annotations

import subprocess
from dataclasses import dataclass

from crawl_framework.config import (
    load_default_config,
)

from crawl_framework.storage.postgres import (
    PostgresCatalog,
)

from crawl_framework.storage.postgres_connection import (
    make_config_postgres_connection_factory,
)


@dataclass(
    frozen=True,
    slots=True,
)
class LocalCatalogFile:

    id: int

    file_path: str

    row_count: int

    file_size: int


def remote_file_exists(
    *,
    host: str,
    user: str,
    port: int,
    remote_path: str,
) -> bool:
    """
    通过 SSH 检查远端文件是否存在。
    """

    target = (
        f"{user}@{host}"
    )

    command = [
        "ssh",
        "-p",
        str(port),
        target,
        "test",
        "-f",
        remote_path,
    ]

    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )

    return (
        result.returncode
        == 0
    )


def remote_file_size(
    *,
    host: str,
    user: str,
    port: int,
    remote_path: str,
) -> int | None:
    """
    获取远端文件大小。

    当前服务器为 Linux，
    使用 stat -c %s。
    """

    target = (
        f"{user}@{host}"
    )

    command = [
        "ssh",
        "-p",
        str(port),
        target,
        "stat",
        "-c",
        "%s",
        remote_path,
    ]

    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )

    if (
        result.returncode
        != 0
    ):

        return None

    value = (
        result.stdout
        .strip()
    )

    if not value:

        return None

    try:

        return int(
            value
        )

    except ValueError:

        return None


def load_local_catalog_files(
    connection,
) -> list[
    LocalCatalogFile
]:
    """
    查询 PostgreSQL 中真正需要远端 Catalog 修复的文件。

    待修复文件必须同时满足：

        storage_status = 'local'
        lifecycle_status = 'active'

    这样可以避免将：

        local + superseded

    的历史文件重新修复为 uploaded。
    """

    sql = """
    SELECT
        id,
        file_path,
        row_count,
        file_size

    FROM marketdata.data_files

    WHERE
        storage_status = 'local'
        AND lifecycle_status = 'active'

    ORDER BY id
    """

    with connection.cursor() as cursor:

        cursor.execute(
            sql
        )

        rows = (
            cursor.fetchall()
        )

    return [
        LocalCatalogFile(
            id=int(
                row[0]
            ),
            file_path=str(
                row[1]
            ),
            row_count=int(
                row[2]
            ),
            file_size=int(
                row[3]
            ),
        )
        for row in rows
    ]

def build_remote_path(
    *,
    remote_root: str,
    relative_path: str,
) -> str:

    return (
        remote_root.rstrip("/")
        + "/"
        + relative_path.lstrip("/")
    )


def main() -> int:

    config = (
        load_default_config()
    )

    connection_factory = (
        make_config_postgres_connection_factory(
            config
        )
    )

    connection = (
        connection_factory()
    )

    catalog = (
        PostgresCatalog(
            connection
        )
    )

    host = (
        config.server.host
    )

    user = (
        config.server.user
    )

    port = int(
        config.sync.ssh_port
    )

    remote_root = (
        str(
            config.storage.server_warehouse
        )
    )

    repaired = 0

    missing = 0

    size_mismatch = 0

    try:

        files = (
            load_local_catalog_files(
                connection
            )
        )

        print(
            "========================================"
        )

        print(
            "Catalog remote repair"
        )

        print(
            "========================================"
        )

        print(
            f"local records: {len(files)}"
        )

        print()

        for item in files:

            remote_path = (
                build_remote_path(
                    remote_root=(
                        remote_root
                    ),
                    relative_path=(
                        item.file_path
                    ),
                )
            )

            print(
                f"[CHECK] id={item.id}"
            )

            print(
                f"        file={item.file_path}"
            )

            print(
                f"        remote={remote_path}"
            )

            exists = (
                remote_file_exists(
                    host=host,
                    user=user,
                    port=port,
                    remote_path=(
                        remote_path
                    ),
                )
            )

            if not exists:

                missing += 1

                print(
                    "        result=MISSING"
                )

                print()

                continue

            size = (
                remote_file_size(
                    host=host,
                    user=user,
                    port=port,
                    remote_path=(
                        remote_path
                    ),
                )
            )

            if (
                size
                != item.file_size
            ):

                size_mismatch += 1

                print(
                    "        result=SIZE_MISMATCH"
                )

                print(
                    "        expected="
                    f"{item.file_size}"
                )

                print(
                    "        actual="
                    f"{size}"
                )

                print()

                continue

            catalog.mark_uploaded(
                file_path=(
                    item.file_path
                ),
                remote_path=(
                    remote_path
                ),
            )

            repaired += 1

            print(
                "        result=REPAIRED"
            )

            print()

        print(
            "========================================"
        )

        print(
            "Summary"
        )

        print(
            "========================================"
        )

        print(
            f"repaired={repaired}"
        )

        print(
            f"missing={missing}"
        )

        print(
            "size_mismatch="
            f"{size_mismatch}"
        )

        return 0

    finally:

        connection.close()


if __name__ == "__main__":

    raise SystemExit(
        main()
    )