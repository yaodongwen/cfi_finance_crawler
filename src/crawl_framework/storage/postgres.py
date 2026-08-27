from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from crawl_framework.storage.parquet_writer import (
    ParquetFileInfo,
)


DEFAULT_SCHEMA = "marketdata"


@dataclass(
    frozen=True,
    slots=True,
)
class DataFileRecord:
    """
    PostgreSQL 中 data_files 表对应的核心对象。

    storage_status:
        描述文件物理存储状态。

        local
            文件当前只在本地。

        uploaded
            文件已经成功上传远端。

    lifecycle_status:
        描述文件逻辑生命周期。

        active
            当前有效，正常查询应使用。

        superseded
            已被新的 compacted / replacement
            文件替代，但文件和 Catalog 仍保留，
            便于审计和回滚。

        archived
            已归档，不参与正常查询。
    """

    site_id: str

    country: str

    dataset: str

    partition_date: str

    bucket: str

    file_path: str

    sha256: str

    row_count: int

    file_size: int

    min_event_time: datetime | None

    max_event_time: datetime | None

    schema_version: int

    storage_status: str = "local"

    lifecycle_status: str = "active"

    remote_path: str | None = None


    @classmethod
    def from_parquet_info(
        cls,
        info: ParquetFileInfo,
    ) -> "DataFileRecord":

        return cls(
            site_id=(
                info.partition.site_id
            ),

            country=(
                info.partition.country
            ),

            dataset=(
                info.partition.dataset
            ),

            partition_date=(
                info.partition
                .partition_date
                .isoformat()
            ),

            bucket=(
                info.partition
                .bucket_label
            ),

            file_path=(
                info.relative_path
                .as_posix()
            ),

            sha256=(
                info.sha256
            ),

            row_count=(
                info.row_count
            ),

            file_size=(
                info.file_size
            ),

            min_event_time=(
                info.min_event_time
            ),

            max_event_time=(
                info.max_event_time
            ),

            schema_version=(
                info.schema_version
            ),

            storage_status=(
                "local"
            ),

            lifecycle_status=(
                "active"
            ),

            remote_path=(
                None
            ),
        )

@dataclass(
    frozen=True,
    slots=True,
)
class CatalogDataFile:
    """
    PostgreSQL data_files 中可供读取的数据文件。

    与 DataFileRecord 的区别：

    DataFileRecord:
        主要用于注册 / 写入 Catalog。

    CatalogDataFile:
        主要用于从 Catalog 查询已有文件，
        包含数据库生成的 id。
    """

    id: int

    site_id: str

    country: str

    dataset: str

    partition_date: str

    bucket: str

    file_path: str

    sha256: str

    row_count: int

    file_size: int

    min_event_time: datetime | None

    max_event_time: datetime | None

    schema_version: int

    storage_status: str

    lifecycle_status: str

    remote_path: str | None

def validate_schema_name(
    value: str,
) -> str:
    """
    防止 schema 名称被直接拼入 SQL 时产生风险。
    """

    name = str(
        value
    ).strip()

    if not name:
        raise ValueError(
            "schema name cannot be empty"
        )

    if not (
        name[0].isalpha()
        or name[0] == "_"
    ):
        raise ValueError(
            f"invalid schema name: {name!r}"
        )

    for char in name:

        if not (
            char.isalnum()
            or char == "_"
        ):
            raise ValueError(
                f"invalid schema name: {name!r}"
            )

    return name


def build_schema_ddl(
    schema: str = DEFAULT_SCHEMA,
) -> tuple[str, ...]:
    """
    返回 Crawl Framework V2 的 PostgreSQL DDL。

    当前只生成 SQL，不执行。
    """

    schema = validate_schema_name(
        schema
    )

    statements = (

        f"""
        CREATE SCHEMA IF NOT EXISTS {schema}
        """,

        f"""
        CREATE TABLE IF NOT EXISTS {schema}.sources
        (
            site_id TEXT PRIMARY KEY,

            country TEXT NOT NULL,

            timezone TEXT,

            enabled BOOLEAN
                NOT NULL
                DEFAULT TRUE,

            created_at TIMESTAMPTZ
                NOT NULL
                DEFAULT NOW(),

            updated_at TIMESTAMPTZ
                NOT NULL
                DEFAULT NOW()
        )
        """,

        f"""
        CREATE TABLE IF NOT EXISTS {schema}.instruments
        (
            instrument_id TEXT PRIMARY KEY,

            country TEXT,

            market TEXT,

            symbol TEXT,

            name TEXT,

            currency TEXT,

            created_at TIMESTAMPTZ
                NOT NULL
                DEFAULT NOW(),

            updated_at TIMESTAMPTZ
                NOT NULL
                DEFAULT NOW()
        )
        """,

        f"""
        CREATE TABLE IF NOT EXISTS {schema}.source_instruments
        (
            site_id TEXT NOT NULL,

            source_symbol TEXT NOT NULL,

            instrument_id TEXT NOT NULL,

            source_url TEXT,

            metadata JSONB,

            created_at TIMESTAMPTZ
                NOT NULL
                DEFAULT NOW(),

            updated_at TIMESTAMPTZ
                NOT NULL
                DEFAULT NOW(),

            PRIMARY KEY
            (
                site_id,
                source_symbol
            )
        )
        """,

        f"""
        CREATE TABLE IF NOT EXISTS {schema}.data_files
        (
            id BIGSERIAL PRIMARY KEY,

            site_id TEXT NOT NULL,

            country TEXT NOT NULL,

            dataset TEXT NOT NULL,

            partition_date DATE NOT NULL,

            bucket TEXT NOT NULL,

            file_path TEXT NOT NULL UNIQUE,

            sha256 TEXT NOT NULL,

            row_count BIGINT NOT NULL,

            file_size BIGINT NOT NULL,

            min_event_time TIMESTAMPTZ,

            max_event_time TIMESTAMPTZ,

            schema_version INTEGER
                NOT NULL,

            storage_status TEXT
                NOT NULL
                DEFAULT 'local',

            lifecycle_status TEXT
                NOT NULL
                DEFAULT 'active',

            remote_path TEXT,

            created_at TIMESTAMPTZ
                NOT NULL
                DEFAULT NOW(),

            updated_at TIMESTAMPTZ
                NOT NULL
                DEFAULT NOW()
        )
        """,

        f"""
        ALTER TABLE {schema}.data_files

        ADD COLUMN IF NOT EXISTS
            lifecycle_status TEXT
            NOT NULL
            DEFAULT 'active'
        """,

        f"""
        CREATE INDEX IF NOT EXISTS
        idx_data_files_active_lookup

        ON {schema}.data_files
        (
            site_id,
            dataset,
            lifecycle_status,
            storage_status,
            partition_date,
            bucket
        )
        """,

        f"""
        CREATE INDEX IF NOT EXISTS
        idx_data_files_lookup

        ON {schema}.data_files
        (
            site_id,
            dataset,
            partition_date,
            bucket
        )
        """,

        f"""
        CREATE INDEX IF NOT EXISTS
        idx_data_files_event_range

        ON {schema}.data_files
        (
            min_event_time,
            max_event_time
        )
        """,

        f"""
        CREATE TABLE IF NOT EXISTS
        {schema}.record_registry
        (
            record_uid TEXT PRIMARY KEY,

            site_id TEXT NOT NULL,

            dataset TEXT NOT NULL,

            source_id TEXT NOT NULL,

            version_hash TEXT NOT NULL,

            mutation_policy TEXT NOT NULL,

            first_seen_at TIMESTAMPTZ
                NOT NULL
                DEFAULT NOW(),

            last_seen_at TIMESTAMPTZ
                NOT NULL
                DEFAULT NOW()
        )
        """,

        f"""
        CREATE INDEX IF NOT EXISTS
        idx_record_registry_source

        ON {schema}.record_registry
        (
            site_id,
            dataset,
            source_id
        )
        """,

        f"""
        CREATE TABLE IF NOT EXISTS
        {schema}.record_instrument
        (
            record_uid TEXT NOT NULL,

            instrument_id TEXT NOT NULL,

            relation_type TEXT NOT NULL,

            confidence DOUBLE PRECISION,

            created_at TIMESTAMPTZ
                NOT NULL
                DEFAULT NOW(),

            PRIMARY KEY
            (
                record_uid,
                instrument_id,
                relation_type
            )
        )
        """,

        f"""
        CREATE TABLE IF NOT EXISTS
        {schema}.crawl_runs
        (
            run_id UUID PRIMARY KEY,

            site_id TEXT NOT NULL,

            dataset TEXT,

            started_at TIMESTAMPTZ
                NOT NULL,

            finished_at TIMESTAMPTZ,

            status TEXT NOT NULL,

            records_seen BIGINT
                NOT NULL
                DEFAULT 0,

            records_new BIGINT
                NOT NULL
                DEFAULT 0,

            records_updated BIGINT
                NOT NULL
                DEFAULT 0,

            records_unchanged BIGINT
                NOT NULL
                DEFAULT 0,

            files_written BIGINT
                NOT NULL
                DEFAULT 0,

            error_count BIGINT
                NOT NULL
                DEFAULT 0,

            metadata JSONB
        )
        """,

        f"""
        CREATE TABLE IF NOT EXISTS
        {schema}.checkpoints
        (
            site_id TEXT NOT NULL,

            dataset TEXT NOT NULL,

            scope_type TEXT NOT NULL,

            source_key TEXT NOT NULL,

            state JSONB NOT NULL,

            updated_at TIMESTAMPTZ
                NOT NULL
                DEFAULT NOW(),

            PRIMARY KEY
            (
                site_id,
                dataset,
                scope_type,
                source_key
            )
        )
        """,
    )

    return tuple(
        statement.strip()
        for statement in statements
    )


def build_insert_data_file_sql(
    schema: str = DEFAULT_SCHEMA,
) -> str:
    """
    data_files UPSERT SQL。

    file_path 为唯一键。

    新文件注册时 lifecycle_status
    使用 DataFileRecord 中的值，
    默认 active。

    发生 file_path 冲突时：

        只更新文件元数据；

        不覆盖已有物理状态或生命周期状态。

    原因：

        storage_status / remote_path 属于显式上传状态，
        lifecycle_status 属于显式生命周期状态。
        不能因为 recovery、重复注册或重试
        让 uploaded 回退为 local、清空 remote_path，
        或把 superseded / archived 重新激活。
    """

    schema = validate_schema_name(
        schema
    )

    return f"""
    INSERT INTO {schema}.data_files
    (
        site_id,
        country,
        dataset,
        partition_date,
        bucket,
        file_path,
        sha256,
        row_count,
        file_size,
        min_event_time,
        max_event_time,
        schema_version,
        storage_status,
        lifecycle_status,
        remote_path
    )
    VALUES
    (
        %(site_id)s,
        %(country)s,
        %(dataset)s,
        %(partition_date)s,
        %(bucket)s,
        %(file_path)s,
        %(sha256)s,
        %(row_count)s,
        %(file_size)s,
        %(min_event_time)s,
        %(max_event_time)s,
        %(schema_version)s,
        %(storage_status)s,
        %(lifecycle_status)s,
        %(remote_path)s
    )

    ON CONFLICT(file_path)
    DO UPDATE SET

        sha256 =
            EXCLUDED.sha256,

        row_count =
            EXCLUDED.row_count,

        file_size =
            EXCLUDED.file_size,

        min_event_time =
            EXCLUDED.min_event_time,

        max_event_time =
            EXCLUDED.max_event_time,

        schema_version =
            EXCLUDED.schema_version,

        updated_at =
            NOW()
    """.strip()


def data_file_params(
    record: DataFileRecord,
) -> dict[str, Any]:
    """
    DataFileRecord -> SQL params。
    """

    return {
        "site_id":
            record.site_id,

        "country":
            record.country,

        "dataset":
            record.dataset,

        "partition_date":
            record.partition_date,

        "bucket":
            record.bucket,

        "file_path":
            record.file_path,

        "sha256":
            record.sha256,

        "row_count":
            record.row_count,

        "file_size":
            record.file_size,

        "min_event_time":
            record.min_event_time,

        "max_event_time":
            record.max_event_time,

        "schema_version":
            record.schema_version,

        "storage_status":
            record.storage_status,

        "lifecycle_status":
            record.lifecycle_status,

        "remote_path":
            record.remote_path,
    }


class PostgresCatalog:
    """
    PostgreSQL Catalog 接口。

    当前类不主动创建数据库连接。

    外部传入 DB-API compatible connection。

    例如未来：

        psycopg.connect(...)

    这样 unit test 可以使用 fake connection，
    不需要碰真实服务器。
    """

    def __init__(
        self,
        connection,
        *,
        schema: str = DEFAULT_SCHEMA,
    ) -> None:

        self.connection = (
            connection
        )

        self.schema = (
            validate_schema_name(
                schema
            )
        )


    def initialize_schema(
        self,
    ) -> None:

        statements = (
            build_schema_ddl(
                self.schema
            )
        )

        with self.connection.cursor() as cursor:

            for statement in statements:

                cursor.execute(
                    statement
                )

        self.connection.commit()


    def register_data_file(
        self,
        record: DataFileRecord,
    ) -> None:

        sql = (
            build_insert_data_file_sql(
                self.schema
            )
        )

        params = data_file_params(
            record
        )

        with self.connection.cursor() as cursor:

            cursor.execute(
                sql,
                params,
            )

        self.connection.commit()


    def register_parquet_file(
        self,
        info: ParquetFileInfo,
    ) -> DataFileRecord:

        record = (
            DataFileRecord
            .from_parquet_info(
                info
            )
        )

        self.register_data_file(
            record
        )

        return record

    def mark_uploaded(
        self,
        *,
        file_path: str | Path,
        remote_path: str,
    ) -> None:
        """
        标记 data_files 已完成远端上传。

        file_path 必须与 PostgreSQL data_files.file_path
        中保存的相对路径一致，例如：

            site=naver_finance/
            country=KR/
            dataset=forum_post/
            year=2026/
            month=08/
            day=26/
            bucket=5f/
            part-xxxx.parquet

        当前版本不读取 cursor.rowcount，
        以兼容现有 FakeCursor 和数据库 adapter。
        """

        normalized_file_path = (
            Path(
                file_path
            )
            .as_posix()
        )

        normalized_remote_path = (
            str(
                remote_path
            )
            .strip()
        )

        if not normalized_file_path:

            raise ValueError(
                "file_path cannot be empty"
            )

        if not normalized_remote_path:

            raise ValueError(
                "remote_path cannot be empty"
            )

        sql = f"""
        UPDATE {self.schema}.data_files

        SET
            storage_status = 'uploaded',
            remote_path = %(remote_path)s,
            updated_at = NOW()

        WHERE file_path = %(file_path)s
        """

        params = {
            "file_path":
                normalized_file_path,

            "remote_path":
                normalized_remote_path,
        }

        with self.connection.cursor() as cursor:

            cursor.execute(
                sql,
                params,
            )

        self.connection.commit()


    def mark_superseded(
        self,
        *,
        file_path: str | Path,
    ) -> None:
        """
        将一个 data_files 文件标记为已被替代。

        只修改 lifecycle_status。

        不修改：

            storage_status
            remote_path

        因为文件即使被 superseded，
        仍然可能真实存在于远端。
        """

        normalized_file_path = (
            Path(
                file_path
            )
            .as_posix()
        )

        if not normalized_file_path:

            raise ValueError(
                "file_path cannot be empty"
            )

        sql = f"""
        UPDATE {self.schema}.data_files

        SET
            lifecycle_status = 'superseded',
            updated_at = NOW()

        WHERE file_path = %(file_path)s
        """

        params = {
            "file_path":
                normalized_file_path,
        }

        with self.connection.cursor() as cursor:

            cursor.execute(
                sql,
                params,
            )

        self.connection.commit()


    def mark_active(
        self,
        *,
        file_path: str | Path,
    ) -> None:
        """
        将一个 data_files 文件恢复为 active。

        用于：

            rollback
            repair
            恢复被误 supersede 的文件
        """

        normalized_file_path = (
            Path(
                file_path
            )
            .as_posix()
        )

        if not normalized_file_path:

            raise ValueError(
                "file_path cannot be empty"
            )

        sql = f"""
        UPDATE {self.schema}.data_files

        SET
            lifecycle_status = 'active',
            updated_at = NOW()

        WHERE file_path = %(file_path)s
        """

        params = {
            "file_path":
                normalized_file_path,
        }

        with self.connection.cursor() as cursor:

            cursor.execute(
                sql,
                params,
            )

        self.connection.commit()


    def mark_archived(
        self,
        *,
        file_path: str | Path,
    ) -> None:
        """
        将 data_files 文件标记为 archived。

        archived 表示：

            Catalog 仍然保留
            文件可能仍然存在
            但正常查询不应读取
        """

        normalized_file_path = (
            Path(
                file_path
            )
            .as_posix()
        )

        if not normalized_file_path:

            raise ValueError(
                "file_path cannot be empty"
            )

        sql = f"""
        UPDATE {self.schema}.data_files

        SET
            lifecycle_status = 'archived',
            updated_at = NOW()

        WHERE file_path = %(file_path)s
        """

        params = {
            "file_path":
                normalized_file_path,
        }

        with self.connection.cursor() as cursor:

            cursor.execute(
                sql,
                params,
            )

        self.connection.commit()


    def list_active_data_files(
        self,
        *,
        site_id: str,
        dataset: str,
        country: str | None = None,
        partition_date: str | None = None,
        bucket: str | None = None,
    ) -> list[CatalogDataFile]:
        """
        查询当前真正可供正常读取的数据文件。

        一个文件必须同时满足：

            storage_status = 'uploaded'
            lifecycle_status = 'active'

        才属于有效远端数据集。

        可选过滤：

            country
            partition_date
            bucket

        返回顺序：

            partition_date
            bucket
            id

        这样 reader / compactor / repair 工具都可以
        统一使用这一接口，而不是各自手写 SQL。
        """

        normalized_site_id = str(
            site_id
        ).strip()

        normalized_dataset = str(
            dataset
        ).strip()

        if not normalized_site_id:

            raise ValueError(
                "site_id cannot be empty"
            )

        if not normalized_dataset:

            raise ValueError(
                "dataset cannot be empty"
            )

        conditions = [
            "site_id = %(site_id)s",
            "dataset = %(dataset)s",
            "storage_status = 'uploaded'",
            "lifecycle_status = 'active'",
        ]

        params: dict[str, Any] = {
            "site_id":
                normalized_site_id,

            "dataset":
                normalized_dataset,
        }

        if country is not None:

            normalized_country = str(
                country
            ).strip()

            if not normalized_country:

                raise ValueError(
                    "country cannot be empty"
                )

            conditions.append(
                "country = %(country)s"
            )

            params[
                "country"
            ] = normalized_country

        if partition_date is not None:

            normalized_partition_date = str(
                partition_date
            ).strip()

            if not normalized_partition_date:

                raise ValueError(
                    "partition_date cannot be empty"
                )

            conditions.append(
                "partition_date = %(partition_date)s"
            )

            params[
                "partition_date"
            ] = normalized_partition_date

        if bucket is not None:

            normalized_bucket = str(
                bucket
            ).strip()

            if not normalized_bucket:

                raise ValueError(
                    "bucket cannot be empty"
                )

            conditions.append(
                "bucket = %(bucket)s"
            )

            params[
                "bucket"
            ] = normalized_bucket

        where_clause = (
            "\n            AND ".join(
                conditions
            )
        )

        sql = f"""
        SELECT
            id,
            site_id,
            country,
            dataset,
            partition_date,
            bucket,
            file_path,
            sha256,
            row_count,
            file_size,
            min_event_time,
            max_event_time,
            schema_version,
            storage_status,
            lifecycle_status,
            remote_path

        FROM {self.schema}.data_files

        WHERE
            {where_clause}

        ORDER BY
            partition_date ASC,
            bucket ASC,
            id ASC
        """

        with self.connection.cursor() as cursor:

            cursor.execute(
                sql,
                params,
            )

            rows = (
                cursor.fetchall()
            )

        return [
            CatalogDataFile(
                id=int(
                    row[0]
                ),

                site_id=str(
                    row[1]
                ),

                country=str(
                    row[2]
                ),

                dataset=str(
                    row[3]
                ),

                partition_date=(
                    row[4].isoformat()
                    if hasattr(
                        row[4],
                        "isoformat",
                    )
                    else str(
                        row[4]
                    )
                ),

                bucket=str(
                    row[5]
                ),

                file_path=str(
                    row[6]
                ),

                sha256=str(
                    row[7]
                ),

                row_count=int(
                    row[8]
                ),

                file_size=int(
                    row[9]
                ),

                min_event_time=(
                    row[10]
                ),

                max_event_time=(
                    row[11]
                ),

                schema_version=int(
                    row[12]
                ),

                storage_status=str(
                    row[13]
                ),

                lifecycle_status=str(
                    row[14]
                ),

                remote_path=(
                    str(
                        row[15]
                    )
                    if row[15] is not None
                    else None
                ),
            )

            for row in rows
        ]

    def list_active_data_files_range(
            self,
            *,
            site_id: str,
            dataset: str,
            start_partition_date: str,
            end_partition_date: str,
            country: str | None = None,
            bucket: str | None = None,
        ) -> list[CatalogDataFile]:
        """
        查询指定 partition_date 范围内当前有效的数据文件。

        有效文件必须同时满足：

            storage_status = 'uploaded'
            lifecycle_status = 'active'

        日期范围语义：

            start_partition_date
                <= partition_date
                <= end_partition_date

        可选过滤：

            country
            bucket

        该方法用于 Query Layer 的范围剪枝，
        避免查询 N 天时调用 PostgreSQL N 次。

        返回顺序：

            partition_date
            bucket
            id
        """

        normalized_site_id = str(
            site_id
        ).strip()

        normalized_dataset = str(
            dataset
        ).strip()

        normalized_start = str(
            start_partition_date
        ).strip()

        normalized_end = str(
            end_partition_date
        ).strip()

        if not normalized_site_id:

            raise ValueError(
                "site_id cannot be empty"
            )

        if not normalized_dataset:

            raise ValueError(
                "dataset cannot be empty"
            )

        if not normalized_start:

            raise ValueError(
                "start_partition_date "
                "cannot be empty"
            )

        if not normalized_end:

            raise ValueError(
                "end_partition_date "
                "cannot be empty"
            )

        if (
            normalized_start
            >
            normalized_end
        ):

            raise ValueError(
                "start_partition_date "
                "cannot be after "
                "end_partition_date"
            )

        conditions = [
            "site_id = %(site_id)s",
            "dataset = %(dataset)s",
            "storage_status = 'uploaded'",
            "lifecycle_status = 'active'",
            (
                "partition_date "
                ">= %(start_partition_date)s"
            ),
            (
                "partition_date "
                "<= %(end_partition_date)s"
            ),
        ]

        params: dict[str, Any] = {
            "site_id":
                normalized_site_id,

            "dataset":
                normalized_dataset,

            "start_partition_date":
                normalized_start,

            "end_partition_date":
                normalized_end,
        }

        if country is not None:

            normalized_country = str(
                country
            ).strip()

            if not normalized_country:

                raise ValueError(
                    "country cannot be empty"
                )

            conditions.append(
                "country = %(country)s"
            )

            params[
                "country"
            ] = normalized_country

        if bucket is not None:

            normalized_bucket = str(
                bucket
            ).strip()

            if not normalized_bucket:

                raise ValueError(
                    "bucket cannot be empty"
                )

            conditions.append(
                "bucket = %(bucket)s"
            )

            params[
                "bucket"
            ] = normalized_bucket

        where_clause = (
            "\n            AND ".join(
                conditions
            )
        )

        sql = f"""
        SELECT
            id,
            site_id,
            country,
            dataset,
            partition_date,
            bucket,
            file_path,
            sha256,
            row_count,
            file_size,
            min_event_time,
            max_event_time,
            schema_version,
            storage_status,
            lifecycle_status,
            remote_path

        FROM {self.schema}.data_files

        WHERE
            {where_clause}

        ORDER BY
            partition_date ASC,
            bucket ASC,
            id ASC
        """

        with self.connection.cursor() as cursor:

            cursor.execute(
                sql,
                params,
            )

            rows = (
                cursor.fetchall()
            )

        return [
            CatalogDataFile(
                id=int(
                    row[0]
                ),

                site_id=str(
                    row[1]
                ),

                country=str(
                    row[2]
                ),

                dataset=str(
                    row[3]
                ),

                partition_date=(
                    row[4].isoformat()
                    if hasattr(
                        row[4],
                        "isoformat",
                    )
                    else str(
                        row[4]
                    )
                ),

                bucket=str(
                    row[5]
                ),

                file_path=str(
                    row[6]
                ),

                sha256=str(
                    row[7]
                ),

                row_count=int(
                    row[8]
                ),

                file_size=int(
                    row[9]
                ),

                min_event_time=(
                    row[10]
                ),

                max_event_time=(
                    row[11]
                ),

                schema_version=int(
                    row[12]
                ),

                storage_status=str(
                    row[13]
                ),

                lifecycle_status=str(
                    row[14]
                ),

                remote_path=(
                    str(
                        row[15]
                    )
                    if row[15] is not None
                    else None
                ),
            )
            for row in rows
        ]

    def list_active_data_files_multi_bucket(
        self,
        *,
        site_id: str,
        dataset: str,
        buckets: tuple[str, ...] | list[str],
        country: str | None = None,
        partition_date: str | None = None,
    ) -> list[CatalogDataFile]:
        """
        一次 PostgreSQL 查询多个 bucket 中的 active uploaded 文件。

        用于 multi-instrument Query Layer。

        SQL 语义：

            site_id = ...
            dataset = ...
            storage_status = 'uploaded'
            lifecycle_status = 'active'
            bucket = ANY(...)

        可选：

            country
            partition_date

        buckets:

            必须至少包含一个 bucket。

            内部会：
                strip
                去空
                稳定去重

        返回顺序：

            partition_date
            bucket
            id
        """

        normalized_site_id = str(
            site_id
        ).strip()

        normalized_dataset = str(
            dataset
        ).strip()

        if not normalized_site_id:

            raise ValueError(
                "site_id cannot be empty"
            )

        if not normalized_dataset:

            raise ValueError(
                "dataset cannot be empty"
            )

        normalized_buckets: list[str] = []

        seen_buckets: set[str] = set()

        for raw_bucket in buckets:

            normalized_bucket = str(
                raw_bucket
            ).strip()

            if not normalized_bucket:

                raise ValueError(
                    "bucket cannot be empty"
                )

            if normalized_bucket in seen_buckets:

                continue

            seen_buckets.add(
                normalized_bucket
            )

            normalized_buckets.append(
                normalized_bucket
            )

        if not normalized_buckets:

            raise ValueError(
                "buckets cannot be empty"
            )

        conditions = [
            "site_id = %(site_id)s",
            "dataset = %(dataset)s",
            "storage_status = 'uploaded'",
            "lifecycle_status = 'active'",
            "bucket = ANY(%(buckets)s)",
        ]

        params: dict[str, Any] = {
            "site_id":
                normalized_site_id,

            "dataset":
                normalized_dataset,

            "buckets":
                normalized_buckets,
        }

        if country is not None:

            normalized_country = str(
                country
            ).strip()

            if not normalized_country:

                raise ValueError(
                    "country cannot be empty"
                )

            conditions.append(
                "country = %(country)s"
            )

            params[
                "country"
            ] = normalized_country

        if partition_date is not None:

            normalized_partition_date = str(
                partition_date
            ).strip()

            if not normalized_partition_date:

                raise ValueError(
                    "partition_date cannot be empty"
                )

            conditions.append(
                "partition_date = %(partition_date)s"
            )

            params[
                "partition_date"
            ] = normalized_partition_date

        where_clause = (
            "\n            AND ".join(
                conditions
            )
        )

        sql = f"""
        SELECT
            id,
            site_id,
            country,
            dataset,
            partition_date,
            bucket,
            file_path,
            sha256,
            row_count,
            file_size,
            min_event_time,
            max_event_time,
            schema_version,
            storage_status,
            lifecycle_status,
            remote_path

        FROM {self.schema}.data_files

        WHERE
            {where_clause}

        ORDER BY
            partition_date ASC,
            bucket ASC,
            id ASC
        """

        with self.connection.cursor() as cursor:

            cursor.execute(
                sql,
                params,
            )

            rows = (
                cursor.fetchall()
            )

        return [
            CatalogDataFile(
                id=int(
                    row[0]
                ),

                site_id=str(
                    row[1]
                ),

                country=str(
                    row[2]
                ),

                dataset=str(
                    row[3]
                ),

                partition_date=(
                    row[4].isoformat()
                    if hasattr(
                        row[4],
                        "isoformat",
                    )
                    else str(
                        row[4]
                    )
                ),

                bucket=str(
                    row[5]
                ),

                file_path=str(
                    row[6]
                ),

                sha256=str(
                    row[7]
                ),

                row_count=int(
                    row[8]
                ),

                file_size=int(
                    row[9]
                ),

                min_event_time=(
                    row[10]
                ),

                max_event_time=(
                    row[11]
                ),

                schema_version=int(
                    row[12]
                ),

                storage_status=str(
                    row[13]
                ),

                lifecycle_status=str(
                    row[14]
                ),

                remote_path=(
                    str(
                        row[15]
                    )
                    if row[15] is not None
                    else None
                ),
            )

            for row in rows
        ]


    def list_active_data_files_range_multi_bucket(
        self,
        *,
        site_id: str,
        dataset: str,
        start_partition_date: str,
        end_partition_date: str,
        buckets: tuple[str, ...] | list[str],
        country: str | None = None,
    ) -> list[CatalogDataFile]:
        """
        一次 PostgreSQL 查询：

            partition_date 范围
            +
            多个 bucket

        中所有 active uploaded 数据文件。

        用于 multi-instrument Query Layer。

        相比逐 bucket 调用：

            list_active_data_files_range()

        本方法把：

            N buckets
            -> N SQL

        优化为：

            N buckets
            -> 1 SQL

        日期范围：

            start_partition_date
                <= partition_date
                <= end_partition_date

        bucket：

            bucket = ANY(%(buckets)s)

        返回顺序：

            partition_date
            bucket
            id
        """

        normalized_site_id = str(
            site_id
        ).strip()

        normalized_dataset = str(
            dataset
        ).strip()

        normalized_start = str(
            start_partition_date
        ).strip()

        normalized_end = str(
            end_partition_date
        ).strip()

        if not normalized_site_id:

            raise ValueError(
                "site_id cannot be empty"
            )

        if not normalized_dataset:

            raise ValueError(
                "dataset cannot be empty"
            )

        if not normalized_start:

            raise ValueError(
                "start_partition_date "
                "cannot be empty"
            )

        if not normalized_end:

            raise ValueError(
                "end_partition_date "
                "cannot be empty"
            )

        if (
            normalized_start
            >
            normalized_end
        ):

            raise ValueError(
                "start_partition_date "
                "cannot be after "
                "end_partition_date"
            )

        normalized_buckets: list[str] = []

        seen_buckets: set[str] = set()

        for raw_bucket in buckets:

            normalized_bucket = str(
                raw_bucket
            ).strip()

            if not normalized_bucket:

                raise ValueError(
                    "bucket cannot be empty"
                )

            if normalized_bucket in seen_buckets:

                continue

            seen_buckets.add(
                normalized_bucket
            )

            normalized_buckets.append(
                normalized_bucket
            )

        if not normalized_buckets:

            raise ValueError(
                "buckets cannot be empty"
            )

        conditions = [
            "site_id = %(site_id)s",
            "dataset = %(dataset)s",
            "storage_status = 'uploaded'",
            "lifecycle_status = 'active'",
            (
                "partition_date "
                ">= %(start_partition_date)s"
            ),
            (
                "partition_date "
                "<= %(end_partition_date)s"
            ),
            "bucket = ANY(%(buckets)s)",
        ]

        params: dict[str, Any] = {
            "site_id":
                normalized_site_id,

            "dataset":
                normalized_dataset,

            "start_partition_date":
                normalized_start,

            "end_partition_date":
                normalized_end,

            "buckets":
                normalized_buckets,
        }

        if country is not None:

            normalized_country = str(
                country
            ).strip()

            if not normalized_country:

                raise ValueError(
                    "country cannot be empty"
                )

            conditions.append(
                "country = %(country)s"
            )

            params[
                "country"
            ] = normalized_country

        where_clause = (
            "\n            AND ".join(
                conditions
            )
        )

        sql = f"""
        SELECT
            id,
            site_id,
            country,
            dataset,
            partition_date,
            bucket,
            file_path,
            sha256,
            row_count,
            file_size,
            min_event_time,
            max_event_time,
            schema_version,
            storage_status,
            lifecycle_status,
            remote_path

        FROM {self.schema}.data_files

        WHERE
            {where_clause}

        ORDER BY
            partition_date ASC,
            bucket ASC,
            id ASC
        """

        with self.connection.cursor() as cursor:

            cursor.execute(
                sql,
                params,
            )

            rows = (
                cursor.fetchall()
            )

        return [
            CatalogDataFile(
                id=int(
                    row[0]
                ),

                site_id=str(
                    row[1]
                ),

                country=str(
                    row[2]
                ),

                dataset=str(
                    row[3]
                ),

                partition_date=(
                    row[4].isoformat()
                    if hasattr(
                        row[4],
                        "isoformat",
                    )
                    else str(
                        row[4]
                    )
                ),

                bucket=str(
                    row[5]
                ),

                file_path=str(
                    row[6]
                ),

                sha256=str(
                    row[7]
                ),

                row_count=int(
                    row[8]
                ),

                file_size=int(
                    row[9]
                ),

                min_event_time=(
                    row[10]
                ),

                max_event_time=(
                    row[11]
                ),

                schema_version=int(
                    row[12]
                ),

                storage_status=str(
                    row[13]
                ),

                lifecycle_status=str(
                    row[14]
                ),

                remote_path=(
                    str(
                        row[15]
                    )
                    if row[15] is not None
                    else None
                ),
            )

            for row in rows
        ]

    def get_data_file(
        self,
        *,
        file_path: str | Path,
    ) -> CatalogDataFile | None:
        """
        根据唯一 file_path 查询一个 Catalog 文件。

        这个方法不限制 lifecycle/storage 状态。

        因为：

            repair
            rollback
            audit
            recovery

        有时需要查询 superseded / local / archived 文件。
        """

        normalized_file_path = (
            Path(
                file_path
            )
            .as_posix()
        )

        if not normalized_file_path:

            raise ValueError(
                "file_path cannot be empty"
            )

        sql = f"""
        SELECT
            id,
            site_id,
            country,
            dataset,
            partition_date,
            bucket,
            file_path,
            sha256,
            row_count,
            file_size,
            min_event_time,
            max_event_time,
            schema_version,
            storage_status,
            lifecycle_status,
            remote_path

        FROM {self.schema}.data_files

        WHERE
            file_path = %(file_path)s
        """

        params = {
            "file_path":
                normalized_file_path,
        }

        with self.connection.cursor() as cursor:

            cursor.execute(
                sql,
                params,
            )

            row = (
                cursor.fetchone()
            )

        if row is None:

            return None

        return CatalogDataFile(
            id=int(
                row[0]
            ),

            site_id=str(
                row[1]
            ),

            country=str(
                row[2]
            ),

            dataset=str(
                row[3]
            ),

            partition_date=(
                row[4].isoformat()
                if hasattr(
                    row[4],
                    "isoformat",
                )
                else str(
                    row[4]
                )
            ),

            bucket=str(
                row[5]
            ),

            file_path=str(
                row[6]
            ),

            sha256=str(
                row[7]
            ),

            row_count=int(
                row[8]
            ),

            file_size=int(
                row[9]
            ),

            min_event_time=(
                row[10]
            ),

            max_event_time=(
                row[11]
            ),

            schema_version=int(
                row[12]
            ),

            storage_status=str(
                row[13]
            ),

            lifecycle_status=str(
                row[14]
            ),

            remote_path=(
                str(
                    row[15]
                )
                if row[15] is not None
                else None
            ),
        )
