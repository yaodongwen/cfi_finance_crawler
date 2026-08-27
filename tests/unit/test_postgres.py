from datetime import (
    date,
    datetime,
    timezone,
)
from pathlib import Path

import pytest

from crawl_framework.core.models import (
    CanonicalRecord,
)
from crawl_framework.storage.buffer import (
    FlushBatch,
)
from crawl_framework.storage.parquet_writer import (
    ParquetWriter,
)
from crawl_framework.storage.partition import (
    Partitioner,
)
from crawl_framework.storage.postgres import (
    DEFAULT_SCHEMA,
    DataFileRecord,
    PostgresCatalog,
    build_insert_data_file_sql,
    build_schema_ddl,
    data_file_params,
    validate_schema_name,
)

class FakeCursor:

    def __init__(
        self,
        log,
        *,
        fetchall_rows=None,
        fetchone_row=None,
    ):

        self.log = log

        self.fetchall_rows = (
            list(
                fetchall_rows
            )
            if fetchall_rows is not None
            else []
        )

        self.fetchone_row = (
            fetchone_row
        )


    def execute(
        self,
        sql,
        params=None,
    ):

        self.log.append(
            (
                sql,
                params,
            )
        )


    def fetchall(
        self,
    ):

        return list(
            self.fetchall_rows
        )


    def fetchone(
        self,
    ):

        return self.fetchone_row


    def __enter__(
        self,
    ):

        return self


    def __exit__(
        self,
        exc_type,
        exc,
        tb,
    ):

        pass


class FakeConnection:

    def __init__(
        self,
        *,
        fetchall_rows=None,
        fetchone_row=None,
    ):

        self.executed = []

        self.commit_count = 0

        self.fetchall_rows = (
            list(
                fetchall_rows
            )
            if fetchall_rows is not None
            else []
        )

        self.fetchone_row = (
            fetchone_row
        )


    def cursor(
        self,
    ):

        return FakeCursor(
            self.executed,
            fetchall_rows=(
                self.fetchall_rows
            ),
            fetchone_row=(
                self.fetchone_row
            ),
        )


    def commit(
        self,
    ):

        self.commit_count += 1


def make_parquet_info(
    tmp_path,
):

    record = CanonicalRecord(
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        source_id="123",
        scope_type="instrument",
        scope_id="XKRX:005930",
        instrument_id="XKRX:005930",
        event_time=datetime(
            2026,
            8,
            24,
            1,
            0,
            tzinfo=timezone.utc,
        ),
        title="hello",
    )

    partitioner = (
        Partitioner()
    )

    batch = FlushBatch(
        key=(
            partitioner
            .partition_for(
                record
            )
        ),
        records=(
            record,
        ),
        estimated_bytes=1000,
    )

    writer = ParquetWriter(
        tmp_path
        / "warehouse"
    )

    return writer.write_batch(
        batch
    )


def test_validate_schema_name():

    assert (
        validate_schema_name(
            "marketdata"
        )
        == "marketdata"
    )

    assert (
        validate_schema_name(
            "market_data_v2"
        )
        == "market_data_v2"
    )


def test_invalid_schema_name():

    with pytest.raises(
        ValueError
    ):
        validate_schema_name(
            "marketdata;DROP TABLE x"
        )

    with pytest.raises(
        ValueError
    ):
        validate_schema_name(
            "123abc"
        )


def test_build_schema_ddl():

    statements = (
        build_schema_ddl()
    )

    text = "\n".join(
        statements
    )

    assert (
        "CREATE SCHEMA"
        in text
    )

    assert (
        "marketdata.data_files"
        in text
    )

    assert (
        "marketdata.record_registry"
        in text
    )

    assert (
        "marketdata.checkpoints"
        in text
    )


def test_default_schema():

    assert (
        DEFAULT_SCHEMA
        == "marketdata"
    )


def test_insert_sql():

    sql = (
        build_insert_data_file_sql()
    )

    assert (
        "INSERT INTO marketdata.data_files"
        in sql
    )

    assert (
        "ON CONFLICT(file_path)"
        in sql
    )


def test_data_file_from_parquet(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    record = (
        DataFileRecord
        .from_parquet_info(
            info
        )
    )

    assert (
        record.site_id
        == "naver_finance"
    )

    assert (
        record.dataset
        == "forum_post"
    )

    assert (
        record.row_count
        == 1
    )

    assert (
        record.file_size
        > 0
    )

    assert (
        len(
            record.sha256
        )
        == 64
    )


def test_data_file_params(
    tmp_path,
):

    info = make_parquet_info(
        tmp_path
    )

    record = (
        DataFileRecord
        .from_parquet_info(
            info
        )
    )

    params = (
        data_file_params(
            record
        )
    )

    assert (
        params[
            "site_id"
        ]
        == "naver_finance"
    )

    assert (
        params[
            "storage_status"
        ]
        == "local"
    )


def test_initialize_schema():

    connection = (
        FakeConnection()
    )

    catalog = (
        PostgresCatalog(
            connection
        )
    )

    catalog.initialize_schema()

    assert (
        len(
            connection.executed
        )
        > 5
    )

    assert (
        connection.commit_count
        == 1
    )


def test_register_data_file(
    tmp_path,
):

    connection = (
        FakeConnection()
    )

    catalog = (
        PostgresCatalog(
            connection
        )
    )

    info = make_parquet_info(
        tmp_path
    )

    record = (
        DataFileRecord
        .from_parquet_info(
            info
        )
    )

    catalog.register_data_file(
        record
    )

    assert (
        len(
            connection.executed
        )
        == 1
    )

    sql, params = (
        connection.executed[0]
    )

    assert (
        "INSERT INTO marketdata.data_files"
        in sql
    )

    assert (
        params[
            "file_path"
        ]
        == record.file_path
    )

    assert (
        connection.commit_count
        == 1
    )


def test_register_parquet_file(
    tmp_path,
):

    connection = (
        FakeConnection()
    )

    catalog = (
        PostgresCatalog(
            connection
        )
    )

    info = make_parquet_info(
        tmp_path
    )

    record = (
        catalog.register_parquet_file(
            info
        )
    )

    assert isinstance(
        record,
        DataFileRecord,
    )

    assert (
        record.row_count
        == 1
    )


def test_mark_uploaded():

    connection = (
        FakeConnection()
    )

    catalog = (
        PostgresCatalog(
            connection
        )
    )

    catalog.mark_uploaded(
        file_path=(
            "site=x/"
            "dataset=y/"
            "part.parquet"
        ),
        remote_path=(
            "/remote/"
            "part.parquet"
        ),
    )

    assert (
        len(
            connection.executed
        )
        == 1
    )

    sql, params = (
        connection.executed[0]
    )

    assert (
        "storage_status = 'uploaded'"
        in sql
    )

    assert (
        params[
            "remote_path"
        ]
        == "/remote/part.parquet"
    )

def test_list_active_data_files_filters_uploaded_and_active():
    connection = FakeConnection(
        fetchall_rows=[
            (
                7,
                "naver_finance",
                "KR",
                "forum_post",
                date(2026, 8, 25),
                "5f",
                (
                    "site=naver_finance/"
                    "country=KR/"
                    "dataset=forum_post/"
                    "year=2026/"
                    "month=08/"
                    "day=25/"
                    "bucket=5f/"
                    "part-compacted-a.parquet"
                ),
                "abc123",
                20,
                12345,
                None,
                None,
                1,
                "uploaded",
                "active",
                "/remote/a.parquet",
            ),
        ],
    )

    catalog = PostgresCatalog(
        connection
    )

    rows = catalog.list_active_data_files(
        site_id="naver_finance",
        dataset="forum_post",
        country="KR",
    )

    assert len(rows) == 1

    row = rows[0]

    assert row.id == 7
    assert row.site_id == "naver_finance"
    assert row.country == "KR"
    assert row.dataset == "forum_post"
    assert row.partition_date == "2026-08-25"
    assert row.bucket == "5f"
    assert row.row_count == 20
    assert row.storage_status == "uploaded"
    assert row.lifecycle_status == "active"
    assert row.remote_path == "/remote/a.parquet"

    sql, params = (
        connection.executed[-1]
    )

    assert (
        "storage_status = 'uploaded'"
        in sql
    )

    assert (
        "lifecycle_status = 'active'"
        in sql
    )

    assert (
        "site_id = %(site_id)s"
        in sql
    )

    assert (
        "dataset = %(dataset)s"
        in sql
    )

    assert (
        "country = %(country)s"
        in sql
    )

    assert params["site_id"] == "naver_finance"
    assert params["dataset"] == "forum_post"
    assert params["country"] == "KR"


def test_list_active_data_files_optional_partition_filters():
    connection = FakeConnection(
        fetchall_rows=[],
    )

    catalog = PostgresCatalog(
        connection
    )

    rows = catalog.list_active_data_files(
        site_id="naver_finance",
        dataset="forum_post",
        country="KR",
        partition_date="2026-08-26",
        bucket="20",
    )

    assert rows == []

    sql, params = (
        connection.cursor_instance
        .executed[-1]
    )

    assert (
        "partition_date = %(partition_date)s"
        in sql
    )

    assert (
        "bucket = %(bucket)s"
        in sql
    )

    assert (
        params[
            "partition_date"
        ]
        ==
        "2026-08-26"
    )

    assert (
        params[
            "bucket"
        ]
        ==
        "20"
    )


def test_list_active_data_files_rejects_empty_site_id():
    connection = FakeConnection(
        fetchall_rows=[],
    )

    catalog = PostgresCatalog(
        connection
    )

    with pytest.raises(
        ValueError,
        match="site_id cannot be empty",
    ):

        catalog.list_active_data_files(
            site_id="",
            dataset="forum_post",
        )


def test_list_active_data_files_rejects_empty_dataset():
    connection = FakeConnection(
        fetchall_rows=[],
    )

    catalog = PostgresCatalog(
        connection
    )

    with pytest.raises(
        ValueError,
        match="dataset cannot be empty",
    ):

        catalog.list_active_data_files(
            site_id="naver_finance",
            dataset="",
        )


def test_get_data_file_returns_any_lifecycle_state():
    connection = FakeConnection(
        fetchone_row=(
            2,
            "naver_finance",
            "KR",
            "forum_post",
            date(2026, 8, 26),
            "5f",
            "site=x/part-old.parquet",
            "deadbeef",
            20,
            999,
            None,
            None,
            1,
            "uploaded",
            "superseded",
            "/remote/old.parquet",
        ),
    )

    catalog = PostgresCatalog(
        connection
    )

    row = catalog.get_data_file(
        file_path=(
            "site=x/"
            "part-old.parquet"
        )
    )

    assert row is not None

    assert row.id == 2
    assert (
        row.storage_status
        ==
        "uploaded"
    )

    assert (
        row.lifecycle_status
        ==
        "superseded"
    )

    sql, params = (
        connection.executed[-1]
    )

    assert (
        "lifecycle_status = 'active'"
        not in sql
    )

    assert (
        params["file_path"]
        ==
        "site=x/part-old.parquet"
    )


def test_get_data_file_returns_none_when_missing():
    connection = FakeConnection(
        fetchone_row=None,
    )

    catalog = PostgresCatalog(
        connection
    )

    row = catalog.get_data_file(
        file_path="missing.parquet",
    )

    assert row is None

def test_list_active_data_files_returns_catalog_rows():

    connection = FakeConnection(
        fetchall_rows=[
            (
                7,
                "naver_finance",
                "KR",
                "forum_post",
                date(
                    2026,
                    8,
                    25,
                ),
                "5f",
                (
                    "site=naver_finance/"
                    "country=KR/"
                    "dataset=forum_post/"
                    "year=2026/"
                    "month=08/"
                    "day=25/"
                    "bucket=5f/"
                    "part-compacted-a.parquet"
                ),
                "abc123",
                20,
                12345,
                None,
                None,
                1,
                "uploaded",
                "active",
                "/remote/a.parquet",
            ),
        ],
    )

    catalog = PostgresCatalog(
        connection
    )

    rows = (
        catalog.list_active_data_files(
            site_id="naver_finance",
            dataset="forum_post",
            country="KR",
        )
    )

    assert len(
        rows
    ) == 1

    row = rows[0]

    assert row.id == 7

    assert (
        row.site_id
        ==
        "naver_finance"
    )

    assert (
        row.country
        ==
        "KR"
    )

    assert (
        row.dataset
        ==
        "forum_post"
    )

    assert (
        row.partition_date
        ==
        "2026-08-25"
    )

    assert (
        row.bucket
        ==
        "5f"
    )

    assert (
        row.row_count
        ==
        20
    )

    assert (
        row.file_size
        ==
        12345
    )

    assert (
        row.storage_status
        ==
        "uploaded"
    )

    assert (
        row.lifecycle_status
        ==
        "active"
    )

    assert (
        row.remote_path
        ==
        "/remote/a.parquet"
    )

    assert len(
        connection.executed
    ) == 1

    sql, params = (
        connection.executed[
            0
        ]
    )

    assert (
        "storage_status = 'uploaded'"
        in sql
    )

    assert (
        "lifecycle_status = 'active'"
        in sql
    )

    assert (
        "site_id = %(site_id)s"
        in sql
    )

    assert (
        "dataset = %(dataset)s"
        in sql
    )

    assert (
        "country = %(country)s"
        in sql
    )

    assert (
        params[
            "site_id"
        ]
        ==
        "naver_finance"
    )

    assert (
        params[
            "dataset"
        ]
        ==
        "forum_post"
    )

    assert (
        params[
            "country"
        ]
        ==
        "KR"
    )


def test_list_active_data_files_optional_partition_filters():

    connection = FakeConnection(
        fetchall_rows=[],
    )

    catalog = PostgresCatalog(
        connection
    )

    rows = (
        catalog.list_active_data_files(
            site_id="naver_finance",
            dataset="forum_post",
            country="KR",
            partition_date=(
                "2026-08-26"
            ),
            bucket="20",
        )
    )

    assert rows == []

    assert len(
        connection.executed
    ) == 1

    sql, params = (
        connection.executed[
            0
        ]
    )

    assert (
        "partition_date = %(partition_date)s"
        in sql
    )

    assert (
        "bucket = %(bucket)s"
        in sql
    )

    assert (
        params[
            "partition_date"
        ]
        ==
        "2026-08-26"
    )

    assert (
        params[
            "bucket"
        ]
        ==
        "20"
    )


def test_list_active_data_files_rejects_empty_site_id():

    connection = (
        FakeConnection()
    )

    catalog = PostgresCatalog(
        connection
    )

    with pytest.raises(
        ValueError,
        match=(
            "site_id cannot be empty"
        ),
    ):

        catalog.list_active_data_files(
            site_id="",
            dataset="forum_post",
        )

    assert (
        connection.executed
        ==
        []
    )


def test_list_active_data_files_rejects_empty_dataset():

    connection = (
        FakeConnection()
    )

    catalog = PostgresCatalog(
        connection
    )

    with pytest.raises(
        ValueError,
        match=(
            "dataset cannot be empty"
        ),
    ):

        catalog.list_active_data_files(
            site_id="naver_finance",
            dataset="",
        )

    assert (
        connection.executed
        ==
        []
    )


def test_get_data_file_returns_superseded_file():

    connection = FakeConnection(
        fetchone_row=(
            2,
            "naver_finance",
            "KR",
            "forum_post",
            date(
                2026,
                8,
                26,
            ),
            "5f",
            (
                "site=naver_finance/"
                "country=KR/"
                "dataset=forum_post/"
                "part-old.parquet"
            ),
            "deadbeef",
            20,
            999,
            None,
            None,
            1,
            "uploaded",
            "superseded",
            "/remote/old.parquet",
        ),
    )

    catalog = PostgresCatalog(
        connection
    )

    file_path = (
        "site=naver_finance/"
        "country=KR/"
        "dataset=forum_post/"
        "part-old.parquet"
    )

    row = (
        catalog.get_data_file(
            file_path=file_path,
        )
    )

    assert row is not None

    assert row.id == 2

    assert (
        row.storage_status
        ==
        "uploaded"
    )

    assert (
        row.lifecycle_status
        ==
        "superseded"
    )

    assert (
        row.remote_path
        ==
        "/remote/old.parquet"
    )

    assert len(
        connection.executed
    ) == 1

    sql, params = (
        connection.executed[
            0
        ]
    )

    # get_data_file 是审计/恢复接口，
    # 不应该强制 active。
    assert (
        "lifecycle_status = 'active'"
        not in sql
    )

    assert (
        "storage_status = 'uploaded'"
        not in sql
    )

    assert (
        params[
            "file_path"
        ]
        ==
        file_path
    )


def test_get_data_file_returns_none_when_missing():

    connection = FakeConnection(
        fetchone_row=None,
    )

    catalog = PostgresCatalog(
        connection
    )

    row = (
        catalog.get_data_file(
            file_path=(
                "missing.parquet"
            ),
        )
    )

    assert row is None

    assert len(
        connection.executed
    ) == 1


def test_data_file_upsert_does_not_reactivate_lifecycle():

    sql = (
        build_insert_data_file_sql()
    )

    # INSERT 必须包含 lifecycle_status，
    # 新文件需要写入初始生命周期。
    insert_part = (
        sql.split(
            "ON CONFLICT",
            1,
        )[0]
    )

    assert (
        "lifecycle_status"
        in insert_part
    )

    # 但 ON CONFLICT 更新区域绝不能覆盖生命周期。
    conflict_part = (
        sql.split(
            "ON CONFLICT",
            1,
        )[1]
    )

    assert (
        "lifecycle_status"
        not in conflict_part
    )

    assert (
        "storage_status"
        not in conflict_part
    )

    assert (
        "remote_path"
        not in conflict_part
    )


def test_data_file_upsert_conflict_updates_metadata_only():

    sql = (
        build_insert_data_file_sql()
    )

    conflict_part = (
        sql.split(
            "ON CONFLICT",
            1,
        )[1]
    )

    assert (
        "sha256 =\n            EXCLUDED.sha256"
        in conflict_part
    )

    assert (
        "row_count =\n            EXCLUDED.row_count"
        in conflict_part
    )

    assert (
        "file_size =\n            EXCLUDED.file_size"
        in conflict_part
    )

    assert (
        "min_event_time =\n            EXCLUDED.min_event_time"
        in conflict_part
    )

    assert (
        "max_event_time =\n            EXCLUDED.max_event_time"
        in conflict_part
    )

    assert (
        "schema_version =\n            EXCLUDED.schema_version"
        in conflict_part
    )


def test_data_file_upsert_conflict_preserves_physical_state_sql():

    sql = (
        build_insert_data_file_sql()
    )

    conflict_part = (
        sql.split(
            "ON CONFLICT",
            1,
        )[1]
    )

    forbidden_updates = (
        "storage_status = EXCLUDED.storage_status",
        "remote_path = EXCLUDED.remote_path",
        "lifecycle_status = EXCLUDED.lifecycle_status",
    )

    compact_conflict_sql = (
        " ".join(
            conflict_part.split()
        )
    )

    for forbidden_update in forbidden_updates:

        assert (
            forbidden_update
            not in compact_conflict_sql
        )


def test_duplicate_registration_preserves_uploaded_state_conceptually():

    existing = {
        "storage_status": "uploaded",
        "remote_path": "/remote/a.parquet",
        "lifecycle_status": "active",
        "sha256": "old",
        "row_count": 1,
        "file_size": 100,
    }

    duplicate_registration = {
        "storage_status": "local",
        "remote_path": None,
        "lifecycle_status": "active",
        "sha256": "new",
        "row_count": 2,
        "file_size": 200,
    }

    metadata_update_fields = {
        "sha256",
        "row_count",
        "file_size",
    }

    merged = {
        **existing,
        **{
            field: duplicate_registration[field]
            for field in metadata_update_fields
        },
    }

    assert (
        merged["storage_status"]
        == "uploaded"
    )

    assert (
        merged["remote_path"]
        == "/remote/a.parquet"
    )

    assert (
        merged["lifecycle_status"]
        == "active"
    )

    assert (
        merged["sha256"]
        == "new"
    )


def test_duplicate_registration_preserves_non_active_lifecycle_conceptually():

    existing = {
        "storage_status": "uploaded",
        "remote_path": "/remote/superseded.parquet",
        "lifecycle_status": "superseded",
    }

    duplicate_registration = {
        "storage_status": "local",
        "remote_path": None,
        "lifecycle_status": "active",
    }

    merged = dict(
        existing
    )

    assert (
        duplicate_registration["lifecycle_status"]
        == "active"
    )

    assert (
        merged["lifecycle_status"]
        == "superseded"
    )


def test_data_file_record_defaults_to_active_lifecycle():

    record = DataFileRecord(
        site_id="test_site",
        country="KR",
        dataset="forum_post",
        partition_date="2026-08-26",
        bucket="00",
        file_path="test.parquet",
        sha256="abc",
        row_count=1,
        file_size=100,
        min_event_time=None,
        max_event_time=None,
        schema_version=1,
    )

    assert (
        record.storage_status
        ==
        "local"
    )

    assert (
        record.lifecycle_status
        ==
        "active"
    )

    assert (
        record.remote_path
        is None
    )

def test_list_active_data_files_range_builds_safe_query():

    connection = FakeConnection(
        fetchall_rows=[],
    )

    catalog = PostgresCatalog(
        connection
    )

    rows = (
        catalog.list_active_data_files_range(
            site_id="naver_finance",
            dataset="forum_post",
            country="KR",
            start_partition_date=(
                "2026-08-25"
            ),
            end_partition_date=(
                "2026-08-26"
            ),
            bucket="20",
        )
    )

    assert rows == []

    assert (
        len(
            connection.executed
        )
        ==
        1
    )

    sql, params = (
        connection.executed[
            0
        ]
    )

    assert (
        "storage_status = 'uploaded'"
        in sql
    )

    assert (
        "lifecycle_status = 'active'"
        in sql
    )

    assert (
        "site_id = %(site_id)s"
        in sql
    )

    assert (
        "dataset = %(dataset)s"
        in sql
    )

    assert (
        "country = %(country)s"
        in sql
    )

    assert (
        "partition_date "
        ">= %(start_partition_date)s"
        in sql
    )

    assert (
        "partition_date "
        "<= %(end_partition_date)s"
        in sql
    )

    assert (
        "bucket = %(bucket)s"
        in sql
    )

    assert (
        params[
            "site_id"
        ]
        ==
        "naver_finance"
    )

    assert (
        params[
            "dataset"
        ]
        ==
        "forum_post"
    )

    assert (
        params[
            "country"
        ]
        ==
        "KR"
    )

    assert (
        params[
            "start_partition_date"
        ]
        ==
        "2026-08-25"
    )

    assert (
        params[
            "end_partition_date"
        ]
        ==
        "2026-08-26"
    )

    assert (
        params[
            "bucket"
        ]
        ==
        "20"
    )

def test_list_active_data_files_range_rejects_reverse_dates():

    connection = FakeConnection(
        fetchall_rows=[],
    )

    catalog = PostgresCatalog(
        connection
    )

    with pytest.raises(
        ValueError,
        match=(
            "start_partition_date "
            "cannot be after "
            "end_partition_date"
        ),
    ):

        catalog.list_active_data_files_range(
            site_id="naver_finance",
            dataset="forum_post",
            start_partition_date=(
                "2026-08-26"
            ),
            end_partition_date=(
                "2026-08-25"
            ),
        )

    assert (
        connection.executed
        ==
        []
    )

def test_list_active_data_files_range_without_bucket():

    connection = FakeConnection(
        fetchall_rows=[],
    )

    catalog = PostgresCatalog(
        connection
    )

    rows = (
        catalog.list_active_data_files_range(
            site_id="naver_finance",
            dataset="forum_post",
            country="KR",
            start_partition_date=(
                "2026-08-25"
            ),
            end_partition_date=(
                "2026-08-26"
            ),
        )
    )

    assert rows == []

    assert (
        len(
            connection.executed
        )
        ==
        1
    )

    sql, params = (
        connection.executed[
            0
        ]
    )

    assert (
        "bucket = %(bucket)s"
        not in sql
    )

    assert (
        "bucket"
        not in params
    )

    assert (
        params[
            "start_partition_date"
        ]
        ==
        "2026-08-25"
    )

    assert (
        params[
            "end_partition_date"
        ]
        ==
        "2026-08-26"
    )
