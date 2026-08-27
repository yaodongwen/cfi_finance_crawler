from __future__ import annotations

from datetime import (
    date,
    datetime,
    timezone,
)

from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from crawl_framework.storage.query import (
    CatalogFileResolver,
    CatalogParquetReader,
    QuerySpec,
    build_parquet_filters,
    normalize_query,
)


UTC = timezone.utc


# ============================================================
# Fake catalog file
# ============================================================


class FakeCatalogFile:

    def __init__(
        self,
        *,
        file_id: int,
        file_path: str,
        remote_path: str,
        row_count: int,
        file_size: int,
        partition_date: date,
    ) -> None:

        self.id = file_id

        self.file_path = (
            file_path
        )

        self.remote_path = (
            remote_path
        )

        self.row_count = (
            row_count
        )

        self.file_size = (
            file_size
        )

        self.partition_date = (
            partition_date
        )


# ============================================================
# Fake catalog
# ============================================================


class FakeCatalog:

    def __init__(
        self,
        files_by_date,
    ) -> None:

        self.files_by_date = (
            files_by_date
        )

        self.calls = []


    def list_active_data_files(
        self,
        *,
        site_id,
        dataset,
        country=None,
        partition_date=None,
        bucket=None,
    ):

        self.calls.append(
            {
                "site_id":
                    site_id,

                "dataset":
                    dataset,

                "country":
                    country,

                "partition_date":
                    partition_date,

                "bucket":
                    bucket,
            }
        )

        if partition_date is None:

            result = []

            for files in (
                self.files_by_date
                .values()
            ):

                result.extend(
                    files
                )

            return result

        return list(
            self.files_by_date.get(
                partition_date,
                [],
            )
        )

    def list_active_data_files_range(
        self,
        *,
        site_id,
        dataset,
        start_partition_date,
        end_partition_date,
        country=None,
        bucket=None,
    ):

        self.calls.append(
            {
                "method":
                    "range",

                "site_id":
                    site_id,

                "dataset":
                    dataset,

                "country":
                    country,

                "start_partition_date":
                    start_partition_date,

                "end_partition_date":
                    end_partition_date,

                "bucket":
                    bucket,
            }
        )

        result = []

        for (
            partition_date,
            files,
        ) in (
            self.files_by_date.items()
        ):

            if (
                partition_date
                <
                start_partition_date
            ):

                continue

            if (
                partition_date
                >
                end_partition_date
            ):

                continue

            result.extend(
                files
            )

        return result


class TrackingResolver(
    CatalogFileResolver
):

    def __init__(
        self,
    ):

        super().__init__()

        self.materialized_ids = []

    def materialize(
        self,
        *,
        item,
        temp_dir,
    ):

        self.materialized_ids.append(
            item.id
        )

        return Path(
            item.remote_path
        )

# ============================================================
# Helpers
# ============================================================


def write_test_parquet(
    path,
):
    table = pa.table(
        {
            "record_uid":
                pa.array(
                    [
                        "uid-a",
                        "uid-b",
                        "uid-c",
                    ],
                    type=pa.string(),
                ),

            "instrument_id":
                pa.array(
                    [
                        "XKRX:005930",
                        "XKRX:042700",
                        "XKRX:042700",
                    ],
                    type=pa.string(),
                ),

            "event_time":
                pa.array(
                    [
                        datetime(
                            2026,
                            8,
                            25,
                            10,
                            0,
                            tzinfo=UTC,
                        ),

                        datetime(
                            2026,
                            8,
                            25,
                            11,
                            0,
                            tzinfo=UTC,
                        ),

                        datetime(
                            2026,
                            8,
                            26,
                            12,
                            0,
                            tzinfo=UTC,
                        ),
                    ],
                    type=pa.timestamp(
                        "us",
                        tz="UTC",
                    ),
                ),

            "title":
                pa.array(
                    [
                        "Samsung",
                        "Hanmi A",
                        "Hanmi B",
                    ],
                    type=pa.string(),
                ),
        }
    )

    pq.write_table(
        table,
        path,
        write_statistics=True,
    )

    return table


# ============================================================
# Query normalization
# ============================================================


def test_normalize_query_dates_are_inclusive():

    query = normalize_query(
        QuerySpec(
            site_id="naver_finance",
            dataset="forum_post",
            country="kr",
            start_date="2026-08-25",
            end_date="2026-08-26",
            timezone="UTC",
            columns=(
                "record_uid",
                "event_time",
            ),
        )
    )

    assert (
        query.country
        ==
        "KR"
    )

    assert (
        query.timezone_name
        ==
        "UTC"
    )

    assert (
        query.start_time
        ==
        datetime(
            2026,
            8,
            25,
            0,
            0,
            tzinfo=UTC,
        )
    )

    assert (
        query.end_time_exclusive
        ==
        datetime(
            2026,
            8,
            27,
            0,
            0,
            tzinfo=UTC,
        )
    )

def test_normalize_query_rejects_reverse_dates():

    with pytest.raises(
        ValueError
    ):

        normalize_query(
            QuerySpec(
                site_id="naver_finance",
                dataset="forum_post",
                start_date="2026-08-26",
                end_date="2026-08-25",
            )
        )


# ============================================================
# Filters
# ============================================================


def test_build_parquet_filters():

    query = normalize_query(
        QuerySpec(
            site_id="naver_finance",
            dataset="forum_post",
            instrument_id=(
                "XKRX:042700"
            ),
            record_uid="uid-b",
            start_date="2026-08-25",
            end_date="2026-08-26",
        )
    )

    filters = (
        build_parquet_filters(
            query
        )
    )

    assert filters is not None

    inner = filters[0]

    assert (
        (
            "instrument_id",
            "=",
            "XKRX:042700",
        )
        in inner
    )

    assert (
        (
            "record_uid",
            "=",
            "uid-b",
        )
        in inner
    )


# ============================================================
# End-to-end local query
# ============================================================


def test_query_filters_instrument_and_date(
    tmp_path,
):

    parquet_path = (
        tmp_path
        /
        "test.parquet"
    )

    table = write_test_parquet(
        parquet_path
    )

    item = FakeCatalogFile(
        file_id=1,
        file_path="test.parquet",
        remote_path=str(
            parquet_path
        ),
        row_count=(
            table.num_rows
        ),
        file_size=(
            parquet_path
            .stat()
            .st_size
        ),
        partition_date=date(
            2026,
            8,
            25,
        ),
    )

    catalog = FakeCatalog(
        {
            "2026-08-25":
                [
                    item,
                ],
        }
    )

    reader = (
        CatalogParquetReader(
            catalog=catalog,
            resolver=(
                CatalogFileResolver()
            ),
        )
    )

    result = reader.query(
        QuerySpec(
            site_id="naver_finance",
            dataset="forum_post",
            country="KR",
            instrument_id=(
                "XKRX:042700"
            ),
            start_date="2026-08-25",
            end_date="2026-08-25",
            columns=(
                "record_uid",
                "instrument_id",
                "event_time",
                "title",
            ),
        )
    )

    assert (
        result.table.num_rows
        ==
        1
    )

    rows = (
        result.table
        .to_pylist()
    )

    assert (
        rows[0][
            "record_uid"
        ]
        ==
        "uid-b"
    )

    assert (
        rows[0][
            "instrument_id"
        ]
        ==
        "XKRX:042700"
    )

    assert (
        result.stats.catalog_files
        ==
        1
    )

    assert (
        result.stats.parquet_files_read
        ==
        1
    )

    assert (
        result.stats.rows_read
        ==
        3
    )

    assert (
        result.stats.rows_returned
        ==
        1
    )


def test_query_catalog_uses_single_range_call(
    tmp_path,
):

    path_25 = (
        tmp_path
        /
        "25.parquet"
    )

    path_26 = (
        tmp_path
        /
        "26.parquet"
    )

    table_25 = write_test_parquet(
        path_25
    )

    table_26 = write_test_parquet(
        path_26
    )

    file_25 = FakeCatalogFile(
        file_id=25,
        file_path="25.parquet",
        remote_path=str(
            path_25
        ),
        row_count=(
            table_25.num_rows
        ),
        file_size=(
            path_25.stat().st_size
        ),
        partition_date=date(
            2026,
            8,
            25,
        ),
    )

    file_26 = FakeCatalogFile(
        file_id=26,
        file_path="26.parquet",
        remote_path=str(
            path_26
        ),
        row_count=(
            table_26.num_rows
        ),
        file_size=(
            path_26.stat().st_size
        ),
        partition_date=date(
            2026,
            8,
            26,
        ),
    )

    catalog = FakeCatalog(
        {
            "2026-08-25":
                [
                    file_25,
                ],

            "2026-08-26":
                [
                    file_26,
                ],
        }
    )

    reader = CatalogParquetReader(
        catalog=catalog,
        resolver=CatalogFileResolver(),
    )

    reader.query(
        QuerySpec(
            site_id="naver_finance",
            dataset="forum_post",
            country="KR",
            start_date="2026-08-25",
            end_date="2026-08-26",
            timezone="UTC",
            columns=(
                "record_uid",
                "event_time",
            ),
        )
    )

    assert (
        len(
            catalog.calls
        )
        ==
        1
    )

    call = catalog.calls[0]

    assert (
        call[
            "method"
        ]
        ==
        "range"
    )

    assert (
        call[
            "start_partition_date"
        ]
        ==
        "2026-08-25"
    )

    assert (
        call[
            "end_partition_date"
        ]
        ==
        "2026-08-26"
    )

def test_normalize_query_seoul_local_date_to_utc():

    query = normalize_query(
        QuerySpec(
            site_id="naver_finance",
            dataset="forum_post",
            country="KR",
            start_date="2026-08-26",
            end_date="2026-08-26",
            timezone="Asia/Seoul",
        )
    )

    assert (
        query.timezone_name
        ==
        "Asia/Seoul"
    )

    assert (
        query.start_time
        ==
        datetime(
            2026,
            8,
            25,
            15,
            0,
            tzinfo=UTC,
        )
    )

    assert (
        query.end_time_exclusive
        ==
        datetime(
            2026,
            8,
            26,
            15,
            0,
            tzinfo=UTC,
        )
    )

def test_normalize_query_rejects_unknown_timezone():

    with pytest.raises(
        ValueError,
        match="unknown timezone",
    ):

        normalize_query(
            QuerySpec(
                site_id="naver_finance",
                dataset="forum_post",
                timezone=(
                    "Mars/Olympus_Mons"
                ),
            )
        )

def test_query_seoul_day_uses_utc_partition_range(
    tmp_path,
):

    path_25 = (
        tmp_path
        /
        "25.parquet"
    )

    path_26 = (
        tmp_path
        /
        "26.parquet"
    )

    table_25 = write_test_parquet(
        path_25
    )

    table_26 = write_test_parquet(
        path_26
    )

    file_25 = FakeCatalogFile(
        file_id=25,
        file_path="25.parquet",
        remote_path=str(
            path_25
        ),
        row_count=(
            table_25.num_rows
        ),
        file_size=(
            path_25.stat().st_size
        ),
        partition_date=date(
            2026,
            8,
            25,
        ),
    )

    file_26 = FakeCatalogFile(
        file_id=26,
        file_path="26.parquet",
        remote_path=str(
            path_26
        ),
        row_count=(
            table_26.num_rows
        ),
        file_size=(
            path_26.stat().st_size
        ),
        partition_date=date(
            2026,
            8,
            26,
        ),
    )

    catalog = FakeCatalog(
        {
            "2026-08-25":
                [
                    file_25,
                ],

            "2026-08-26":
                [
                    file_26,
                ],
        }
    )

    reader = CatalogParquetReader(
        catalog=catalog,
        resolver=CatalogFileResolver(),
    )

    reader.query(
        QuerySpec(
            site_id="naver_finance",
            dataset="forum_post",
            country="KR",
            start_date="2026-08-26",
            end_date="2026-08-26",
            timezone="Asia/Seoul",
            columns=(
                "record_uid",
                "event_time",
            ),
        )
    )

    assert (
        len(
            catalog.calls
        )
        ==
        1
    )

    call = catalog.calls[0]

    assert (
        call[
            "method"
        ]
        ==
        "range"
    )

    assert (
        call[
            "start_partition_date"
        ]
        ==
        "2026-08-25"
    )

    assert (
        call[
            "end_partition_date"
        ]
        ==
        "2026-08-26"
    )

def test_query_seoul_day_includes_previous_utc_date(
    tmp_path,
):

    parquet_path = (
        tmp_path
        /
        "seoul-day.parquet"
    )

    table = pa.table(
        {
            "record_uid":
                pa.array(
                    [
                        "before-local-day",
                        "seoul-morning",
                        "seoul-evening",
                        "after-local-day",
                    ],
                    type=pa.string(),
                ),

            "instrument_id":
                pa.array(
                    [
                        "XKRX:042700",
                        "XKRX:042700",
                        "XKRX:042700",
                        "XKRX:042700",
                    ],
                    type=pa.string(),
                ),

            "event_time":
                pa.array(
                    [
                        # 2026-08-25 23:59 KST
                        datetime(
                            2026,
                            8,
                            25,
                            14,
                            59,
                            tzinfo=UTC,
                        ),

                        # 2026-08-26 08:00 KST
                        datetime(
                            2026,
                            8,
                            25,
                            23,
                            0,
                            tzinfo=UTC,
                        ),

                        # 2026-08-26 20:00 KST
                        datetime(
                            2026,
                            8,
                            26,
                            11,
                            0,
                            tzinfo=UTC,
                        ),

                        # 2026-08-27 00:00 KST
                        datetime(
                            2026,
                            8,
                            26,
                            15,
                            0,
                            tzinfo=UTC,
                        ),
                    ],
                    type=pa.timestamp(
                        "us",
                        tz="UTC",
                    ),
                ),
        }
    )

    pq.write_table(
        table,
        parquet_path,
        write_statistics=True,
    )

    item = FakeCatalogFile(
        file_id=1,
        file_path="seoul-day.parquet",
        remote_path=str(
            parquet_path
        ),
        row_count=(
            table.num_rows
        ),
        file_size=(
            parquet_path
            .stat()
            .st_size
        ),
        partition_date=date(
            2026,
            8,
            25,
        ),
    )

    catalog = FakeCatalog(
        {
            "2026-08-25":
                [
                    item,
                ],

            "2026-08-26":
                [],
        }
    )

    reader = CatalogParquetReader(
        catalog=catalog,
        resolver=CatalogFileResolver(),
    )

    result = reader.query(
        QuerySpec(
            site_id="naver_finance",
            dataset="forum_post",
            country="KR",
            instrument_id=(
                "XKRX:042700"
            ),
            start_date="2026-08-26",
            end_date="2026-08-26",
            timezone="Asia/Seoul",
            columns=(
                "record_uid",
                "instrument_id",
                "event_time",
            ),
        )
    )

    uids = [
        row[
            "record_uid"
        ]
        for row in (
            result.table
            .to_pylist()
        )
    ]

    assert uids == [
        "seoul-morning",
        "seoul-evening",
    ]

def test_instrument_bucket_matches_partitioner():

    from crawl_framework.storage.query import (
        instrument_bucket,
    )

    assert (
        instrument_bucket(
            "XKRX:005930"
        )
        ==
        "5f"
    )

    assert (
        instrument_bucket(
            "XKRX:042700"
        )
        ==
        "20"
    )

    assert (
        instrument_bucket(
            "XKRX:000660"
        )
        ==
        "f1"
    )

def test_query_instrument_uses_bucket_pruning(
    tmp_path,
):

    parquet_path = (
        tmp_path
        /
        "042700.parquet"
    )

    table = write_test_parquet(
        parquet_path
    )

    item = FakeCatalogFile(
        file_id=1,
        file_path="042700.parquet",
        remote_path=str(
            parquet_path
        ),
        row_count=(
            table.num_rows
        ),
        file_size=(
            parquet_path
            .stat()
            .st_size
        ),
        partition_date=date(
            2026,
            8,
            25,
        ),
    )

    catalog = FakeCatalog(
        {
            "2026-08-25":
                [
                    item,
                ],
        }
    )

    reader = CatalogParquetReader(
        catalog=catalog,
        resolver=CatalogFileResolver(),
    )

    reader.query(
        QuerySpec(
            site_id="naver_finance",
            dataset="forum_post",
            country="KR",
            instrument_id=(
                "XKRX:042700"
            ),
            start_date="2026-08-25",
            end_date="2026-08-25",
            timezone="UTC",
            columns=(
                "record_uid",
                "instrument_id",
                "event_time",
            ),
        )
    )

    assert (
        len(
            catalog.calls
        )
        ==
        1
    )

    assert (
        catalog.calls[0][
            "bucket"
        ]
        ==
        "20"
    )

    assert (
        catalog.calls[0][
            "method"
        ]
        ==
        "range"
    )

def test_query_without_instrument_does_not_prune_bucket(
    tmp_path,
):

    parquet_path = (
        tmp_path
        /
        "all.parquet"
    )

    table = write_test_parquet(
        parquet_path
    )

    item = FakeCatalogFile(
        file_id=1,
        file_path="all.parquet",
        remote_path=str(
            parquet_path
        ),
        row_count=(
            table.num_rows
        ),
        file_size=(
            parquet_path
            .stat()
            .st_size
        ),
        partition_date=date(
            2026,
            8,
            25,
        ),
    )

    catalog = FakeCatalog(
        {
            "2026-08-25":
                [
                    item,
                ],
        }
    )

    reader = CatalogParquetReader(
        catalog=catalog,
        resolver=CatalogFileResolver(),
    )

    reader.query(
        QuerySpec(
            site_id="naver_finance",
            dataset="forum_post",
            country="KR",
            start_date="2026-08-25",
            end_date="2026-08-25",
            timezone="UTC",
            columns=(
                "record_uid",
                "event_time",
            ),
        )
    )

    assert (
        catalog.calls[0][
            "bucket"
        ]
        is None
    )

def test_iter_batches_rejects_invalid_batch_size():

    catalog = FakeCatalog(
        {}
    )

    reader = CatalogParquetReader(
        catalog=catalog,
        resolver=CatalogFileResolver(),
    )

    with pytest.raises(
        ValueError,
        match=(
            "batch_size must be >= 1"
        ),
    ):

        list(
            reader.iter_batches(
                QuerySpec(
                    site_id="naver_finance",
                    dataset="forum_post",
                ),
                batch_size=0,
            )
        )

def test_iter_batches_streams_multiple_batches(
    tmp_path,
):

    parquet_path = (
        tmp_path
        /
        "stream.parquet"
    )

    table = pa.table(
        {
            "record_uid":
                pa.array(
                    [
                        "r1",
                        "r2",
                        "r3",
                        "r4",
                        "r5",
                    ],
                    type=pa.string(),
                ),

            "instrument_id":
                pa.array(
                    [
                        "XKRX:042700",
                        "XKRX:042700",
                        "XKRX:042700",
                        "XKRX:042700",
                        "XKRX:042700",
                    ],
                    type=pa.string(),
                ),

            "event_time":
                pa.array(
                    [
                        datetime(
                            2026,
                            8,
                            26,
                            1,
                            0,
                            tzinfo=UTC,
                        ),
                        datetime(
                            2026,
                            8,
                            26,
                            2,
                            0,
                            tzinfo=UTC,
                        ),
                        datetime(
                            2026,
                            8,
                            26,
                            3,
                            0,
                            tzinfo=UTC,
                        ),
                        datetime(
                            2026,
                            8,
                            26,
                            4,
                            0,
                            tzinfo=UTC,
                        ),
                        datetime(
                            2026,
                            8,
                            26,
                            5,
                            0,
                            tzinfo=UTC,
                        ),
                    ],
                    type=pa.timestamp(
                        "us",
                        tz="UTC",
                    ),
                ),
        }
    )

    pq.write_table(
        table,
        parquet_path,
    )

    item = FakeCatalogFile(
        file_id=1,
        file_path="stream.parquet",
        remote_path=str(
            parquet_path
        ),
        row_count=5,
        file_size=(
            parquet_path
            .stat()
            .st_size
        ),
        partition_date=date(
            2026,
            8,
            26,
        ),
    )

    catalog = FakeCatalog(
        {
            "2026-08-26":
                [
                    item,
                ],
        }
    )

    reader = CatalogParquetReader(
        catalog=catalog,
        resolver=CatalogFileResolver(),
    )

    batches = list(
        reader.iter_batches(
            QuerySpec(
                site_id="naver_finance",
                dataset="forum_post",
                country="KR",
                instrument_id=(
                    "XKRX:042700"
                ),
                start_date=(
                    "2026-08-26"
                ),
                end_date=(
                    "2026-08-26"
                ),
                timezone="UTC",
                columns=(
                    "record_uid",
                    "instrument_id",
                    "event_time",
                ),
            ),
            batch_size=2,
        )
    )

    assert (
        len(
            batches
        )
        ==
        3
    )

    assert [
        batch.num_rows
        for batch in batches
    ] == [
        2,
        2,
        1,
    ]

    assert sum(
        batch.num_rows
        for batch in batches
    ) == 5

def test_iter_batches_applies_filters(
    tmp_path,
):

    parquet_path = (
        tmp_path
        /
        "filtered.parquet"
    )

    table = pa.table(
        {
            "record_uid":
                pa.array(
                    [
                        "keep-1",
                        "drop-other-stock",
                        "keep-2",
                        "drop-next-day",
                    ],
                    type=pa.string(),
                ),

            "instrument_id":
                pa.array(
                    [
                        "XKRX:042700",
                        "XKRX:005930",
                        "XKRX:042700",
                        "XKRX:042700",
                    ],
                    type=pa.string(),
                ),

            "event_time":
                pa.array(
                    [
                        datetime(
                            2026,
                            8,
                            26,
                            1,
                            0,
                            tzinfo=UTC,
                        ),
                        datetime(
                            2026,
                            8,
                            26,
                            2,
                            0,
                            tzinfo=UTC,
                        ),
                        datetime(
                            2026,
                            8,
                            26,
                            14,
                            0,
                            tzinfo=UTC,
                        ),
                        datetime(
                            2026,
                            8,
                            27,
                            1,
                            0,
                            tzinfo=UTC,
                        ),
                    ],
                    type=pa.timestamp(
                        "us",
                        tz="UTC",
                    ),
                ),
        }
    )

    pq.write_table(
        table,
        parquet_path,
    )

    item = FakeCatalogFile(
        file_id=1,
        file_path="filtered.parquet",
        remote_path=str(
            parquet_path
        ),
        row_count=4,
        file_size=(
            parquet_path
            .stat()
            .st_size
        ),
        partition_date=date(
            2026,
            8,
            26,
        ),
    )

    catalog = FakeCatalog(
        {
            "2026-08-26":
                [
                    item,
                ],
        }
    )

    reader = CatalogParquetReader(
        catalog=catalog,
        resolver=CatalogFileResolver(),
    )

    batches = list(
        reader.iter_batches(
            QuerySpec(
                site_id="naver_finance",
                dataset="forum_post",
                country="KR",
                instrument_id=(
                    "XKRX:042700"
                ),
                start_date=(
                    "2026-08-26"
                ),
                end_date=(
                    "2026-08-26"
                ),
                timezone="UTC",
                columns=(
                    "record_uid",
                    "instrument_id",
                    "event_time",
                ),
            ),
            batch_size=10,
        )
    )

    result = pa.Table.from_batches(
        batches
    )

    assert (
        result[
            "record_uid"
        ]
        .to_pylist()
        ==
        [
            "keep-1",
            "keep-2",
        ]
    )

def test_iter_batches_rejects_invalid_max_rows():

    catalog = FakeCatalog(
        {}
    )

    reader = CatalogParquetReader(
        catalog=catalog,
        resolver=CatalogFileResolver(),
    )

    with pytest.raises(
        ValueError,
        match=(
            "max_rows must be >= 1"
        ),
    ):

        list(
            reader.iter_batches(
                QuerySpec(
                    site_id="naver_finance",
                    dataset="forum_post",
                ),
                max_rows=0,
            )
        )

def test_iter_batches_respects_max_rows(
    tmp_path,
):

    parquet_path = (
        tmp_path
        /
        "max-rows.parquet"
    )

    table = pa.table(
        {
            "record_uid":
                pa.array(
                    [
                        "r1",
                        "r2",
                        "r3",
                        "r4",
                        "r5",
                    ],
                    type=pa.string(),
                ),

            "instrument_id":
                pa.array(
                    [
                        "XKRX:042700",
                        "XKRX:042700",
                        "XKRX:042700",
                        "XKRX:042700",
                        "XKRX:042700",
                    ],
                    type=pa.string(),
                ),

            "event_time":
                pa.array(
                    [
                        datetime(
                            2026,
                            8,
                            26,
                            hour,
                            0,
                            tzinfo=UTC,
                        )
                        for hour in (
                            1,
                            2,
                            3,
                            4,
                            5,
                        )
                    ],
                    type=pa.timestamp(
                        "us",
                        tz="UTC",
                    ),
                ),
        }
    )

    pq.write_table(
        table,
        parquet_path,
    )

    item = FakeCatalogFile(
        file_id=1,
        file_path="max-rows.parquet",
        remote_path=str(
            parquet_path
        ),
        row_count=5,
        file_size=(
            parquet_path
            .stat()
            .st_size
        ),
        partition_date=date(
            2026,
            8,
            26,
        ),
    )

    catalog = FakeCatalog(
        {
            "2026-08-26":
                [
                    item,
                ],
        }
    )

    reader = CatalogParquetReader(
        catalog=catalog,
        resolver=CatalogFileResolver(),
    )

    batches = list(
        reader.iter_batches(
            QuerySpec(
                site_id="naver_finance",
                dataset="forum_post",
                country="KR",
                instrument_id=(
                    "XKRX:042700"
                ),
                start_date=(
                    "2026-08-26"
                ),
                end_date=(
                    "2026-08-26"
                ),
                timezone="UTC",
                columns=(
                    "record_uid",
                    "instrument_id",
                    "event_time",
                ),
            ),
            batch_size=2,
            max_rows=3,
        )
    )

    assert sum(
        batch.num_rows
        for batch in batches
    ) == 3

    table = pa.Table.from_batches(
        batches
    )

    assert (
        table[
            "record_uid"
        ]
        .to_pylist()
        ==
        [
            "r1",
            "r2",
            "r3",
        ]
    )

def test_iter_batches_max_rows_stops_before_next_file(
    tmp_path,
):

    path_1 = (
        tmp_path
        /
        "one.parquet"
    )

    path_2 = (
        tmp_path
        /
        "two.parquet"
    )

    def make_table(
        prefix,
    ):

        return pa.table(
            {
                "record_uid":
                    pa.array(
                        [
                            f"{prefix}-1",
                            f"{prefix}-2",
                            f"{prefix}-3",
                        ],
                        type=pa.string(),
                    ),

                "instrument_id":
                    pa.array(
                        [
                            "XKRX:042700",
                            "XKRX:042700",
                            "XKRX:042700",
                        ],
                        type=pa.string(),
                    ),

                "event_time":
                    pa.array(
                        [
                            datetime(
                                2026,
                                8,
                                26,
                                1,
                                0,
                                tzinfo=UTC,
                            ),
                            datetime(
                                2026,
                                8,
                                26,
                                2,
                                0,
                                tzinfo=UTC,
                            ),
                            datetime(
                                2026,
                                8,
                                26,
                                3,
                                0,
                                tzinfo=UTC,
                            ),
                        ],
                        type=pa.timestamp(
                            "us",
                            tz="UTC",
                        ),
                    ),
            }
        )

    table_1 = make_table(
        "a"
    )

    table_2 = make_table(
        "b"
    )

    pq.write_table(
        table_1,
        path_1,
    )

    pq.write_table(
        table_2,
        path_2,
    )

    item_1 = FakeCatalogFile(
        file_id=1,
        file_path="one.parquet",
        remote_path=str(
            path_1
        ),
        row_count=3,
        file_size=(
            path_1.stat().st_size
        ),
        partition_date=date(
            2026,
            8,
            26,
        ),
    )

    item_2 = FakeCatalogFile(
        file_id=2,
        file_path="two.parquet",
        remote_path=str(
            path_2
        ),
        row_count=3,
        file_size=(
            path_2.stat().st_size
        ),
        partition_date=date(
            2026,
            8,
            26,
        ),
    )

    catalog = FakeCatalog(
        {
            "2026-08-26":
                [
                    item_1,
                    item_2,
                ],
        }
    )

    resolver = TrackingResolver()

    reader = CatalogParquetReader(
        catalog=catalog,
        resolver=resolver,
    )

    batches = list(
        reader.iter_batches(
            QuerySpec(
                site_id="naver_finance",
                dataset="forum_post",
                country="KR",
                instrument_id=(
                    "XKRX:042700"
                ),
                start_date=(
                    "2026-08-26"
                ),
                end_date=(
                    "2026-08-26"
                ),
                timezone="UTC",
                columns=(
                    "record_uid",
                    "event_time",
                ),
            ),
            batch_size=10,
            max_rows=2,
        )
    )

    assert sum(
        batch.num_rows
        for batch in batches
    ) == 2

    assert (
        resolver.materialized_ids
        ==
        [
            1,
        ]
    )