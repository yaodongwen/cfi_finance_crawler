from __future__ import annotations

from dataclasses import dataclass
from datetime import (
    date,
    datetime,
    timezone,
)
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from crawl_framework.storage.query import (
    CatalogFileResolver,
    CatalogParquetReader,
    QuerySpec,
    StreamingQueryStats,
    instrument_buckets,
    normalize_query,
)
from crawl_framework.storage.scale import (
    QueryScaleObservation,
    assert_catalog_sql_not_per_instrument,
)


UTC = timezone.utc


@dataclass
class FakeCatalogFile:
    id: int
    file_path: str
    remote_path: str
    row_count: int
    file_size: int
    partition_date: str
    bucket: str
    site_id: str = "naver_finance"
    country: str = "KR"
    dataset: str = "forum_post"
    sha256: str = "x"
    min_event_time: datetime | None = None
    max_event_time: datetime | None = None
    schema_version: int = 1
    storage_status: str = "uploaded"
    lifecycle_status: str = "active"


class FakeCatalog:

    def __init__(
        self,
        files_by_bucket,
    ):

        self.files_by_bucket = (
            files_by_bucket
        )

        self.calls = []

    def _files_for_buckets(
        self,
        buckets,
    ):

        result = []

        for bucket in buckets:

            result.extend(
                self.files_by_bucket.get(
                    bucket,
                    [],
                )
            )

        return result

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
                "method": "range",
                "site_id": site_id,
                "dataset": dataset,
                "country": country,
                "start_partition_date":
                    start_partition_date,
                "end_partition_date":
                    end_partition_date,
                "bucket": bucket,
            }
        )

        if bucket is None:

            return (
                self._files_for_buckets(
                    self.files_by_bucket.keys()
                )
            )

        return list(
            self.files_by_bucket.get(
                bucket,
                [],
            )
        )

    def list_active_data_files(
        self,
        *,
        site_id,
        dataset,
        country=None,
        bucket=None,
    ):

        self.calls.append(
            {
                "method": "all",
                "site_id": site_id,
                "dataset": dataset,
                "country": country,
                "bucket": bucket,
            }
        )

        if bucket is None:

            return (
                self._files_for_buckets(
                    self.files_by_bucket.keys()
                )
            )

        return list(
            self.files_by_bucket.get(
                bucket,
                [],
            )
        )

    def list_active_data_files_range_multi_bucket(
        self,
        *,
        site_id,
        dataset,
        start_partition_date,
        end_partition_date,
        buckets,
        country=None,
    ):

        normalized_buckets = tuple(
            buckets
        )

        self.calls.append(
            {
                "method":
                    "range_multi_bucket",

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

                "buckets":
                    normalized_buckets,
            }
        )

        return (
            self._files_for_buckets(
                normalized_buckets
            )
        )

    def list_active_data_files_multi_bucket(
        self,
        *,
        site_id,
        dataset,
        buckets,
        country=None,
        partition_date=None,
    ):

        normalized_buckets = tuple(
            buckets
        )

        self.calls.append(
            {
                "method":
                    "multi_bucket",

                "site_id":
                    site_id,

                "dataset":
                    dataset,

                "country":
                    country,

                "partition_date":
                    partition_date,

                "buckets":
                    normalized_buckets,
            }
        )

        return (
            self._files_for_buckets(
                normalized_buckets
            )
        )


def write_file(
    path: Path,
    *,
    instrument_id: str,
    prefix: str,
):

    table = pa.table(
        {
            "record_uid":
                pa.array(
                    [
                        f"{prefix}-1",
                        f"{prefix}-2",
                    ],
                    type=pa.string(),
                ),

            "instrument_id":
                pa.array(
                    [
                        instrument_id,
                        instrument_id,
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
        path,
    )

    return table


def test_normalize_query_merges_single_and_multiple_instruments():

    query = normalize_query(
        QuerySpec(
            site_id="naver_finance",
            dataset="forum_post",
            instrument_id=(
                "XKRX:005930"
            ),
            instrument_ids=(
                "XKRX:005930",
                "XKRX:000660",
                "XKRX:042700",
                "XKRX:000660",
            ),
        )
    )

    assert (
        query.instrument_ids
        ==
        (
            "XKRX:005930",
            "XKRX:000660",
            "XKRX:042700",
        )
    )

    assert (
        query.instrument_id
        is None
    )


def test_single_instrument_remains_backward_compatible():

    query = normalize_query(
        QuerySpec(
            site_id="naver_finance",
            dataset="forum_post",
            instrument_id=(
                "XKRX:042700"
            ),
        )
    )

    assert (
        query.instrument_id
        ==
        "XKRX:042700"
    )

    assert (
        query.instrument_ids
        ==
        (
            "XKRX:042700",
        )
    )

    assert query.bucket == "20"
    assert query.buckets == ("20",)


def test_instrument_buckets_are_deduplicated():

    buckets = instrument_buckets(
        (
            "XKRX:005930",
            "XKRX:005930",
            "XKRX:000660",
            "XKRX:042700",
        )
    )

    assert buckets == (
        "5f",
        "f1",
        "20",
    )


def test_multi_instrument_catalog_queries_all_buckets_once():

    catalog = FakeCatalog(
        {
            "5f": [],
            "f1": [],
            "20": [],
        }
    )

    reader = CatalogParquetReader(
        catalog=catalog,
        resolver=CatalogFileResolver(),
    )

    list(
        reader.iter_batches(
            QuerySpec(
                site_id="naver_finance",
                dataset="forum_post",
                country="KR",
                instrument_ids=(
                    "XKRX:005930",
                    "XKRX:000660",
                    "XKRX:042700",
                    "XKRX:005930",
                ),
                start_date="2026-08-26",
                end_date="2026-08-26",
                timezone="UTC",
                columns=(
                    "record_uid",
                    "instrument_id",
                    "event_time",
                ),
            )
        )
    )

    assert len(
        catalog.calls
    ) == 1

    call = catalog.calls[0]

    assert (
        call["method"]
        ==
        "range_multi_bucket"
    )

    assert (
        call["buckets"]
        ==
        (
            "5f",
            "f1",
            "20",
        )
    )

    assert (
        call[
            "start_partition_date"
        ]
        ==
        "2026-08-26"
    )

    assert (
        call[
            "end_partition_date"
        ]
        ==
        "2026-08-26"
    )

def test_multi_instrument_query_reads_only_requested_instruments(
    tmp_path,
):

    path_005930 = (
        tmp_path
        /
        "005930.parquet"
    )

    path_042700 = (
        tmp_path
        /
        "042700.parquet"
    )

    table_005930 = write_file(
        path_005930,
        instrument_id=(
            "XKRX:005930"
        ),
        prefix="samsung",
    )

    table_042700 = write_file(
        path_042700,
        instrument_id=(
            "XKRX:042700"
        ),
        prefix="hanmi",
    )

    item_005930 = FakeCatalogFile(
        id=1,
        file_path="005930.parquet",
        remote_path=str(
            path_005930
        ),
        row_count=(
            table_005930.num_rows
        ),
        file_size=(
            path_005930.stat().st_size
        ),
        partition_date=(
            "2026-08-26"
        ),
        bucket="5f",
    )

    item_042700 = FakeCatalogFile(
        id=2,
        file_path="042700.parquet",
        remote_path=str(
            path_042700
        ),
        row_count=(
            table_042700.num_rows
        ),
        file_size=(
            path_042700.stat().st_size
        ),
        partition_date=(
            "2026-08-26"
        ),
        bucket="20",
    )

    catalog = FakeCatalog(
        {
            "5f": [
                item_005930,
            ],
            "20": [
                item_042700,
            ],
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
            instrument_ids=(
                "XKRX:005930",
                "XKRX:042700",
            ),
            start_date="2026-08-26",
            end_date="2026-08-26",
            timezone="UTC",
            columns=(
                "record_uid",
                "instrument_id",
                "event_time",
            ),
        )
    )

    assert (
        result.table.num_rows
        ==
        4
    )

    assert set(
        result.table[
            "instrument_id"
        ]
        .to_pylist()
    ) == {
        "XKRX:005930",
        "XKRX:042700",
    }

    assert len(
        catalog.calls
    ) == 1

    call = catalog.calls[0]

    assert (
        call["method"]
        ==
        "range_multi_bucket"
    )

    assert set(
        call["buckets"]
    ) == {
        "5f",
        "20",
    }


def test_large_universe_uses_single_catalog_query_for_100_instruments():

    instruments = tuple(
        f"XKRX:{number:06d}"
        for number in range(
            100
        )
    )

    catalog = FakeCatalog(
        {}
    )

    reader = CatalogParquetReader(
        catalog=catalog,
        resolver=CatalogFileResolver(),
    )

    stats = StreamingQueryStats()

    list(
        reader.iter_batches(
            QuerySpec(
                site_id="naver_finance",
                dataset="forum_post",
                country="KR",
                instrument_ids=instruments,
                start_date="2026-08-26",
                end_date="2026-08-26",
                timezone="UTC",
                columns=(
                    "record_uid",
                    "instrument_id",
                    "event_time",
                ),
            ),
            stats=stats,
        )
    )

    assert len(
        catalog.calls
    ) == 1

    call = catalog.calls[0]

    assert (
        call["method"]
        ==
        "range_multi_bucket"
    )

    observation = QueryScaleObservation(
        instrument_count=len(
            instruments
        ),
        unique_bucket_count=len(
            call["buckets"]
        ),
        catalog_sql_calls=(
            stats.catalog_sql_calls
        ),
        catalog_files=(
            stats.catalog_files
        ),
        candidate_physical_rows=(
            stats.candidate_physical_rows
        ),
        rows_yielded=(
            stats.rows_yielded
        ),
    )

    assert_catalog_sql_not_per_instrument(
        observation
    )


def test_large_universe_uses_single_catalog_query_for_1000_instruments():

    instruments = tuple(
        f"XKRX:{number:06d}"
        for number in range(
            1000
        )
    )

    catalog = FakeCatalog(
        {}
    )

    reader = CatalogParquetReader(
        catalog=catalog,
        resolver=CatalogFileResolver(),
    )

    stats = StreamingQueryStats()

    list(
        reader.iter_batches(
            QuerySpec(
                site_id="naver_finance",
                dataset="forum_post",
                country="KR",
                instrument_ids=instruments,
                start_date="2026-08-26",
                end_date="2026-08-26",
                timezone="UTC",
                columns=(
                    "record_uid",
                    "instrument_id",
                    "event_time",
                ),
            ),
            stats=stats,
        )
    )

    assert len(
        catalog.calls
    ) == 1

    call = catalog.calls[0]

    assert (
        call["method"]
        ==
        "range_multi_bucket"
    )

    assert len(
        call["buckets"]
    ) <= 256

    observation = QueryScaleObservation(
        instrument_count=len(
            instruments
        ),
        unique_bucket_count=len(
            call["buckets"]
        ),
        catalog_sql_calls=(
            stats.catalog_sql_calls
        ),
        catalog_files=(
            stats.catalog_files
        ),
        candidate_physical_rows=(
            stats.candidate_physical_rows
        ),
        rows_yielded=(
            stats.rows_yielded
        ),
    )

    assert_catalog_sql_not_per_instrument(
        observation
    )
    
