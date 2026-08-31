from __future__ import annotations

import pytest

from datetime import date
from pathlib import Path

import pyarrow as pa

from crawl_framework.core.plugin import (
    CrawlContext,
)
from crawl_framework.core.pipeline import (
    StoragePipeline,
)
from crawl_framework.core.concurrent_runtime import (
    ConcurrentProductionRuntime,
)
from crawl_framework.core.runtime import (
    CrawlRuntime,
)
from crawl_framework.storage.buffer import (
    BufferConfig,
    RecordBuffer,
)
from crawl_framework.storage.checkpoint import (
    FileCheckpointStore,
)
from crawl_framework.storage.cleaner import (
    Cleaner,
    CleanupResult,
)
from crawl_framework.storage.parquet_writer import (
    ParquetWriter,
)
from crawl_framework.storage.postgres import (
    CatalogDataFile,
    PostgresCatalog,
)
from crawl_framework.storage.query import (
    CatalogFileResolver,
    CatalogParquetReader,
    QuerySpec,
)
from crawl_framework.storage.record_index import (
    RecordIndexStore,
)
from crawl_framework.storage.recovery import (
    RecoveryStore,
)
from crawl_framework.storage.seen_store import (
    SQLiteSeenStore,
)
from crawl_framework.storage.uploader import (
    LocalUploader,
)
from crawl_framework.sites.tossinvest import (
    TossInvestPlugin,
)


class FakeCursor:

    def __init__(
        self,
        log,
    ):

        self.log = log

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
    ):

        self.executed = []
        self.commit_count = 0

    def cursor(
        self,
    ):

        return FakeCursor(
            self.executed
        )

    def commit(
        self,
    ):

        self.commit_count += 1


class KeepFilesCleaner(
    Cleaner
):

    def clean(
        self,
        info,
        context,
    ):

        return CleanupResult(
            file_path=info.file_path,
            decision="delete",
            deleted=False,
            reason="architecture smoke keeps local files",
            record_index_deleted=False,
        )


class FakeCatalog:

    def __init__(
        self,
        files,
    ):

        self.files = files
        self.calls = []

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
                "bucket": bucket,
            }
        )

        return [
            item
            for item in self.files
            if (
                (
                    bucket is None
                    or item.bucket == bucket
                )
                and
                start_partition_date
                <= item.partition_date
                <= end_partition_date
            )
        ]


def catalog_files_from_connection(
    connection,
    *,
    remote_root: Path,
):

    files = []

    for sql, params in connection.executed:

        if (
            "INSERT INTO marketdata.data_files"
            not in sql
        ):

            continue

        remote_path = (
            remote_root
            /
            params[
                "file_path"
            ]
        )

        files.append(
            CatalogDataFile(
                id=len(
                    files
                )
                + 1,
                site_id=params[
                    "site_id"
                ],
                country=params[
                    "country"
                ],
                dataset=params[
                    "dataset"
                ],
                partition_date=params[
                    "partition_date"
                ],
                bucket=params[
                    "bucket"
                ],
                file_path=params[
                    "file_path"
                ],
                sha256=params[
                    "sha256"
                ],
                row_count=params[
                    "row_count"
                ],
                file_size=params[
                    "file_size"
                ],
                min_event_time=params[
                    "min_event_time"
                ],
                max_event_time=params[
                    "max_event_time"
                ],
                schema_version=params[
                    "schema_version"
                ],
                storage_status="uploaded",
                lifecycle_status="active",
                remote_path=(
                    remote_path
                    .as_posix()
                ),
            )
        )

    return files


@pytest.mark.asyncio
async def test_tossinvest_runtime_storage_query_smoke_without_core_changes(
    tmp_path,
):

    remote_root = (
        tmp_path
        /
        "remote"
    )

    seen_store = SQLiteSeenStore(
        tmp_path
        /
        "state"
        /
        "seen.sqlite3"
    )

    checkpoint_store = FileCheckpointStore(
        tmp_path
        /
        "state"
        /
        "checkpoints"
    )

    connection = FakeConnection()

    record_index_store = RecordIndexStore()

    pipeline = StoragePipeline(
        seen_store=seen_store,
        buffer=RecordBuffer(
            config=BufferConfig(
                min_rows=1,
                max_rows=10,
                target_bytes=1024 * 1024,
                flush_seconds=30,
            )
        ),
        parquet_writer=ParquetWriter(
            tmp_path
            /
            "warehouse"
        ),
        uploader=LocalUploader(
            remote_root,
            verify_size=True,
            verify_sha256=True,
        ),
        catalog=PostgresCatalog(
            connection
        ),
        recovery_store=RecoveryStore(
            tmp_path
            /
            "state"
            /
            "recovery"
        ),
        cleaner=KeepFilesCleaner(),
        checkpoint_store=checkpoint_store,
        record_index_store=record_index_store,
    )

    runtime = CrawlRuntime(
        plugin=TossInvestPlugin(),
        pipeline=pipeline,
        checkpoint_store=checkpoint_store,
        context=CrawlContext(
            extra={
                "instruments": [
                    "A005930",
                ],
                "tossinvest_raw": {
                    "forum_post": {
                        "A005930": [
                            {
                                "id": "100",
                                "symbol": "A005930",
                                "created_at": (
                                    "2026-08-26T09:00:00+09:00"
                                ),
                                "title": "first",
                                "content": "hello",
                            },
                        ]
                    }
                },
            }
        ),
    )

    results = await runtime.run(
        datasets=(
            "forum_post",
        ),
        flush_at_end=True,
    )

    assert len(
        results
    ) == 1

    assert (
        runtime.stats.normalized_records
        == 1
    )

    assert (
        pipeline.stats.records_new
        == 1
    )

    assert (
        pipeline.stats.files_written
        == 1
    )

    second_runtime = CrawlRuntime(
        plugin=TossInvestPlugin(),
        pipeline=pipeline,
        checkpoint_store=FileCheckpointStore(
            tmp_path
            /
            "state"
            /
            "second_checkpoints"
        ),
        context=runtime.context,
    )

    await second_runtime.run(
        datasets=(
            "forum_post",
        ),
        flush_at_end=True,
    )

    assert (
        pipeline.stats.records_new
        == 1
    )

    assert (
        pipeline.stats.records_unchanged
        == 1
    )

    assert (
        pipeline.stats.files_written
        == 1
    )

    checkpoint_files = list(
        (
            tmp_path
            /
            "state"
            /
            "checkpoints"
        ).rglob(
            "*.json"
        )
    )

    assert len(
        checkpoint_files
    ) == 1

    catalog_files = catalog_files_from_connection(
        connection,
        remote_root=remote_root,
    )

    assert len(
        catalog_files
    ) == 1

    reader = CatalogParquetReader(
        catalog=FakeCatalog(
            catalog_files
        ),
        resolver=CatalogFileResolver(),
    )

    result = reader.query(
        QuerySpec(
            site_id="tossinvest",
            dataset="forum_post",
            country="KR",
            instrument_id="XKRX:005930",
            start_date=date(
                2026,
                8,
                26,
            ),
            end_date=date(
                2026,
                8,
                26,
            ),
            timezone="Asia/Seoul",
            columns=(
                "site_id",
                "dataset",
                "source_id",
                "instrument_id",
                "title",
            ),
        )
    )

    assert result.table.num_rows == 1

    row = result.table.to_pylist()[0]

    assert row["site_id"] == "tossinvest"
    assert row["dataset"] == "forum_post"
    assert row["source_id"] == "100"
    assert row["instrument_id"] == "XKRX:005930"

    assert (
        result.stats.catalog_files
        == 1
    )


@pytest.mark.asyncio
async def test_tossinvest_reuses_concurrent_production_runtime_without_core_changes(
    tmp_path,
):

    remote_root = (
        tmp_path
        /
        "remote"
    )

    checkpoint_store = FileCheckpointStore(
        tmp_path
        /
        "state"
        /
        "checkpoints"
    )

    connection = FakeConnection()

    record_index_store = RecordIndexStore()

    pipeline = StoragePipeline(
        seen_store=SQLiteSeenStore(
            tmp_path
            /
            "state"
            /
            "seen.sqlite3"
        ),
        buffer=RecordBuffer(
            config=BufferConfig(
                min_rows=1,
                max_rows=2,
                target_bytes=1024 * 1024,
                flush_seconds=30,
            )
        ),
        parquet_writer=ParquetWriter(
            tmp_path
            /
            "warehouse"
        ),
        uploader=LocalUploader(
            remote_root,
            verify_size=True,
            verify_sha256=True,
        ),
        catalog=PostgresCatalog(
            connection
        ),
        recovery_store=RecoveryStore(
            tmp_path
            /
            "state"
            /
            "recovery"
        ),
        cleaner=KeepFilesCleaner(),
        checkpoint_store=checkpoint_store,
        record_index_store=record_index_store,
    )

    runtime = ConcurrentProductionRuntime(
        plugin=TossInvestPlugin(),
        pipeline=pipeline,
        checkpoint_store=checkpoint_store,
        context=CrawlContext(
            extra={
                "instruments": [
                    "A005930",
                    "A000660",
                ],
                "tossinvest_raw": {
                    "forum_post": {
                        "A005930": [
                            {
                                "id": "100",
                                "symbol": "A005930",
                                "created_at": (
                                    "2026-08-26T09:00:00+09:00"
                                ),
                                "title": "samsung first",
                                "content": "hello",
                            },
                            {
                                "id": "101",
                                "symbol": "A005930",
                                "created_at": (
                                    "2026-08-26T09:01:00+09:00"
                                ),
                                "title": "samsung second",
                                "content": "world",
                            },
                        ],
                        "A000660": [
                            {
                                "id": "200",
                                "symbol": "A000660",
                                "created_at": (
                                    "2026-08-26T10:00:00+09:00"
                                ),
                                "title": "hynix first",
                                "content": "hello",
                            },
                        ],
                    }
                },
            }
        ),
        crawl_workers=2,
        writer_workers=1,
        upload_workers=2,
        catalog_workers=2,
        record_queue_size=2,
        upload_queue_size=2,
        catalog_queue_size=2,
    )

    result = await runtime.run(
        datasets=(
            "forum_post",
        ),
        flush_at_end=True,
    )

    assert (
        result["call_graph"]
        ==
        "cli.main -> app_factory -> CrawlBootstrap -> "
        "ConcurrentProductionRuntime -> crawl_queue -> "
        "record_queue -> writer workers -> upload_queue -> "
        "upload workers -> catalog_queue -> catalog workers -> "
        "SeenStore/checkpoint"
    )

    stats = result[
        "production_stats"
    ]

    assert (
        stats.crawl_workers
        == 2
    )

    assert (
        stats.upload_workers
        == 2
    )

    assert (
        stats.catalog_workers
        == 2
    )

    assert (
        stats.records_crawled
        == 3
    )

    assert (
        stats.uploads_started
        ==
        stats.uploads_completed
        ==
        stats.catalog_jobs_completed
        ==
        2
    )

    assert (
        pipeline.stats.records_new
        == 3
    )

    assert (
        pipeline.stats.files_written
        == 2
    )

    assert (
        pipeline.stats.files_registered
        == 2
    )

    checkpoint_files = list(
        (
            tmp_path
            /
            "state"
            /
            "checkpoints"
        ).rglob(
            "*.json"
        )
    )

    assert len(
        checkpoint_files
    ) == 2

    catalog_files = catalog_files_from_connection(
        connection,
        remote_root=remote_root,
    )

    assert len(
        catalog_files
    ) == 2

    reader = CatalogParquetReader(
        catalog=FakeCatalog(
            catalog_files
        ),
        resolver=CatalogFileResolver(),
    )

    query_result = reader.query(
        QuerySpec(
            site_id="tossinvest",
            dataset="forum_post",
            country="KR",
            instrument_id="XKRX:005930",
            start_date=date(
                2026,
                8,
                26,
            ),
            end_date=date(
                2026,
                8,
                26,
            ),
            timezone="Asia/Seoul",
            columns=(
                "site_id",
                "dataset",
                "source_id",
                "instrument_id",
                "title",
            ),
        )
    )

    assert (
        query_result.table.num_rows
        == 2
    )
