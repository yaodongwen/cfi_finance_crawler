from datetime import datetime, timezone

import pytest

from crawl_framework.core.models import (
    CanonicalRecord,
)
from crawl_framework.storage.buffer import (
    FlushBatch,
)
from crawl_framework.storage.catalog_workers import (
    CatalogIndexJob,
    ParallelCatalogConfig,
    ParallelCatalogIndexWorkers,
)
from crawl_framework.storage.parquet_writer import (
    ParquetWriter,
)
from crawl_framework.storage.partition import (
    Partitioner,
)
from crawl_framework.storage.uploader import (
    UploadResult,
)


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
        content="test content",
    )

    partitioner = Partitioner()

    batch = FlushBatch(
        key=partitioner.partition_for(
            record
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


def upload_result_for(
    info,
):

    return UploadResult(
        local_path=info.file_path,
        remote_path=str(
            info.relative_path
        ),
        status="verified",
        local_size=info.file_size,
        remote_size=info.file_size,
        local_sha256=info.sha256,
        remote_sha256=info.sha256,
    )


@pytest.mark.asyncio
async def test_parallel_catalog_workers_register_all_jobs(
    tmp_path,
):

    files = [
        make_parquet_info(
            tmp_path
            / f"run-{index}"
        )
        for index in range(
            3
        )
    ]

    jobs = [
        CatalogIndexJob(
            parquet_info=info,
            upload_result=upload_result_for(
                info
            ),
        )
        for info in files
    ]

    events = []

    def register_index(
        job,
    ):

        events.append(
            (
                "index",
                job.parquet_info.relative_path,
            )
        )

    def register_catalog(
        job,
    ):

        events.append(
            (
                "catalog",
                job.parquet_info.relative_path,
            )
        )

    workers = ParallelCatalogIndexWorkers(
        register_catalog=register_catalog,
        register_record_index=register_index,
        config=ParallelCatalogConfig(
            worker_count=2,
            queue_size=2,
        ),
    )

    result = await workers.run(
        jobs
    )

    assert (
        result.registered
        == 3
    )

    assert (
        result.max_queue_size
        <= 2
    )

    for job in jobs:

        index_position = events.index(
            (
                "index",
                job.parquet_info.relative_path,
            )
        )

        catalog_position = events.index(
            (
                "catalog",
                job.parquet_info.relative_path,
            )
        )

        assert (
            index_position
            <
            catalog_position
        )


def test_parallel_catalog_config_rejects_invalid_values():

    with pytest.raises(
        ValueError,
        match="queue_size",
    ):

        ParallelCatalogConfig(
            queue_size=0
        )
