import time
from datetime import datetime, timezone

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
from crawl_framework.storage.upload_workers import (
    ParallelUploadConfig,
    ParallelUploadWorkers,
)
from crawl_framework.storage.uploader import (
    UploadResult,
)


class FakeUploader:

    def __init__(
        self,
        *,
        delay=0.0,
    ):

        self.delay = delay
        self.calls = []


    def upload(
        self,
        info,
    ):

        if self.delay:

            time.sleep(
                self.delay
            )

        self.calls.append(
            info.relative_path
        )

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


@pytest.mark.asyncio
async def test_parallel_upload_workers_upload_all_files(
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

    uploader = FakeUploader()

    workers = ParallelUploadWorkers(
        uploader,
        config=ParallelUploadConfig(
            worker_count=2,
            queue_size=2,
        ),
    )

    result = await workers.run(
        files
    )

    assert (
        len(
            result.uploads
        )
        == 3
    )

    assert all(
        upload.verified
        for upload in result.uploads
    )

    assert (
        len(
            uploader.calls
        )
        == 3
    )

    assert (
        result.max_queue_size
        <= 2
    )


def test_parallel_upload_config_rejects_invalid_values():

    with pytest.raises(
        ValueError,
        match="worker_count",
    ):

        ParallelUploadConfig(
            worker_count=0
        )
