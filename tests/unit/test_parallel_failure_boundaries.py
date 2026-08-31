from pathlib import Path
from types import SimpleNamespace

import pytest

from crawl_framework.core.parallel_pipeline import (
    BoundedAsyncPipeline,
    PipelineStage,
)
from crawl_framework.storage.catalog_workers import (
    CatalogIndexJob,
    ParallelCatalogIndexWorkers,
)
from crawl_framework.storage.upload_workers import (
    ParallelUploadWorkers,
)
from crawl_framework.storage.uploader import (
    UploadResult,
)


async def source():

    yield 1


def make_file_info(
    tmp_path,
):

    path = (
        tmp_path
        / "part.parquet"
    )

    path.write_bytes(
        b"data"
    )

    return SimpleNamespace(
        file_path=path,
        relative_path=Path(
            "part.parquet"
        ),
        file_size=4,
        sha256="sha",
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
async def test_parallel_pipeline_propagates_stage_failure():

    async def fail(
        item,
    ):

        raise RuntimeError(
            "stage failed"
        )

    pipeline = BoundedAsyncPipeline(
        [
            PipelineStage(
                name="crawl",
                worker_count=1,
                queue_size=1,
                handler=fail,
            )
        ]
    )

    with pytest.raises(
        RuntimeError,
        match="stage failed",
    ):

        await pipeline.run(
            source()
        )


class FailingUploader:

    def upload(
        self,
        info,
    ):

        raise RuntimeError(
            "upload failed"
        )


@pytest.mark.asyncio
async def test_parallel_upload_failure_propagates_without_result(
    tmp_path,
):

    info = make_file_info(
        tmp_path
    )

    workers = ParallelUploadWorkers(
        FailingUploader()
    )

    with pytest.raises(
        RuntimeError,
        match="upload failed",
    ):

        await workers.run(
            [
                info,
            ]
        )


@pytest.mark.asyncio
async def test_parallel_catalog_failure_propagates_without_registered_count(
    tmp_path,
):

    info = make_file_info(
        tmp_path
    )

    job = CatalogIndexJob(
        parquet_info=info,
        upload_result=upload_result_for(
            info
        ),
    )

    events = []

    def register_index(
        item,
    ):

        events.append(
            "index"
        )

    def register_catalog(
        item,
    ):

        raise RuntimeError(
            "catalog failed"
        )

    workers = ParallelCatalogIndexWorkers(
        register_catalog=register_catalog,
        register_record_index=register_index,
    )

    with pytest.raises(
        RuntimeError,
        match="catalog failed",
    ):

        await workers.run(
            [
                job,
            ]
        )

    assert (
        events
        == [
            "index",
        ]
    )
