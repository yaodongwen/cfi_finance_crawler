import asyncio

import pytest

from crawl_framework.core.parallel_pipeline import (
    BoundedAsyncPipeline,
    PipelineStage,
)


async def source(
    count,
):

    for value in range(
        count
    ):

        yield value


@pytest.mark.asyncio
async def test_bounded_async_pipeline_processes_stages_in_order():

    async def double(
        item,
    ):

        return item * 2

    async def stringify(
        item,
    ):

        return str(
            item
        )

    pipeline = BoundedAsyncPipeline(
        [
            PipelineStage(
                name="write",
                worker_count=1,
                queue_size=2,
                handler=double,
            ),
            PipelineStage(
                name="upload",
                worker_count=1,
                queue_size=2,
                handler=stringify,
            ),
        ]
    )

    outputs, stats = await pipeline.run(
        source(
            3
        )
    )

    assert (
        outputs
        == [
            "0",
            "2",
            "4",
        ]
    )

    assert (
        stats.input_items
        == 3
    )

    assert (
        stats.output_items
        == 3
    )

    assert (
        stats.stage_counts
        == {
            "write": 3,
            "upload": 3,
        }
    )


@pytest.mark.asyncio
async def test_bounded_async_pipeline_supports_multi_worker_stages():

    async def expand(
        item,
    ):

        await asyncio.sleep(
            0
        )

        return (
            item,
            item + 10,
        )

    async def passthrough(
        item,
    ):

        return item

    pipeline = BoundedAsyncPipeline(
        [
            PipelineStage(
                name="crawl",
                worker_count=2,
                queue_size=2,
                handler=expand,
            ),
            PipelineStage(
                name="write",
                worker_count=3,
                queue_size=3,
                handler=passthrough,
            ),
        ]
    )

    outputs, stats = await pipeline.run(
        source(
            5
        )
    )

    assert sorted(
        outputs
    ) == sorted(
        [
            0,
            10,
            1,
            11,
            2,
            12,
            3,
            13,
            4,
            14,
        ]
    )

    assert (
        stats.stage_counts[
            "crawl"
        ]
        == 5
    )

    assert (
        stats.stage_counts[
            "write"
        ]
        == 10
    )


@pytest.mark.asyncio
async def test_bounded_async_pipeline_records_queue_upper_bounds():

    async def slow(
        item,
    ):

        await asyncio.sleep(
            0
        )

        return item

    pipeline = BoundedAsyncPipeline(
        [
            PipelineStage(
                name="records",
                worker_count=1,
                queue_size=1,
                handler=slow,
            ),
            PipelineStage(
                name="uploads",
                worker_count=1,
                queue_size=1,
                handler=slow,
            ),
        ]
    )

    _, stats = await pipeline.run(
        source(
            20
        )
    )

    assert (
        stats.max_queue_sizes[
            "records"
        ]
        <= 1
    )

    assert (
        stats.max_queue_sizes[
            "uploads"
        ]
        <= 1
    )


def test_pipeline_stage_rejects_invalid_worker_count():

    with pytest.raises(
        ValueError,
        match="worker_count",
    ):

        PipelineStage(
            name="bad",
            worker_count=0,
            queue_size=1,
            handler=lambda item: item,
        )
