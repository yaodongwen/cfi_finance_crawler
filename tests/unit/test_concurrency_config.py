import pytest

import asyncio

import pytest

from crawl_framework.core.concurrency import (
    GlobalStageBudget,
    DatasetResourceBudget,
    QueueSizeConfig,
    StageConcurrencyConfig,
)


@pytest.mark.asyncio
async def test_global_stage_budget_caps_and_observes_shared_uploads():
    budget = GlobalStageBudget(
        writer_workers=3,
        upload_workers=2,
        catalog_workers=4,
    )
    active = 0
    observed_max = 0

    async def upload_job():
        nonlocal active, observed_max
        async with budget.upload_slot():
            active += 1
            observed_max = max(observed_max, active)
            await asyncio.sleep(0.01)
            active -= 1

    await asyncio.gather(*(upload_job() for _ in range(8)))
    snapshot = budget.snapshot()

    assert observed_max == 2
    assert snapshot.upload.limit == 2
    assert snapshot.upload.max_active == 2
    assert snapshot.upload.waits > 0
    assert snapshot.upload.active == 0
    assert snapshot.to_dict()["writer"]["limit"] == 3


def test_global_stage_budget_rejects_non_positive_limit():
    with pytest.raises(ValueError, match="limit must be >= 1"):
        GlobalStageBudget(upload_workers=0)


def test_stage_concurrency_defaults_are_positive():

    config = StageConcurrencyConfig()

    assert (
        config.crawl_workers
        == 1
    )

    assert (
        config.catalog_workers
        == 1
    )


def test_stage_concurrency_rejects_non_positive_values():

    with pytest.raises(
        ValueError,
        match="crawl_workers",
    ):

        StageConcurrencyConfig(
            crawl_workers=0
        )


def test_queue_size_defaults_are_positive():

    config = QueueSizeConfig()

    assert (
        config.records
        == 1000
    )

    assert (
        config.catalog
        == 128
    )


def test_queue_size_rejects_non_positive_values():

    with pytest.raises(
        ValueError,
        match="uploads",
    ):

        QueueSizeConfig(
            uploads=0
        )


def test_dataset_resource_budget_accepts_optional_positive_values():

    budget = DatasetResourceBudget(
        crawl_workers=2,
        http_concurrency=3,
        detail_workers=4,
        attachment_workers=5,
    )

    assert budget.crawl_workers == 2
    assert budget.http_concurrency == 3
    assert budget.detail_workers == 4
    assert budget.attachment_workers == 5


def test_dataset_resource_budget_rejects_non_positive_values():

    with pytest.raises(
        ValueError,
        match="detail_workers",
    ):

        DatasetResourceBudget(
            detail_workers=0
        )
