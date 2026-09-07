import pytest

from crawl_framework.core.concurrency import (
    DatasetResourceBudget,
    QueueSizeConfig,
    StageConcurrencyConfig,
)


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
