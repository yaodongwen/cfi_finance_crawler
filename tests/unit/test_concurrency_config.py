import pytest

from crawl_framework.core.concurrency import (
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
