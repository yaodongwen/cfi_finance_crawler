import pytest

from crawl_framework.core.dataset import (
    DATASETS,
    get_dataset_spec,
    is_registered_dataset,
    list_datasets,
)


def test_dataset_registry():

    assert (
        "news_article"
        in DATASETS
    )

    assert (
        "forum_post"
        in DATASETS
    )

    assert (
        "research_report"
        in DATASETS
    )


def test_news_article():

    spec = get_dataset_spec(
        "news_article"
    )

    assert (
        spec.name
        == "news_article"
    )

    assert (
        spec.mutation_policy
        == "versioned"
    )

    assert (
        spec.requires_instrument
        is False
    )

    assert (
        spec.allows_multiple_instruments
        is True
    )


def test_forum_post():

    spec = get_dataset_spec(
        "forum_post"
    )

    assert (
        spec.default_scope_type
        == "instrument"
    )

    assert (
        spec.partition_time_field
        == "event_time"
    )


def test_holding_snapshot_requires_time():

    spec = get_dataset_spec(
        "holding_snapshot"
    )

    assert (
        spec.allow_missing_event_time
        is False
    )


def test_relation_dataset():

    spec = get_dataset_spec(
        "news_instrument"
    )

    assert (
        spec.relation_dataset
        is True
    )

    assert (
        spec.requires_instrument
        is True
    )


def test_financial_report_contract():

    spec = get_dataset_spec(
        "financial_report"
    )

    assert spec.mutation_policy == "versioned"
    assert spec.partition_time_field == "event_time"
    assert spec.default_scope_type == "instrument"
    assert spec.requires_instrument is True
    assert spec.allows_multiple_instruments is True
    assert spec.allow_missing_event_time is True
    assert spec.allow_missing_content is True
    assert spec.partition_time_granularity == "year"
    assert spec.partition_bucket_count == 1


def test_financial_report_instrument_contract():

    spec = get_dataset_spec(
        "financial_report_instrument"
    )

    assert spec.mutation_policy == "immutable"
    assert spec.partition_time_field == "crawled_at"
    assert spec.default_scope_type == "instrument"
    assert spec.requires_instrument is True
    assert spec.relation_dataset is True
    assert spec.partition_time_granularity == "year"
    assert spec.partition_bucket_count == 1


def test_attachment_remains_generic_for_financial_reports():

    spec = get_dataset_spec(
        "attachment"
    )

    assert spec.default_scope_type == "record"
    assert spec.mutation_policy == "immutable"
    assert spec.requires_instrument is False
    assert spec.partition_time_granularity == "day"
    assert spec.partition_bucket_count is None


def test_unknown_dataset():

    with pytest.raises(
        KeyError
    ):
        get_dataset_spec(
            "something_unknown"
        )


def test_is_registered_dataset():

    assert is_registered_dataset(
        "comment"
    )

    assert not is_registered_dataset(
        "unknown"
    )


def test_list_datasets():

    names = list_datasets()

    assert (
        names
        == sorted(names)
    )

    assert (
        "author_post"
        in names
    )
