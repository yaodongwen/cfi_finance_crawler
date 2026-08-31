from __future__ import annotations

import pytest

from crawl_framework.storage.scale import (
    FileSizeObservation,
    QueryScaleObservation,
    RolloutRunPlan,
    assert_catalog_sql_not_per_instrument,
    observation_to_dict,
    streaming_memory_is_bounded,
    summarize_file_sizes,
    validate_rollout_run_plan,
    StreamingMemoryObservation,
)


def test_query_scale_guard_rejects_one_sql_per_instrument():

    with pytest.raises(
        AssertionError,
        match="one-per-instrument",
    ):

        assert_catalog_sql_not_per_instrument(
            QueryScaleObservation(
                instrument_count=100,
                unique_bucket_count=80,
                catalog_sql_calls=100,
                catalog_files=0,
                candidate_physical_rows=0,
                rows_yielded=0,
            )
        )


def test_query_scale_guard_accepts_bucket_level_sql():

    assert_catalog_sql_not_per_instrument(
        QueryScaleObservation(
            instrument_count=1000,
            unique_bucket_count=251,
            catalog_sql_calls=1,
            catalog_files=12,
            candidate_physical_rows=10000,
            rows_yielded=500,
        )
    )


def test_streaming_memory_observation_requires_no_full_accumulation():

    assert streaming_memory_is_bounded(
        StreamingMemoryObservation(
            batches_yielded=10,
            max_batch_rows=1000,
            rows_yielded=10000,
            accumulated_result_rows=0,
        )
    )

    assert not streaming_memory_is_bounded(
        StreamingMemoryObservation(
            batches_yielded=10,
            max_batch_rows=1000,
            rows_yielded=10000,
            accumulated_result_rows=10000,
        )
    )


def test_summarize_file_sizes_records_tuning_inputs():

    summary = summarize_file_sizes(
        [
            (
                100,
                10,
            ),
            (
                300,
                30,
            ),
        ]
    )

    assert summary == FileSizeObservation(
        file_count=2,
        total_bytes=400,
        total_rows=40,
        min_bytes=100,
        max_bytes=300,
    )

    assert observation_to_dict(
        summary
    )["total_bytes"] == 400


def test_rollout_plan_blocks_large_production_write_without_authorization():

    with pytest.raises(
        ValueError,
        match="staged authorization",
    ):

        validate_rollout_run_plan(
            RolloutRunPlan(
                instrument_count=500,
                duration_minutes=60,
                production=True,
                read_only=False,
            )
        )


def test_rollout_plan_allows_read_only_large_validation():

    validate_rollout_run_plan(
        RolloutRunPlan(
            instrument_count=1000,
            duration_minutes=30,
            production=True,
            read_only=True,
        )
    )
