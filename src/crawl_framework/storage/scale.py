from __future__ import annotations

from dataclasses import (
    asdict,
    dataclass,
)


@dataclass(
    frozen=True,
    slots=True,
)
class QueryScaleObservation:
    instrument_count: int

    unique_bucket_count: int

    catalog_sql_calls: int

    catalog_files: int

    candidate_physical_rows: int

    rows_yielded: int


@dataclass(
    frozen=True,
    slots=True,
)
class StreamingMemoryObservation:
    batches_yielded: int

    max_batch_rows: int

    rows_yielded: int

    accumulated_result_rows: int = 0


@dataclass(
    frozen=True,
    slots=True,
)
class FileSizeObservation:
    file_count: int

    total_bytes: int

    total_rows: int

    min_bytes: int

    max_bytes: int


@dataclass(
    frozen=True,
    slots=True,
)
class RolloutRunPlan:
    instrument_count: int

    duration_minutes: int

    read_only: bool = False

    production: bool = False


def assert_catalog_sql_not_per_instrument(
    observation: QueryScaleObservation,
) -> None:
    """
    Guardrail for large universe queries.
    """

    if observation.instrument_count < 1:

        raise ValueError(
            "instrument_count must be >= 1"
        )

    if (
        observation.catalog_sql_calls
        >=
        observation.instrument_count
    ):

        raise AssertionError(
            "catalog SQL calls scale one-per-instrument"
        )


def streaming_memory_is_bounded(
    observation: StreamingMemoryObservation,
) -> bool:
    """
    Streaming is bounded when no full result rows are accumulated
    and batch size remains below yielded rows for multi-batch scans.
    """

    if observation.accumulated_result_rows != 0:

        return False

    if observation.batches_yielded < 1:

        return observation.rows_yielded == 0

    return (
        observation.max_batch_rows
        <=
        max(
            observation.rows_yielded,
            1,
        )
    )


def summarize_file_sizes(
    sizes: list[tuple[int, int]],
) -> FileSizeObservation:
    """
    sizes entries are (bytes, rows).
    """

    if not sizes:

        raise ValueError(
            "sizes cannot be empty"
        )

    byte_values = [
        item[0]
        for item in sizes
    ]

    row_values = [
        item[1]
        for item in sizes
    ]

    return FileSizeObservation(
        file_count=len(
            sizes
        ),
        total_bytes=sum(
            byte_values
        ),
        total_rows=sum(
            row_values
        ),
        min_bytes=min(
            byte_values
        ),
        max_bytes=max(
            byte_values
        ),
    )


def validate_rollout_run_plan(
    plan: RolloutRunPlan,
) -> None:
    """
    Production rollout must be explicit and bounded.
    """

    if plan.instrument_count < 1:

        raise ValueError(
            "instrument_count must be >= 1"
        )

    if plan.duration_minutes < 1:

        raise ValueError(
            "duration_minutes must be >= 1"
        )

    if (
        plan.production
        and
        plan.instrument_count > 50
        and
        not plan.read_only
    ):

        raise ValueError(
            "production write rollout above 50 instruments "
            "requires explicit staged authorization"
        )


def observation_to_dict(
    observation,
) -> dict:

    return asdict(
        observation
    )
