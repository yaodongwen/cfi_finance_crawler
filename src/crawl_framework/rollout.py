from __future__ import annotations

from dataclasses import (
    asdict,
    dataclass,
)
from enum import Enum
from typing import Sequence


class RolloutStage(
    str,
    Enum,
):
    """
    Phase G rollout order.
    """

    INSTRUMENTS_50 = "50_instruments"
    INSTRUMENTS_500 = "500_instruments"
    FULL_MARKET = "full_market"


ROLLOUT_ORDER: tuple[
    RolloutStage,
    ...
] = (
    RolloutStage.INSTRUMENTS_50,
    RolloutStage.INSTRUMENTS_500,
    RolloutStage.FULL_MARKET,
)


@dataclass(
    frozen=True,
    slots=True,
)
class RolloutStageSpec:
    stage: RolloutStage

    minimum_instruments: int

    maximum_instruments: int | None


@dataclass(
    frozen=True,
    slots=True,
)
class RolloutObservation:
    stage: RolloutStage

    instrument_count: int

    attempted_scopes: int

    successful_scopes: int

    failed_scopes: int

    records_written: int

    files_written: int

    max_file_bytes: int

    recovery_failed: int = 0

    pending_recovery: int = 0


@dataclass(
    frozen=True,
    slots=True,
)
class RolloutDecision:
    can_expand: bool

    reasons: tuple[
        str,
        ...
    ]


STAGE_SPECS: dict[
    RolloutStage,
    RolloutStageSpec,
] = {
    RolloutStage.INSTRUMENTS_50:
        RolloutStageSpec(
            stage=RolloutStage.INSTRUMENTS_50,
            minimum_instruments=50,
            maximum_instruments=50,
        ),
    RolloutStage.INSTRUMENTS_500:
        RolloutStageSpec(
            stage=RolloutStage.INSTRUMENTS_500,
            minimum_instruments=500,
            maximum_instruments=500,
        ),
    RolloutStage.FULL_MARKET:
        RolloutStageSpec(
            stage=RolloutStage.FULL_MARKET,
            minimum_instruments=501,
            maximum_instruments=None,
        ),
}


def next_rollout_stage(
    completed: Sequence[
        RolloutStage
    ],
) -> RolloutStage | None:
    """
    Return the first Phase G stage that has not completed.
    """

    completed_set = set(
        completed
    )

    for stage in ROLLOUT_ORDER:

        if stage not in completed_set:

            return stage

    return None


def validate_stage_instruments(
    stage: RolloutStage,
    instruments: Sequence[
        str
    ],
) -> None:
    """
    Enforce 50 -> 500 -> full-market sizing.
    """

    spec = STAGE_SPECS[
        stage
    ]

    unique = tuple(
        dict.fromkeys(
            str(
                value
            ).strip()
            for value in instruments
            if str(
                value
            ).strip()
        )
    )

    count = len(
        unique
    )

    if count < spec.minimum_instruments:

        raise ValueError(
            f"{stage.value} requires at least "
            f"{spec.minimum_instruments} instruments"
        )

    if (
        spec.maximum_instruments is not None
        and count > spec.maximum_instruments
    ):

        raise ValueError(
            f"{stage.value} allows at most "
            f"{spec.maximum_instruments} instruments"
        )


def evaluate_rollout_observation(
    observation: RolloutObservation,
    *,
    max_failure_rate: float = 0.02,
    max_file_bytes: int = 512 * 1024 * 1024,
) -> RolloutDecision:
    """
    Decide whether the next Phase G stage may start.
    """

    reasons: list[
        str
    ] = []

    if observation.instrument_count < 1:

        reasons.append(
            "instrument_count must be >= 1"
        )

    if observation.attempted_scopes < 1:

        reasons.append(
            "attempted_scopes must be >= 1"
        )

    failure_rate = (
        observation.failed_scopes
        /
        observation.attempted_scopes
        if observation.attempted_scopes > 0
        else 1.0
    )

    if failure_rate > max_failure_rate:

        reasons.append(
            "scope failure rate exceeded threshold"
        )

    if observation.recovery_failed:

        reasons.append(
            "recovery failures remain"
        )

    if observation.pending_recovery:

        reasons.append(
            "pending recovery remains"
        )

    if observation.max_file_bytes > max_file_bytes:

        reasons.append(
            "file size threshold exceeded"
        )

    return RolloutDecision(
        can_expand=(
            not reasons
        ),
        reasons=tuple(
            reasons
        ),
    )


def observation_to_dict(
    observation: RolloutObservation,
) -> dict:

    data = asdict(
        observation
    )

    data[
        "stage"
    ] = observation.stage.value

    return data
