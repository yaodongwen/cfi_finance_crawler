import pytest

from crawl_framework.rollout import (
    RolloutObservation,
    RolloutStage,
    evaluate_rollout_observation,
    next_rollout_stage,
    observation_to_dict,
    validate_stage_instruments,
)


def codes(
    count,
):

    return [
        f"{index:06d}"
        for index in range(
            count
        )
    ]


def test_next_rollout_stage_follows_authoritative_order():

    assert (
        next_rollout_stage(
            []
        )
        == RolloutStage.INSTRUMENTS_50
    )

    assert (
        next_rollout_stage(
            [
                RolloutStage.INSTRUMENTS_50,
            ]
        )
        == RolloutStage.INSTRUMENTS_500
    )

    assert (
        next_rollout_stage(
            [
                RolloutStage.INSTRUMENTS_50,
                RolloutStage.INSTRUMENTS_500,
            ]
        )
        == RolloutStage.FULL_MARKET
    )

    assert (
        next_rollout_stage(
            [
                RolloutStage.INSTRUMENTS_50,
                RolloutStage.INSTRUMENTS_500,
                RolloutStage.FULL_MARKET,
            ]
        )
        is None
    )


def test_validate_stage_instruments_enforces_50_instrument_batch():

    validate_stage_instruments(
        RolloutStage.INSTRUMENTS_50,
        codes(
            50
        ),
    )

    with pytest.raises(
        ValueError,
        match="at least 50",
    ):

        validate_stage_instruments(
            RolloutStage.INSTRUMENTS_50,
            codes(
                49
            ),
        )

    with pytest.raises(
        ValueError,
        match="at most 50",
    ):

        validate_stage_instruments(
            RolloutStage.INSTRUMENTS_50,
            codes(
                51
            ),
        )


def test_validate_stage_instruments_enforces_500_instrument_batch():

    validate_stage_instruments(
        RolloutStage.INSTRUMENTS_500,
        codes(
            500
        ),
    )

    with pytest.raises(
        ValueError,
        match="at least 500",
    ):

        validate_stage_instruments(
            RolloutStage.INSTRUMENTS_500,
            codes(
                499
            ),
        )

    with pytest.raises(
        ValueError,
        match="at most 500",
    ):

        validate_stage_instruments(
            RolloutStage.INSTRUMENTS_500,
            codes(
                501
            ),
        )


def test_validate_stage_instruments_requires_full_market_after_500():

    validate_stage_instruments(
        RolloutStage.FULL_MARKET,
        codes(
            501
        ),
    )

    with pytest.raises(
        ValueError,
        match="at least 501",
    ):

        validate_stage_instruments(
            RolloutStage.FULL_MARKET,
            codes(
                500
            ),
        )


def test_rollout_observation_can_expand_when_clean():

    decision = evaluate_rollout_observation(
        RolloutObservation(
            stage=RolloutStage.INSTRUMENTS_50,
            instrument_count=50,
            attempted_scopes=50,
            successful_scopes=50,
            failed_scopes=0,
            records_written=100,
            files_written=2,
            max_file_bytes=1024,
        )
    )

    assert (
        decision.can_expand
        is True
    )

    assert (
        decision.reasons
        == ()
    )


def test_rollout_observation_holds_on_failures_and_recovery():

    decision = evaluate_rollout_observation(
        RolloutObservation(
            stage=RolloutStage.INSTRUMENTS_500,
            instrument_count=500,
            attempted_scopes=500,
            successful_scopes=400,
            failed_scopes=100,
            records_written=1000,
            files_written=20,
            max_file_bytes=1024,
            recovery_failed=1,
            pending_recovery=1,
        )
    )

    assert (
        decision.can_expand
        is False
    )

    assert (
        "scope failure rate exceeded threshold"
        in decision.reasons
    )

    assert (
        "recovery failures remain"
        in decision.reasons
    )

    assert (
        "pending recovery remains"
        in decision.reasons
    )


def test_rollout_observation_holds_on_large_files():

    decision = evaluate_rollout_observation(
        RolloutObservation(
            stage=RolloutStage.INSTRUMENTS_50,
            instrument_count=50,
            attempted_scopes=50,
            successful_scopes=50,
            failed_scopes=0,
            records_written=1000,
            files_written=1,
            max_file_bytes=10,
        ),
        max_file_bytes=9,
    )

    assert (
        decision.can_expand
        is False
    )

    assert (
        decision.reasons
        == (
            "file size threshold exceeded",
        )
    )


def test_observation_to_dict_uses_stage_value():

    data = observation_to_dict(
        RolloutObservation(
            stage=RolloutStage.FULL_MARKET,
            instrument_count=900,
            attempted_scopes=900,
            successful_scopes=900,
            failed_scopes=0,
            records_written=2000,
            files_written=10,
            max_file_bytes=2048,
        )
    )

    assert (
        data[
            "stage"
        ]
        == "full_market"
    )
