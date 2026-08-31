import pytest

from crawl_framework.core.watermark import (
    ContiguousWatermark,
)


def test_watermark_does_not_advance_past_gap():

    watermark = ContiguousWatermark()

    result = watermark.mark_complete(
        3
    )

    assert (
        result.current
        == 0
    )

    assert (
        result.advanced
        is False
    )

    assert (
        watermark.pending_gap_count
        == 1
    )


def test_watermark_advances_when_gap_is_filled():

    watermark = ContiguousWatermark()

    watermark.mark_complete(
        3
    )

    result = watermark.mark_complete(
        1
    )

    assert (
        result.current
        == 1
    )

    result = watermark.mark_complete(
        2
    )

    assert (
        result.current
        == 3
    )

    assert (
        result.advanced
        is True
    )

    assert (
        watermark.pending_gap_count
        == 0
    )


def test_watermark_ignores_already_committed_sequence():

    watermark = ContiguousWatermark(
        start=5
    )

    result = watermark.mark_complete(
        4
    )

    assert (
        result.current
        == 5
    )

    assert (
        result.advanced
        is False
    )


def test_watermark_rejects_negative_start():

    with pytest.raises(
        ValueError,
        match="start",
    ):

        ContiguousWatermark(
            start=-1
        )
