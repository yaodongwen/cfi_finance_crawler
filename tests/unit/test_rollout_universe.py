import pytest

from crawl_framework.rollout_universe import (
    deduplicate_instruments,
    read_universe_snapshot,
    stage_instruments,
    validate_rollout_prefixes,
    write_universe_snapshot,
)


def universe(
    count,
):

    return tuple(
        f"XKRX:{index:06d}"
        for index in range(
            count
        )
    )


def test_deduplicate_instruments_preserves_stable_order():

    assert (
        deduplicate_instruments(
            [
                " XKRX:005930 ",
                "XKRX:000660",
                "XKRX:005930",
                "",
            ]
        )
        == (
            "XKRX:005930",
            "XKRX:000660",
        )
    )


def test_deduplicate_instruments_rejects_non_canonical_values():

    with pytest.raises(
        ValueError,
        match="not a canonical",
    ):

        deduplicate_instruments(
            [
                "005930",
            ]
        )


def test_stage_instruments_uses_snapshot_prefix():

    values = universe(
        600
    )

    assert (
        stage_instruments(
            values,
            50,
        )
        == values[
            :50
        ]
    )

    assert (
        stage_instruments(
            values,
            500,
        )
        == values[
            :500
        ]
    )


def test_stage_instruments_rejects_insufficient_universe():

    with pytest.raises(
        ValueError,
        match="requires at least 50",
    ):

        stage_instruments(
            universe(
                49
            ),
            50,
        )


def test_validate_rollout_prefixes_accepts_full_universe():

    validate_rollout_prefixes(
        universe(
            600
        )
    )


def test_snapshot_write_and_read_are_reproducible(
    tmp_path,
):

    path = (
        tmp_path
        / "naver_finance_kr_rollout_universe.txt"
    )

    written = write_universe_snapshot(
        path,
        universe(
            600
        ),
        metadata={
            "site_id": "naver_finance",
            "source": "unit-test",
            "generated_at": "2026-08-27T00:00:00+00:00",
        },
    )

    read = read_universe_snapshot(
        path
    )

    assert (
        read.instrument_ids
        == written.instrument_ids
    )

    assert (
        read.metadata[
            "count"
        ]
        == 600
    )

    assert (
        read.metadata[
            "source"
        ]
        == "unit-test"
    )


def test_snapshot_read_rejects_metadata_count_mismatch(
    tmp_path,
):

    path = (
        tmp_path
        / "bad.txt"
    )

    path.write_text(
        "\n".join(
            [
                "# crawl_framework rollout universe snapshot",
                '# metadata: {"count": 2}',
                "XKRX:005930",
                "",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="metadata count",
    ):

        read_universe_snapshot(
            path
        )
