from __future__ import annotations

import argparse
import sys

from pathlib import Path

import pytest

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[2]
)

if str(
    PROJECT_ROOT
) not in sys.path:

    sys.path.insert(
        0,
        str(
            PROJECT_ROOT
        ),
    )

from scripts.query_data import (
    effective_instrument_ids,
    read_instruments_file,
)


def make_args(
    *,
    instrument=None,
    instruments=None,
    instruments_file=None,
):

    return argparse.Namespace(
        instrument=instrument,
        instruments=instruments,
        instruments_file=instruments_file,
    )


def test_read_instruments_file_strips_blanks_and_preserves_duplicates(
    tmp_path,
):

    path = (
        tmp_path
        /
        "universe.txt"
    )

    path.write_text(
        "\n"
        " XKRX:005930 \n"
        "\n"
        "XKRX:000660\n"
        "XKRX:005930\n",
        encoding="utf-8",
    )

    assert read_instruments_file(
        path
    ) == (
        "XKRX:005930",
        "XKRX:000660",
        "XKRX:005930",
    )


def test_effective_instrument_ids_merges_and_deduplicates_in_stable_order(
    tmp_path,
):

    path = (
        tmp_path
        /
        "universe.txt"
    )

    path.write_text(
        "XKRX:042700\n"
        "XKRX:373220\n"
        "XKRX:005930\n",
        encoding="utf-8",
    )

    args = make_args(
        instrument=" XKRX:005930 ",
        instruments=[
            "XKRX:000660",
            "XKRX:042700",
        ],
        instruments_file=str(
            path
        ),
    )

    assert effective_instrument_ids(
        args
    ) == (
        "XKRX:005930",
        "XKRX:000660",
        "XKRX:042700",
        "XKRX:373220",
    )


def test_effective_instrument_ids_returns_none_without_inputs():

    assert effective_instrument_ids(
        make_args()
    ) is None


def test_effective_instrument_ids_rejects_empty_file_effective_set(
    tmp_path,
):

    path = (
        tmp_path
        /
        "empty.txt"
    )

    path.write_text(
        "  \n\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match=(
            "--instruments-file produced an empty "
            "effective instrument set"
        ),
    ):

        effective_instrument_ids(
            make_args(
                instruments_file=str(
                    path
                ),
            )
        )
