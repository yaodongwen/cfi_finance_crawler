import importlib.util
import sys

from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "run_naver_smoke_matrix.py"
)

SPEC = importlib.util.spec_from_file_location(
    "run_naver_smoke_matrix",
    SCRIPT_PATH,
)
assert SPEC is not None
assert SPEC.loader is not None
SMOKE_MODULE = importlib.util.module_from_spec(
    SPEC
)
sys.modules[SPEC.name] = SMOKE_MODULE
SPEC.loader.exec_module(
    SMOKE_MODULE
)

smoke_matrix = SMOKE_MODULE.smoke_matrix
SMOKE_DATASETS = SMOKE_MODULE.SMOKE_DATASETS


def test_smoke_matrix_uses_nested_universe_limits():

    matrix = smoke_matrix(
        universe_path=Path("universe.txt")
    )

    limits = []

    for smoke in matrix:
        command = list(
            smoke.command
        )
        index = command.index(
            "--instrument-limit"
        )
        limits.append(
            int(
                command[index + 1]
            )
        )

    assert limits == [
        1,
        3,
        50,
    ]


def test_smoke_matrix_uses_production_profile_and_all_datasets():

    smoke = smoke_matrix(
        universe_path=Path("universe.txt")
    )[0]
    command = list(
        smoke.command
    )

    assert "--profile" in command
    assert (
        command[
            command.index("--profile") + 1
        ]
        == "naver_incremental"
    )

    datasets = [
        command[index + 1]
        for index, value in enumerate(command)
        if value == "--dataset"
    ]

    assert datasets == list(
        SMOKE_DATASETS
    )


def test_smoke_matrix_keeps_pdf_work_bounded():

    smoke_50 = smoke_matrix(
        universe_path=Path("universe.txt")
    )[2]
    command = list(
        smoke_50.command
    )

    assert (
        command[
            command.index("--attachment-limit") + 1
        ]
        == "3"
    )
    assert (
        command[
            command.index("--attachment-workers") + 1
        ]
        == "1"
    )


def test_smoke_matrix_uses_parallel_production_workers():

    smoke = smoke_matrix(
        universe_path=Path("universe.txt")
    )[2]
    command = list(
        smoke.command
    )

    assert (
        command[
            command.index("--crawl-workers") + 1
        ]
        == "4"
    )
    assert (
        command[
            command.index("--writer-workers") + 1
        ]
        == "2"
    )
    assert (
        command[
            command.index("--upload-workers") + 1
        ]
        == "4"
    )
    assert (
        command[
            command.index("--catalog-workers") + 1
        ]
        == "2"
    )
