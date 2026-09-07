import importlib.util
import sys

from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "run_naver_forum_full_rollout.py"
)

SPEC = importlib.util.spec_from_file_location(
    "run_naver_forum_full_rollout",
    SCRIPT_PATH,
)
assert SPEC is not None
assert SPEC.loader is not None
ROLLOUT_MODULE = importlib.util.module_from_spec(
    SPEC
)
sys.modules[SPEC.name] = ROLLOUT_MODULE
SPEC.loader.exec_module(
    ROLLOUT_MODULE
)

build_forum_rollout_command = (
    ROLLOUT_MODULE.build_forum_rollout_command
)


def test_forum_rollout_command_defaults_to_full_universe():

    rollout = build_forum_rollout_command(
        universe_path=Path("universe.txt")
    )

    command = list(
        rollout.command
    )

    assert "--dataset" in command
    assert (
        command[
            command.index("--dataset") + 1
        ]
        == "forum_post"
    )
    assert "--instrument-limit" not in command
    assert "--run-manifest" in command
    assert "--coalesce-scope-flushes" in command
    assert "--trust-rsync-success" in command
    assert "--progress-interval-seconds" in command
    assert "--json" not in command


def test_forum_rollout_command_can_be_limited_for_resume_validation():

    rollout = build_forum_rollout_command(
        universe_path=Path("universe.txt"),
        instrument_limit=500,
    )

    command = list(
        rollout.command
    )

    assert (
        command[
            command.index("--instrument-limit") + 1
        ]
        == "500"
    )
    assert (
        command[
            command.index("--upload-workers") + 1
        ]
        == "2"
    )


def test_forum_rollout_command_can_pin_manifest_path():

    rollout = build_forum_rollout_command(
        universe_path=Path("universe.txt"),
        manifest_path=Path("manifest.json"),
    )

    command = list(
        rollout.command
    )

    assert (
        command[
            command.index("--run-manifest-path") + 1
        ]
        == "manifest.json"
    )


def test_forum_rollout_command_streams_progress_by_default():

    rollout = build_forum_rollout_command(
        universe_path=Path("universe.txt")
    )

    command = list(
        rollout.command
    )

    assert "--json" not in command
    assert "--progress-interval-seconds" in command
    assert "--trust-rsync-success" in command


def test_forum_rollout_command_can_emit_json():

    rollout = build_forum_rollout_command(
        universe_path=Path("universe.txt"),
        json_output=True,
    )

    assert "--json" in rollout.command
