from __future__ import annotations

import argparse
import json
import subprocess
import sys

from dataclasses import (
    asdict,
    dataclass,
)
from datetime import (
    datetime,
    timezone,
)
from pathlib import Path
from typing import (
    Sequence,
)


DEFAULT_UNIVERSE = Path(
    "config/universes/naver_finance_kr_rollout_universe.txt"
)


@dataclass(
    frozen=True,
    slots=True,
)
class ForumRolloutCommand:
    command: tuple[str, ...]


def build_forum_rollout_command(
    *,
    universe_path: Path = DEFAULT_UNIVERSE,
    instrument_limit: int | None = None,
    forum_max_pages: int = 1,
    crawl_workers: int = 16,
    writer_workers: int = 4,
    upload_workers: int = 2,
    catalog_workers: int = 4,
    target_file_size_mb: int = 16,
    progress_interval_seconds: int = 60,
    manifest_path: Path | None = None,
    json_output: bool = False,
    trust_rsync_success: bool = True,
) -> ForumRolloutCommand:

    command: list[str] = [
        sys.executable,
        "-m",
        "crawl_framework.cli.main",
        "crawl",
        "--site",
        "naver_finance",
        "--dataset",
        "forum_post",
        "--instruments-file",
        str(universe_path),
        "--forum-max-pages",
        str(forum_max_pages),
        "--crawl-workers",
        str(crawl_workers),
        "--writer-workers",
        str(writer_workers),
        "--upload-workers",
        str(upload_workers),
        "--catalog-workers",
        str(catalog_workers),
        "--target-file-size-mb",
        str(target_file_size_mb),
        "--run-manifest",
        "--coalesce-scope-flushes",
    ]

    if manifest_path is not None:
        command.extend(
            [
                "--run-manifest-path",
                str(manifest_path),
            ]
        )

    if progress_interval_seconds > 0:
        command.extend(
            [
                "--progress-interval-seconds",
                str(progress_interval_seconds),
            ]
        )

    if trust_rsync_success:
        command.append(
            "--trust-rsync-success"
        )

    if json_output:
        command.append(
            "--json"
        )

    if instrument_limit is not None:
        command.extend(
            [
                "--instrument-limit",
                str(
                    instrument_limit
                ),
            ]
        )

    return ForumRolloutCommand(
        command=tuple(
            command
        )
    )


def build_parser() -> argparse.ArgumentParser:

    parser = argparse.ArgumentParser(
        description="Run Naver forum_post full-market rollout.",
    )
    parser.add_argument(
        "--universe",
        default=str(DEFAULT_UNIVERSE),
    )
    parser.add_argument(
        "--instrument-limit",
        type=int,
        default=None,
    )
    parser.add_argument(
        "--print-command",
        action="store_true",
    )
    parser.add_argument(
        "--manifest-path",
        default=None,
    )
    parser.add_argument(
        "--json",
        action="store_true",
    )
    parser.add_argument(
        "--capture-output",
        action="store_true",
    )
    return parser


def main(
    argv: Sequence[str] | None = None,
) -> int:

    args = build_parser().parse_args(
        argv
    )

    rollout = build_forum_rollout_command(
        universe_path=Path(
            args.universe
        ),
        instrument_limit=(
            args.instrument_limit
        ),
        manifest_path=(
            Path(args.manifest_path)
            if args.manifest_path
            else _default_manifest_path()
        ),
        json_output=(
            args.json
        ),
    )

    if args.print_command:
        print(
            json.dumps(
                asdict(
                    rollout
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    manifest_path = _manifest_path_from_command(
        rollout.command
    )

    if manifest_path is not None:
        print(
            f"manifest={manifest_path}",
            flush=True,
        )

    completed = subprocess.run(
        rollout.command,
        check=False,
        capture_output=(
            args.capture_output
        ),
        text=True,
    )

    result = {
        "returncode": completed.returncode,
        "command": list(
            rollout.command
        ),
        "success": completed.returncode == 0,
    }

    if args.capture_output:
        result["stdout"] = completed.stdout
        result["stderr"] = completed.stderr

    if args.json:
        print(
            json.dumps(
                result,
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        print(
            f"success={result['success']} "
            f"returncode={result['returncode']}"
        )

    return 0 if result["success"] else 1


def _default_manifest_path() -> Path:

    stamp = (
        datetime.now(timezone.utc)
        .strftime("%Y%m%dT%H%M%SZ")
    )

    return Path(
        "state/run_manifests"
    ) / f"naver_forum_full_rollout_{stamp}.json"


def _manifest_path_from_command(
    command: Sequence[str],
) -> str | None:

    try:
        index = command.index(
            "--run-manifest-path"
        )
    except ValueError:
        return None

    try:
        return command[
            index + 1
        ]
    except IndexError:
        return None


if __name__ == "__main__":
    raise SystemExit(
        main(
            sys.argv[1:]
        )
    )
