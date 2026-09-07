from __future__ import annotations

import argparse
import json
import subprocess
import sys

from dataclasses import (
    asdict,
    dataclass,
)
from pathlib import Path
from typing import (
    Sequence,
)


SMOKE_DATASETS = (
    "forum_post",
    "news_article",
    "news_instrument",
    "research_report",
    "research_instrument",
    "attachment",
)

DEFAULT_UNIVERSE = Path(
    "config/universes/naver_finance_kr_rollout_universe.txt"
)


@dataclass(
    frozen=True,
    slots=True,
)
class SmokeCommand:
    name: str
    command: tuple[str, ...]


def build_smoke_command(
    *,
    name: str,
    instrument_limit: int,
    research_categories: Sequence[str],
    attachment_limit: int,
    forum_max_pages: int = 1,
    news_max_pages: int = 1,
    research_max_pages: int = 1,
    universe_path: Path = DEFAULT_UNIVERSE,
) -> SmokeCommand:

    command: list[str] = [
        sys.executable,
        "-m",
        "crawl_framework.cli.main",
        "crawl",
        "--site",
        "naver_finance",
        "--profile",
        "naver_incremental",
        "--instruments-file",
        str(universe_path),
        "--instrument-limit",
        str(instrument_limit),
        "--forum-max-pages",
        str(forum_max_pages),
        "--news-max-pages",
        str(news_max_pages),
        "--research-max-pages",
        str(research_max_pages),
        "--attachment-limit",
        str(attachment_limit),
        "--crawl-workers",
        "4",
        "--writer-workers",
        "2",
        "--upload-workers",
        "4",
        "--catalog-workers",
        "2",
        "--attachment-workers",
        "1",
        "--json",
    ]

    for dataset in SMOKE_DATASETS:
        command.extend(
            [
                "--dataset",
                dataset,
            ]
        )

    for category in research_categories:
        command.extend(
            [
                "--research-category",
                category,
            ]
        )

    return SmokeCommand(
        name=name,
        command=tuple(command),
    )


def smoke_matrix(
    *,
    universe_path: Path = DEFAULT_UNIVERSE,
) -> tuple[SmokeCommand, ...]:

    return (
        build_smoke_command(
            name="smoke_1",
            instrument_limit=1,
            research_categories=("market",),
            attachment_limit=1,
            universe_path=universe_path,
        ),
        build_smoke_command(
            name="smoke_3",
            instrument_limit=3,
            research_categories=("all",),
            attachment_limit=1,
            universe_path=universe_path,
        ),
        build_smoke_command(
            name="smoke_50",
            instrument_limit=50,
            research_categories=("all",),
            attachment_limit=3,
            universe_path=universe_path,
        ),
    )


def run_command(
    smoke: SmokeCommand,
) -> dict:

    completed = subprocess.run(
        smoke.command,
        check=False,
        capture_output=True,
        text=True,
    )

    return {
        "name": smoke.name,
        "returncode": completed.returncode,
        "command": list(smoke.command),
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "success": completed.returncode == 0,
    }


def build_parser() -> argparse.ArgumentParser:

    parser = argparse.ArgumentParser(
        description="Run deterministic Naver production smoke matrix.",
    )
    parser.add_argument(
        "--universe",
        default=str(DEFAULT_UNIVERSE),
    )
    parser.add_argument(
        "--only",
        choices=(
            "smoke_1",
            "smoke_3",
            "smoke_50",
        ),
        default=None,
    )
    parser.add_argument(
        "--print-commands",
        action="store_true",
    )
    parser.add_argument(
        "--json",
        action="store_true",
    )
    return parser


def main(
    argv: Sequence[str] | None = None,
) -> int:

    args = build_parser().parse_args(
        argv
    )

    commands = smoke_matrix(
        universe_path=Path(args.universe),
    )

    if args.only:
        commands = tuple(
            command
            for command in commands
            if command.name == args.only
        )

    if args.print_commands:
        payload = [
            asdict(command)
            for command in commands
        ]
        print(
            json.dumps(
                payload,
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    results = [
        run_command(command)
        for command in commands
    ]

    if args.json:
        print(
            json.dumps(
                results,
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        for result in results:
            print(
                f"{result['name']} success={result['success']} "
                f"returncode={result['returncode']}"
            )

    return (
        0
        if all(
            result["success"]
            for result in results
        )
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(
        main(
            sys.argv[1:]
        )
    )
