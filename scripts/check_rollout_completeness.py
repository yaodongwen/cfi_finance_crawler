from __future__ import annotations

import argparse
import json
import sys

from dataclasses import (
    asdict,
    dataclass,
)
from pathlib import Path
from typing import (
    Any,
    Sequence,
)


INSTRUMENT_SCOPED_DATASETS = {
    "forum_post",
    "news_article",
    "news_instrument",
}

RESEARCH_DATASETS = {
    "research_report",
    "research_instrument",
}

ALL_RESEARCH_CATEGORIES = (
    "market",
    "invest",
    "company",
    "industry",
    "economy",
    "debenture",
)


@dataclass(
    frozen=True,
    slots=True,
)
class DatasetAudit:
    dataset: str
    expected_scopes: int | None
    completed_scopes: int
    missing_checkpoints: int | None
    files_in_catalog: int | None
    attachments_pending: int | None = None


@dataclass(
    frozen=True,
    slots=True,
)
class CompletenessAudit:
    site: str
    profile: str | None
    universe_count: int
    datasets: tuple[str, ...]
    expected_scopes: int
    completed_scopes: int
    missing_checkpoints: int
    failed_scopes: int
    pending_recovery: int
    terminal_failed_recovery: int
    files_in_catalog: int
    attachments_pending: int
    complete: bool
    dataset_audits: tuple[DatasetAudit, ...]
    problems: tuple[str, ...]


def load_json(
    path: Path,
) -> dict[str, Any]:

    obj = json.loads(
        path.read_text(
            encoding="utf-8",
        )
    )

    if not isinstance(
        obj,
        dict,
    ):
        raise ValueError(
            f"JSON root must be object: {path}"
        )

    return obj


def read_universe(
    path: Path | None,
) -> tuple[str, ...]:

    if path is None:
        return ()

    instruments: list[str] = []

    for line in path.read_text(
        encoding="utf-8",
    ).splitlines():
        value = line.strip()
        if (
            not value
            or value.startswith("#")
        ):
            continue
        instruments.append(value)

    return tuple(instruments)


def effective_research_categories(
    manifest: dict[str, Any],
) -> tuple[str, ...]:

    options = manifest.get(
        "options",
        {},
    )

    if not isinstance(
        options,
        dict,
    ):
        return ALL_RESEARCH_CATEGORIES

    raw = options.get(
        "research_categories",
    )

    if not raw:
        return ALL_RESEARCH_CATEGORIES

    values = tuple(
        str(item).strip().lower()
        for item in raw
        if str(item).strip()
    )

    if (
        not values
        or "all" in values
    ):
        return ALL_RESEARCH_CATEGORIES

    return values


def checkpoint_count(
    *,
    checkpoint_root: Path,
    site: str,
    dataset: str,
) -> int:

    root = (
        checkpoint_root
        / f"site={site}"
        / f"dataset={dataset}"
    )

    if not root.exists():
        return 0

    return sum(
        1
        for path in root.rglob("*.json")
        if path.is_file()
    )


def recovery_counts(
    recovery_root: Path,
) -> tuple[int, int, int]:

    if not recovery_root.exists():
        return 0, 0, 0

    pending = 0
    failed = 0
    attachment_pending = 0

    for path in recovery_root.rglob("*.json"):
        try:
            obj = load_json(path)
        except Exception:
            failed += 1
            continue

        stage = str(
            obj.get("stage", "")
        )
        dataset = str(
            obj.get("dataset", "")
        )

        if stage == "failed":
            failed += 1
        elif stage not in {
            "cleanable",
            "deleted",
        }:
            pending += 1
            if dataset == "attachment":
                attachment_pending += 1

    return pending, failed, attachment_pending


def runtime_value(
    manifest: dict[str, Any],
    key: str,
) -> int:

    runtime = manifest.get(
        "runtime",
        {},
    )

    if isinstance(
        runtime,
        dict,
    ):
        production_stats = runtime.get(
            "production_stats",
            {},
        )

        value = None

        if isinstance(
            production_stats,
            dict,
        ):
            value = production_stats.get(
                key,
            )

        if value is None:
            value = runtime.get(
                key,
                0,
            )
        try:
            return int(value or 0)
        except (
            TypeError,
            ValueError,
        ):
            return 0

    return 0


def expected_for_dataset(
    *,
    dataset: str,
    effective_instrument_count: int,
) -> int | None:

    if dataset in INSTRUMENT_SCOPED_DATASETS:
        return effective_instrument_count

    if dataset in RESEARCH_DATASETS:
        return 1

    return None


def audit_manifest(
    *,
    manifest_path: Path,
    universe_path: Path | None = None,
    checkpoint_root: Path = Path("state/checkpoints"),
    recovery_root: Path = Path("state/recovery"),
) -> CompletenessAudit:

    manifest = load_json(
        manifest_path
    )

    site = str(
        manifest.get(
            "site",
            "",
        )
    ).strip()

    if not site:
        raise ValueError(
            "manifest does not contain site"
        )

    profile = manifest.get(
        "profile",
    )
    if profile is not None:
        profile = str(profile)

    datasets = tuple(
        str(item)
        for item in manifest.get(
            "datasets",
            (),
        )
    )

    universe_meta = manifest.get(
        "universe",
    )

    if (
        universe_path is None
        and isinstance(
            universe_meta,
            dict,
        )
        and universe_meta.get("path")
    ):
        universe_path = Path(
            str(
                universe_meta["path"]
            )
        )

    universe = read_universe(
        universe_path
    )
    universe_count = len(
        universe
    )

    if (
        universe_count == 0
        and isinstance(
            universe_meta,
            dict,
        )
    ):
        try:
            universe_count = int(
                universe_meta.get(
                    "count",
                    0,
                )
                or 0
            )
        except (
            TypeError,
            ValueError,
        ):
            universe_count = 0

    options = manifest.get(
        "options",
        {},
    )

    if not isinstance(
        options,
        dict,
    ):
        options = {}

    effective_instrument_count = universe_count

    explicit_instruments = options.get(
        "instruments",
    )

    if isinstance(
        explicit_instruments,
        list,
    ):
        effective_instrument_count = len(
            explicit_instruments
        )

    instrument_limit = options.get(
        "instrument_limit",
    )

    if instrument_limit is not None:
        try:
            effective_instrument_count = min(
                effective_instrument_count,
                int(
                    instrument_limit
                ),
            )
        except (
            TypeError,
            ValueError,
        ):
            pass

    dataset_audits: list[DatasetAudit] = []
    expected_total = 0
    completed_total = 0
    missing_total = 0
    problems: list[str] = []

    for dataset in datasets:
        expected = expected_for_dataset(
            dataset=dataset,
            effective_instrument_count=(
                effective_instrument_count
            ),
        )
        completed = checkpoint_count(
            checkpoint_root=checkpoint_root,
            site=site,
            dataset=dataset,
        )

        if expected is None:
            missing = None
        else:
            expected_total += expected
            completed_total += min(
                completed,
                expected,
            )
            missing = max(
                expected - completed,
                0,
            )
            missing_total += missing
            if missing:
                problems.append(
                    f"{dataset} missing checkpoints: {missing}"
                )

        dataset_audits.append(
            DatasetAudit(
                dataset=dataset,
                expected_scopes=expected,
                completed_scopes=completed,
                missing_checkpoints=missing,
                files_in_catalog=None,
            )
        )

    pending_recovery, failed_recovery, attachment_pending = recovery_counts(
        recovery_root
    )

    if pending_recovery:
        problems.append(
            f"pending recovery manifests: {pending_recovery}"
        )

    if failed_recovery:
        problems.append(
            f"terminal failed recovery manifests: {failed_recovery}"
        )

    runtime_errors = runtime_value(
        manifest,
        "errors",
    )

    if runtime_errors:
        problems.append(
            f"runtime errors: {runtime_errors}"
        )

    files_in_catalog = runtime_value(
        manifest,
        "files_registered",
    )

    if files_in_catalog <= 0:
        files_in_catalog = runtime_value(
            manifest,
            "catalog_jobs_completed",
        )

    if files_in_catalog <= 0:
        problems.append(
            "manifest/runtime reports no catalog-registered files"
        )

    complete = not problems

    return CompletenessAudit(
        site=site,
        profile=profile,
        universe_count=universe_count,
        datasets=datasets,
        expected_scopes=expected_total,
        completed_scopes=completed_total,
        missing_checkpoints=missing_total,
        failed_scopes=runtime_errors,
        pending_recovery=pending_recovery,
        terminal_failed_recovery=failed_recovery,
        files_in_catalog=files_in_catalog,
        attachments_pending=attachment_pending,
        complete=complete,
        dataset_audits=tuple(dataset_audits),
        problems=tuple(problems),
    )


def build_parser() -> argparse.ArgumentParser:

    parser = argparse.ArgumentParser(
        description="Audit rollout completeness for a recorded run manifest.",
    )

    parser.add_argument(
        "--manifest",
        required=True,
        help="Run manifest JSON path.",
    )
    parser.add_argument(
        "--universe",
        default=None,
        help="Universe snapshot override.",
    )
    parser.add_argument(
        "--checkpoint-root",
        default="state/checkpoints",
        help="Checkpoint root directory.",
    )
    parser.add_argument(
        "--recovery-root",
        default="state/recovery",
        help="Recovery manifest root directory.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON output.",
    )

    return parser


def main(
    argv: Sequence[str] | None = None,
) -> int:

    args = build_parser().parse_args(
        argv
    )

    audit = audit_manifest(
        manifest_path=Path(args.manifest),
        universe_path=(
            Path(args.universe)
            if args.universe
            else None
        ),
        checkpoint_root=Path(args.checkpoint_root),
        recovery_root=Path(args.recovery_root),
    )

    payload = asdict(
        audit
    )

    if args.json:
        print(
            json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
            )
        )
    else:
        print(
            f"complete={audit.complete}"
        )
        print(
            f"expected_scopes={audit.expected_scopes}"
        )
        print(
            f"completed_scopes={audit.completed_scopes}"
        )
        print(
            f"missing_checkpoints={audit.missing_checkpoints}"
        )
        print(
            f"pending_recovery={audit.pending_recovery}"
        )
        print(
            f"terminal_failed_recovery={audit.terminal_failed_recovery}"
        )
        print(
            f"files_in_catalog={audit.files_in_catalog}"
        )
        for problem in audit.problems:
            print(
                f"problem={problem}"
            )

    return 0 if audit.complete else 1


if __name__ == "__main__":
    raise SystemExit(
        main(
            sys.argv[1:]
        )
    )
