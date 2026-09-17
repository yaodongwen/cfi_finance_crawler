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
    "financial_report",
    "financial_report_instrument",
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
    natural_complete_scopes: int = 0
    free_access_complete_scopes: int = 0
    partial_scopes: int = 0
    failed_scopes: int = 0
    missing_scopes: int = 0


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
    natural_complete_scopes: int
    free_access_complete_scopes: int
    partial_scopes: int
    missing_scopes: int
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


def kabutan_month_status_counts(
    *,
    checkpoint_root: Path,
    expected_months: tuple[str, ...],
) -> dict[str, int]:
    root = checkpoint_root / "site=kabutan" / "dataset=news_article" / "scope=month"
    counts = {
        "natural": 0,
        "free_access": 0,
        "partial": 0,
        "failed": 0,
        "missing": 0,
    }
    failure_reasons = {"http_failure", "parser_failure", "interrupted"}
    for month_id in expected_months:
        path = root / f"{month_id.replace('-', '')}.json"
        if not path.is_file():
            counts["missing"] += 1
            continue
        try:
            payload = load_json(path)
        except Exception:
            counts["failed"] += 1
            continue
        state = payload.get("state", {})
        if (
            isinstance(state, dict)
            and state.get("month_complete") is True
            and state.get("archive_access_validated") is True
        ):
            counts["natural"] += 1
        elif (
            isinstance(state, dict)
            and state.get("free_access_complete") is True
            and state.get("stop_reason") == "free_access_boundary"
        ):
            counts["free_access"] += 1
        elif isinstance(state, dict) and state.get("stop_reason") in failure_reasons:
            counts["failed"] += 1
        else:
            counts["partial"] += 1
    return counts


def expected_kabutan_months(manifest: dict[str, Any]) -> tuple[str, ...]:
    scope_plan = manifest.get("scope_plan")
    if isinstance(scope_plan, dict) and scope_plan.get("scope_type") == "month":
        values = scope_plan.get("scope_ids", ())
        if isinstance(values, list):
            return tuple(str(value) for value in values)

    options = manifest.get("options", {})
    if not isinstance(options, dict):
        options = {}
    from datetime import datetime
    from crawl_framework.sites.kabutan.market_news import resolve_month_window

    timestamp = manifest.get("started_at") or manifest.get("finished_at")
    now = datetime.fromisoformat(str(timestamp)) if timestamp else None
    profile = str(manifest.get("profile") or "")
    return resolve_month_window(
        mode=(
            "free_full"
            if profile in {"kabutan_free_full", "kabutan_full"}
            else "incremental"
        ),
        start_month=options.get("kabutan_start_month"),
        end_month=options.get("kabutan_end_month"),
        overlap_months=int(options.get("kabutan_overlap_months", 1) or 0),
        now=now,
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
    natural_complete_total = 0
    free_access_complete_total = 0
    partial_total = 0
    checkpoint_failed_total = 0
    actual_missing_total = 0
    problems: list[str] = []

    for dataset in datasets:
        if site == "kabutan" and dataset == "news_article":
            month_ids = expected_kabutan_months(manifest)
            expected = len(month_ids)
            status_counts = kabutan_month_status_counts(
                checkpoint_root=checkpoint_root,
                expected_months=month_ids,
            )
            natural_complete = status_counts["natural"]
            free_access_complete = status_counts["free_access"]
            partial = status_counts["partial"]
            checkpoint_failed = status_counts["failed"]
            actual_missing = status_counts["missing"]
            completed = natural_complete + free_access_complete
        else:
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
            natural_complete = completed
            free_access_complete = 0
            partial = 0
            checkpoint_failed = 0
            actual_missing = 0

        natural_complete_total += natural_complete
        free_access_complete_total += free_access_complete
        partial_total += partial
        checkpoint_failed_total += checkpoint_failed

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
            if site != "kabutan" or dataset != "news_article":
                actual_missing = missing
            actual_missing_total += actual_missing
            if missing:
                problems.append(
                    f"{dataset} missing checkpoints: {missing}"
                )
            if partial:
                problems.append(f"{dataset} partial scopes: {partial}")
            if checkpoint_failed:
                problems.append(f"{dataset} failed scopes: {checkpoint_failed}")

        dataset_audits.append(
            DatasetAudit(
                dataset=dataset,
                expected_scopes=expected,
                completed_scopes=completed,
                missing_checkpoints=missing,
                files_in_catalog=None,
                natural_complete_scopes=natural_complete,
                free_access_complete_scopes=free_access_complete,
                partial_scopes=partial,
                failed_scopes=checkpoint_failed,
                missing_scopes=actual_missing,
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

    if files_in_catalog <= 0 and (
        runtime_value(manifest, "records_crawled") > 0
        or missing_total > 0
    ):
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
        failed_scopes=checkpoint_failed_total + runtime_errors,
        pending_recovery=pending_recovery,
        terminal_failed_recovery=failed_recovery,
        files_in_catalog=files_in_catalog,
        attachments_pending=attachment_pending,
        natural_complete_scopes=natural_complete_total,
        free_access_complete_scopes=free_access_complete_total,
        partial_scopes=partial_total,
        missing_scopes=actual_missing_total,
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
        print(f"natural_complete_scopes={audit.natural_complete_scopes}")
        print(f"free_access_complete_scopes={audit.free_access_complete_scopes}")
        print(f"partial_scopes={audit.partial_scopes}")
        print(f"failed_scopes={audit.failed_scopes}")
        print(f"missing_scopes={audit.missing_scopes}")
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
