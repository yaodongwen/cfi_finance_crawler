import json
import importlib.util
import sys

from pathlib import Path

SCRIPT_PATH = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "check_rollout_completeness.py"
)

SPEC = importlib.util.spec_from_file_location(
    "check_rollout_completeness",
    SCRIPT_PATH,
)
assert SPEC is not None
assert SPEC.loader is not None
AUDIT_MODULE = importlib.util.module_from_spec(
    SPEC
)
sys.modules[SPEC.name] = AUDIT_MODULE
SPEC.loader.exec_module(
    AUDIT_MODULE
)

audit_manifest = AUDIT_MODULE.audit_manifest
main = AUDIT_MODULE.main


def write_json(
    path: Path,
    payload,
) -> None:

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )


def checkpoint(
    root: Path,
    *,
    site: str,
    dataset: str,
    scope: str,
) -> None:

    write_json(
        root
        / f"site={site}"
        / f"dataset={dataset}"
        / "scope=instrument"
        / f"{scope}.json",
        {
            "state": {
                "source_key": scope,
            },
        },
    )


def kabutan_checkpoint(
    root: Path,
    month: str,
    *,
    complete: bool,
    reason: str,
    access_validated: bool = True,
    free_access_complete: bool = False,
) -> None:
    write_json(
        root
        / "site=kabutan"
        / "dataset=news_article"
        / "scope=month"
        / f"{month.replace('-', '')}.json",
        {
            "state": {
                "year": int(month[:4]),
                "month": int(month[5:]),
                "last_completed_page": 3,
                "last_news_id": "n202609100001",
                "month_complete": complete,
                "free_access_complete": free_access_complete,
                "stop_reason": reason,
                "archive_access_validated": access_validated,
            }
        },
    )


def manifest_payload(
    universe: Path,
):

    return {
        "run_id": "run-1",
        "site": "naver_finance",
        "profile": "naver_full",
        "datasets": [
            "forum_post",
            "news_article",
            "research_report",
            "attachment",
        ],
        "universe": {
            "path": str(
                universe
            ),
            "count": 2,
        },
        "options": {
            "research_categories": [
                "market",
            ],
        },
        "runtime": {
            "files_registered": 3,
            "errors": 0,
        },
    }


def test_audit_manifest_reports_complete_run(
    tmp_path,
):

    universe = tmp_path / "universe.txt"
    universe.write_text(
        "XKRX:005930\nXKRX:000660\n",
        encoding="utf-8",
    )

    manifest = tmp_path / "manifest.json"
    write_json(
        manifest,
        manifest_payload(
            universe
        ),
    )

    checkpoints = tmp_path / "checkpoints"
    checkpoint(
        checkpoints,
        site="naver_finance",
        dataset="forum_post",
        scope="XKRX:005930",
    )
    checkpoint(
        checkpoints,
        site="naver_finance",
        dataset="forum_post",
        scope="XKRX:000660",
    )
    checkpoint(
        checkpoints,
        site="naver_finance",
        dataset="news_article",
        scope="XKRX:005930",
    )
    checkpoint(
        checkpoints,
        site="naver_finance",
        dataset="news_article",
        scope="XKRX:000660",
    )
    write_json(
        checkpoints
        / "site=naver_finance"
        / "dataset=research_report"
        / "scope=research_category"
        / "market.json",
        {
            "state": {
                "category": "market",
            },
        },
    )

    audit = audit_manifest(
        manifest_path=manifest,
        checkpoint_root=checkpoints,
        recovery_root=tmp_path / "recovery",
    )

    assert audit.complete is True
    assert audit.expected_scopes == 5
    assert audit.completed_scopes == 5
    assert audit.missing_checkpoints == 0
    assert audit.pending_recovery == 0


def test_kabutan_audit_requires_complete_expected_month_checkpoints(tmp_path):
    manifest = tmp_path / "kabutan.json"
    write_json(manifest, {
        "run_id": "kabutan-run",
        "site": "kabutan",
        "profile": "kabutan_full",
        "datasets": ["news_article"],
        "universe": {"count": 0},
        "scope_plan": {
            "scope_type": "month",
            "scope_ids": ["2026-07", "2026-08", "2026-09"],
            "count": 3,
        },
        "options": {},
        "runtime": {"files_registered": 2, "errors": 0},
    })
    checkpoints = tmp_path / "checkpoints"
    kabutan_checkpoint(checkpoints, "2026-07", complete=True, reason="empty_page")
    kabutan_checkpoint(
        checkpoints, "2026-08", complete=False, reason="safety_capped"
    )
    kabutan_checkpoint(
        checkpoints, "2026-09", complete=True, reason="repeated_page_signature"
    )

    audit = audit_manifest(
        manifest_path=manifest,
        checkpoint_root=checkpoints,
        recovery_root=tmp_path / "recovery",
    )

    assert audit.expected_scopes == 3
    assert audit.completed_scopes == 2
    assert audit.missing_checkpoints == 1
    assert audit.natural_complete_scopes == 2
    assert audit.free_access_complete_scopes == 0
    assert audit.partial_scopes == 1
    assert audit.missing_scopes == 0
    assert audit.complete is False
    assert "news_article missing checkpoints: 1" in audit.problems


def test_kabutan_audit_rejects_legacy_complete_without_archive_validation(tmp_path):
    manifest = tmp_path / "kabutan-legacy.json"
    write_json(manifest, {
        "run_id": "kabutan-legacy",
        "site": "kabutan",
        "profile": "kabutan_full",
        "datasets": ["news_article"],
        "scope_plan": {
            "scope_type": "month",
            "scope_ids": ["2026-07"],
            "count": 1,
        },
        "options": {},
        "runtime": {"files_registered": 0, "errors": 0},
    })
    checkpoints = tmp_path / "checkpoints"
    kabutan_checkpoint(
        checkpoints,
        "2026-07",
        complete=True,
        reason="empty_page",
        access_validated=False,
    )

    audit = audit_manifest(
        manifest_path=manifest,
        checkpoint_root=checkpoints,
        recovery_root=tmp_path / "recovery",
    )

    assert audit.complete is False
    assert audit.completed_scopes == 0
    assert audit.missing_checkpoints == 1
    assert audit.partial_scopes == 1
    assert audit.missing_scopes == 0


def test_kabutan_free_access_boundary_is_complete_not_missing(tmp_path):
    manifest = tmp_path / "kabutan-free.json"
    write_json(manifest, {
        "run_id": "kabutan-free",
        "site": "kabutan",
        "profile": "kabutan_free_full",
        "datasets": ["news_article"],
        "scope_plan": {
            "scope_type": "month",
            "scope_ids": ["2026-09", "2026-08"],
            "count": 2,
        },
        "options": {},
        "runtime": {"files_registered": 1, "errors": 0},
    })
    checkpoints = tmp_path / "checkpoints"
    kabutan_checkpoint(checkpoints, "2026-09", complete=True, reason="empty_page")
    kabutan_checkpoint(
        checkpoints,
        "2026-08",
        complete=False,
        reason="free_access_boundary",
        access_validated=False,
        free_access_complete=True,
    )

    audit = audit_manifest(
        manifest_path=manifest,
        checkpoint_root=checkpoints,
        recovery_root=tmp_path / "recovery",
    )

    assert audit.complete is True
    assert audit.completed_scopes == 2
    assert audit.natural_complete_scopes == 1
    assert audit.free_access_complete_scopes == 1
    assert audit.partial_scopes == 0
    assert audit.failed_scopes == 0
    assert audit.missing_scopes == 0
    assert audit.missing_checkpoints == 0


def test_complete_zero_record_resume_does_not_require_new_catalog_jobs(tmp_path):
    manifest = tmp_path / "kabutan-resume.json"
    write_json(manifest, {
        "run_id": "kabutan-resume",
        "site": "kabutan",
        "profile": "kabutan_full",
        "datasets": ["news_article"],
        "scope_plan": {
            "scope_type": "month",
            "scope_ids": ["2026-08"],
            "count": 1,
        },
        "options": {},
        "runtime": {
            "production_stats": {
                "records_crawled": 0,
                "catalog_jobs_completed": 0,
                "errors": 0,
            }
        },
    })
    checkpoints = tmp_path / "checkpoints"
    kabutan_checkpoint(checkpoints, "2026-08", complete=True, reason="empty_page")

    audit = audit_manifest(
        manifest_path=manifest,
        checkpoint_root=checkpoints,
        recovery_root=tmp_path / "recovery",
    )

    assert audit.complete is True
    assert audit.files_in_catalog == 0


def test_audit_manifest_uses_instrument_limit_and_production_stats(
    tmp_path,
):

    universe = tmp_path / "universe.txt"
    universe.write_text(
        "XKRX:005930\nXKRX:000660\nXKRX:005935\n",
        encoding="utf-8",
    )

    manifest = tmp_path / "manifest.json"
    payload = manifest_payload(
        universe
    )
    payload["options"]["instrument_limit"] = 2
    payload["options"]["instruments"] = [
        "XKRX:005930",
        "XKRX:000660",
    ]
    payload["runtime"] = {
        "production_stats": {
            "catalog_jobs_completed": 5,
            "errors": 0,
        }
    }
    write_json(
        manifest,
        payload,
    )

    checkpoints = tmp_path / "checkpoints"

    for dataset in (
        "forum_post",
        "news_article",
    ):
        checkpoint(
            checkpoints,
            site="naver_finance",
            dataset=dataset,
            scope="XKRX:005930",
        )
        checkpoint(
            checkpoints,
            site="naver_finance",
            dataset=dataset,
            scope="XKRX:000660",
        )

    write_json(
        checkpoints
        / "site=naver_finance"
        / "dataset=research_report"
        / "scope=global"
        / "research.json",
        {
            "state": {
                "category": "market",
            },
        },
    )

    audit = audit_manifest(
        manifest_path=manifest,
        checkpoint_root=checkpoints,
        recovery_root=tmp_path / "recovery",
    )

    assert audit.expected_scopes == 5
    assert audit.files_in_catalog == 5
    assert audit.missing_checkpoints == 0


def test_hkex_financial_report_datasets_require_every_instrument_checkpoint(
    tmp_path,
):
    universe = tmp_path / "hkex.txt"
    universe.write_text(
        "XHKG:00001\nXHKG:00002\n",
        encoding="utf-8",
    )
    manifest = tmp_path / "hkex-manifest.json"
    write_json(
        manifest,
        {
            "run_id": "hkex-run",
            "site": "hkexnews",
            "profile": "hkex_reports_full",
            "datasets": [
                "financial_report",
                "financial_report_instrument",
            ],
            "universe": {
                "path": str(universe),
                "count": 2,
            },
            "options": {
                "instrument_limit": 2,
                "instruments": [
                    "XHKG:00001",
                    "XHKG:00002",
                ],
            },
            "runtime": {
                "production_stats": {
                    "catalog_jobs_completed": 1,
                    "errors": 0,
                },
            },
        },
    )
    checkpoints = tmp_path / "checkpoints"
    for dataset in (
        "financial_report",
        "financial_report_instrument",
    ):
        checkpoint(
            checkpoints,
            site="hkexnews",
            dataset=dataset,
            scope="XHKG:00001",
        )
        checkpoint(
            checkpoints,
            site="hkexnews",
            dataset=dataset,
            scope="XHKG:00002",
        )

    audit = audit_manifest(
        manifest_path=manifest,
        checkpoint_root=checkpoints,
        recovery_root=tmp_path / "recovery",
    )

    assert audit.complete is True
    assert audit.expected_scopes == 4
    assert audit.completed_scopes == 4
    assert audit.missing_checkpoints == 0
    assert [item.expected_scopes for item in audit.dataset_audits] == [2, 2]

    (
        checkpoints
        / "site=hkexnews"
        / "dataset=financial_report_instrument"
        / "scope=instrument"
        / "XHKG:00002.json"
    ).unlink()

    incomplete = audit_manifest(
        manifest_path=manifest,
        checkpoint_root=checkpoints,
        recovery_root=tmp_path / "recovery",
    )

    assert incomplete.complete is False
    assert incomplete.expected_scopes == 4
    assert incomplete.completed_scopes == 3
    assert incomplete.missing_checkpoints == 1
    assert incomplete.problems == (
        "financial_report_instrument missing checkpoints: 1",
    )


def test_audit_manifest_reports_missing_checkpoint_and_recovery(
    tmp_path,
):

    universe = tmp_path / "universe.txt"
    universe.write_text(
        "XKRX:005930\nXKRX:000660\n",
        encoding="utf-8",
    )

    manifest = tmp_path / "manifest.json"
    write_json(
        manifest,
        manifest_payload(
            universe
        ),
    )

    checkpoints = tmp_path / "checkpoints"
    checkpoint(
        checkpoints,
        site="naver_finance",
        dataset="forum_post",
        scope="XKRX:005930",
    )

    recovery = tmp_path / "recovery"
    write_json(
        recovery / "pending.json",
        {
            "stage": "uploaded",
            "dataset": "attachment",
        },
    )

    audit = audit_manifest(
        manifest_path=manifest,
        checkpoint_root=checkpoints,
        recovery_root=recovery,
    )

    assert audit.complete is False
    assert audit.missing_checkpoints == 4
    assert audit.pending_recovery == 1
    assert audit.attachments_pending == 1
    assert (
        "pending recovery manifests: 1"
        in audit.problems
    )


def test_cli_returns_nonzero_for_incomplete_run(
    tmp_path,
    capsys,
):

    universe = tmp_path / "universe.txt"
    universe.write_text(
        "XKRX:005930\n",
        encoding="utf-8",
    )

    manifest = tmp_path / "manifest.json"
    payload = manifest_payload(
        universe
    )
    payload["runtime"]["files_registered"] = 0
    write_json(
        manifest,
        payload,
    )

    exit_code = main(
        [
            "--manifest",
            str(
                manifest
            ),
            "--checkpoint-root",
            str(
                tmp_path / "checkpoints"
            ),
            "--recovery-root",
            str(
                tmp_path / "recovery"
            ),
            "--json",
        ]
    )

    assert exit_code == 1
    output = json.loads(
        capsys.readouterr().out
    )
    assert output["complete"] is False
    assert (
        "manifest/runtime reports no catalog-registered files"
        in output["problems"]
    )
