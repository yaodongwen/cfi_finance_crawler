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
