from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from crawl_framework.cli.main import (
    PlatformCLIOptions,
    build_platform_run_manifest,
    write_platform_run_manifest,
)
from crawl_framework.core.bootstrap import BootstrapResult
from crawl_framework.core.platform import (
    PlatformRunResult,
    PlatformSiteResult,
    PlatformSiteStatus,
)
from crawl_framework.storage.recovery_orchestrator import StartupRecoveryResult


def recovery(*, failed=0, pending=0, can_continue=True):
    return StartupRecoveryResult(
        scanned=2,
        attempted=1,
        recovered=1,
        skipped=0,
        failed=failed,
        terminal_failed=failed,
        remaining_pending=pending,
        can_continue=can_continue,
        results=(),
    )


def site_result(
    site_id,
    profile,
    status,
    *,
    reason=None,
    access_state=None,
    failed_recovery=0,
):
    result = None
    if status is not PlatformSiteStatus.SKIPPED:
        result = BootstrapResult(
            recovery=recovery(
                failed=failed_recovery,
                pending=failed_recovery,
                can_continue=failed_recovery == 0,
            ),
            runtime={
                "resume_plans": {
                    "forum_post": {
                        "total_scopes": 5,
                        "durable_complete_scopes": 2,
                        "recovery_pending_scopes": 0,
                        "incomplete_scopes": 3,
                        "blocked_scopes": 0,
                        "failed_retryable_scopes": 0,
                        "failed_terminal_scopes": 0,
                        "items": [],
                    }
                },
                "production_stats": {
                    "records_crawled": 11,
                    "files_written": 2,
                    "uploads_started": 2,
                    "uploads_completed": 2,
                    "catalog_jobs_started": 2,
                    "catalog_jobs_completed": 2,
                },
                "progress": {
                    "total_scopes": 5,
                    "completed_scopes": 4,
                    "remaining_scopes": 1,
                    "sites": [
                        {
                            "datasets": [
                                {
                                    "skipped_scopes": 2,
                                    "records": {
                                        "crawled": 11,
                                        "normalized": 10,
                                        "new": 7,
                                        "unchanged": 2,
                                        "updated": 1,
                                        "failed": 1,
                                    },
                                    "storage": {
                                        "files_written": 2,
                                        "files_uploaded": 2,
                                        "files_verified": 2,
                                        "files_cataloged": 2,
                                    },
                                }
                            ]
                        }
                    ],
                },
                "pipeline_errors": failed_recovery,
            },
            crawler_started=True,
            success=status is PlatformSiteStatus.COMPLETE,
            message=reason,
        )
    return PlatformSiteResult(
        site_id=site_id,
        profile=profile,
        status=status,
        result=result,
        reason=reason,
        access_state=access_state,
    )


def platform_result(*sites, success=False, interrupted=False):
    return PlatformRunResult(
        profile="global_full",
        sites=sites,
        success=success,
        interrupted=interrupted,
        site_workers=2,
        resource_budget={
            "writer": {"limit": 2, "active": 0, "max_active": 2, "waits": 1},
            "upload": {"limit": 2, "active": 0, "max_active": 2, "waits": 1},
            "catalog": {"limit": 2, "active": 0, "max_active": 1, "waits": 0},
        },
    )


def test_platform_manifest_aggregates_universe_resume_counters_recovery_errors():
    sites = (
        site_result("naver_finance", "naver_full", PlatformSiteStatus.COMPLETE),
        site_result(
            "tossinvest",
            "toss_full",
            PlatformSiteStatus.FAILED,
            reason="RuntimeError: upload failed",
            failed_recovery=1,
        ),
        site_result(
            "kabutan",
            "kabutan_free_full",
            PlatformSiteStatus.BLOCKED,
            reason="waf_human_verification",
            access_state="WAF_BLOCKED",
        ),
        site_result("hkexnews", "hkex_reports_full", PlatformSiteStatus.SKIPPED),
    )
    manifest = build_platform_run_manifest(
        options=PlatformCLIOptions(profile="global_full"),
        result=platform_result(*sites),
        platform_run_id="platform-test",
        started_at=datetime(2026, 9, 17, 1, 2, 3, tzinfo=timezone.utc),
        finished_at=datetime(2026, 9, 17, 1, 3, 3, tzinfo=timezone.utc),
    )

    assert manifest["platform_run_id"] == "platform-test"
    assert manifest["final_state"] == "FAILED"
    assert manifest["site_profiles"]["kabutan"] == "kabutan_free_full"
    assert manifest["universe_snapshots"]["naver_finance"]["count"] >= 1
    assert manifest["universe_snapshots"]["kabutan"] is None
    assert manifest["resume_plan"]["naver_finance"]["forum_post"][
        "durable_complete_scopes"
    ] == 2
    assert manifest["counters"]["total_scopes"] == 15
    assert manifest["counters"]["completed_scopes"] == 12
    assert manifest["counters"]["records_normalized"] == 30
    assert manifest["counters"]["files_cataloged"] == 6
    assert manifest["recovery"]["remaining_pending"] == 1
    assert manifest["blocked_sites"] == ["kabutan"]
    assert manifest["failed_sites"] == ["tossinvest"]
    assert manifest["errors"] == [
        {
            "site_id": "tossinvest",
            "status": "FAILED",
            "message": "RuntimeError: upload failed",
        }
    ]


@pytest.mark.parametrize(
    ("status", "interrupted", "expected"),
    [
        (PlatformSiteStatus.COMPLETE, False, "COMPLETE"),
        (PlatformSiteStatus.BLOCKED, False, "BLOCKED"),
        (PlatformSiteStatus.FAILED, False, "FAILED"),
        (PlatformSiteStatus.INTERRUPTED, True, "INTERRUPTED"),
    ],
)
def test_platform_manifest_records_each_terminal_state(status, interrupted, expected):
    site = site_result("naver_finance", "naver_full", status, reason=expected)
    result = platform_result(
        site,
        success=status in {PlatformSiteStatus.COMPLETE, PlatformSiteStatus.BLOCKED},
        interrupted=interrupted,
    )
    manifest = build_platform_run_manifest(
        options=PlatformCLIOptions(profile="global_full"),
        result=result,
    )
    assert manifest["final_state"] == expected
    assert manifest["interrupted"] is interrupted


def test_platform_manifest_default_name_and_write_are_atomic(tmp_path, monkeypatch):
    site = site_result("kabutan", "kabutan_free_full", PlatformSiteStatus.BLOCKED)
    manifest = build_platform_run_manifest(
        options=PlatformCLIOptions(profile="global_full"),
        result=platform_result(site, success=True),
        platform_run_id="platform-test",
        started_at=datetime(2026, 9, 17, 1, 2, 3, 456789, tzinfo=timezone.utc),
    )
    monkeypatch.chdir(tmp_path)

    path = write_platform_run_manifest(manifest)

    assert path == (
        tmp_path
        / "state/run_manifests/platform_global_full_20260917T010203456789Z.json"
    ).relative_to(tmp_path)
    assert json.loads(path.read_text(encoding="utf-8"))["final_state"] == "BLOCKED"
    assert [item for item in path.parent.iterdir() if item.suffix == ".tmp"] == []
