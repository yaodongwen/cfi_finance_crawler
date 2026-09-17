from __future__ import annotations

import asyncio
import time

from types import SimpleNamespace

import pytest

from crawl_framework.core.bootstrap import BootstrapResult
from crawl_framework.core.concurrency import GlobalStageBudget
from crawl_framework.core.platform import (
    GLOBAL_PROFILE_PLANS,
    PlatformOrchestrator,
    PlatformSiteStatus,
    resolve_global_profile,
)
from crawl_framework.core.shutdown import ShutdownController
from crawl_framework.storage.recovery_orchestrator import StartupRecoveryResult


def recovery(*, can_continue=True):
    return StartupRecoveryResult(
        scanned=0,
        attempted=0,
        recovered=0,
        skipped=0,
        failed=0,
        terminal_failed=0,
        remaining_pending=0,
        can_continue=can_continue,
        results=(),
    )


def bootstrap_result(*, success=True, interrupted=False):
    return BootstrapResult(
        recovery=recovery(can_continue=success),
        runtime={"interrupted": interrupted},
        crawler_started=True,
        success=success and not interrupted,
        message="done" if success else "failed",
    )


class FakeBootstrap:
    def __init__(self, site_id, result=None, error=None):
        self.site_id = site_id
        self.result = result or bootstrap_result()
        self.error = error
        self.calls = 0
        self.runtime = SimpleNamespace(shutdown_controller=None)
        self.recovery_orchestrator = SimpleNamespace(run=lambda: recovery())

    async def run(
        self,
        *,
        datasets=None,
        flush_at_end=True,
        startup_recovery_result=None,
    ):
        del datasets, flush_at_end, startup_recovery_result
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.result


def options_for(plan):
    return SimpleNamespace(
        site=plan.site_id,
        profile=plan.profile,
        datasets=("dataset",),
        flush_at_end=True,
    )


def test_global_profiles_map_to_official_site_profiles_in_stable_order():
    assert [(item.site_id, item.profile) for item in GLOBAL_PROFILE_PLANS["global_full"]] == [
        ("naver_finance", "naver_full"),
        ("tossinvest", "toss_full"),
        ("kabutan", "kabutan_free_full"),
        ("hkexnews", "hkex_reports_full"),
    ]
    assert [item.profile for item in resolve_global_profile("global_incremental")] == [
        "naver_incremental",
        "toss_incremental",
        "kabutan_incremental",
        "hkex_reports_incremental",
    ]
    with pytest.raises(ValueError, match="unknown global profile"):
        resolve_global_profile("unknown")


@pytest.mark.asyncio
async def test_platform_orchestrator_runs_all_official_profiles():
    bootstraps = {}

    def factory(options):
        bootstrap = FakeBootstrap(options.site)
        bootstraps[options.site] = bootstrap
        return bootstrap

    async def preflight(plan, bootstrap):
        del bootstrap
        return "AVAILABLE" if plan.site_id == "kabutan" else None

    result = await PlatformOrchestrator(
        bootstrap_factory=factory,
        site_options_factory=options_for,
        site_preflight=preflight,
        site_workers=1,
    ).run("global_incremental")

    assert result.success is True
    assert result.interrupted is False
    assert [item.status for item in result.sites] == [
        PlatformSiteStatus.COMPLETE,
        PlatformSiteStatus.COMPLETE,
        PlatformSiteStatus.COMPLETE,
        PlatformSiteStatus.COMPLETE,
    ]
    assert all(item.calls == 1 for item in bootstraps.values())
    budgets = {id(item.runtime.stage_budget) for item in bootstraps.values()}
    assert len(budgets) == 1
    assert result.site_workers == 1
    assert result.resource_budget["upload"]["limit"] == 2


@pytest.mark.asyncio
async def test_kabutan_waf_is_blocked_and_other_sites_continue():
    bootstraps = {}

    def factory(options):
        bootstrap = FakeBootstrap(options.site)
        bootstraps[options.site] = bootstrap
        return bootstrap

    async def preflight(plan, bootstrap):
        del bootstrap
        return "WAF_BLOCKED" if plan.site_id == "kabutan" else None

    result = await PlatformOrchestrator(
        bootstrap_factory=factory,
        site_options_factory=options_for,
        site_preflight=preflight,
        site_workers=1,
    ).run("global_full")

    assert result.success is True
    assert result.blocked_sites == ("kabutan",)
    assert bootstraps["kabutan"].calls == 0
    assert bootstraps["hkexnews"].calls == 1


@pytest.mark.asyncio
async def test_site_failure_is_isolated_but_platform_result_fails():
    called = []

    def factory(options):
        called.append(options.site)
        return FakeBootstrap(
            options.site,
            error=RuntimeError("boom") if options.site == "tossinvest" else None,
        )

    async def preflight(plan, bootstrap):
        del plan, bootstrap
        return None

    result = await PlatformOrchestrator(
        bootstrap_factory=factory,
        site_options_factory=options_for,
        site_preflight=preflight,
        site_workers=1,
    ).run("global_incremental")

    assert result.success is False
    assert result.failed_sites == ("tossinvest",)
    assert called == ["naver_finance", "tossinvest", "kabutan", "hkexnews"]


@pytest.mark.asyncio
async def test_interrupted_site_skips_remaining_sites():
    created = []

    def factory(options):
        created.append(options.site)
        return FakeBootstrap(
            options.site,
            result=bootstrap_result(success=False, interrupted=True),
        )

    async def preflight(plan, bootstrap):
        del plan, bootstrap
        return None

    result = await PlatformOrchestrator(
        bootstrap_factory=factory,
        site_options_factory=options_for,
        shutdown_controller=ShutdownController(),
        site_preflight=preflight,
        site_workers=1,
    ).run("global_full")

    assert result.interrupted is True
    assert created == ["naver_finance"]
    assert [item.status for item in result.sites] == [
        PlatformSiteStatus.INTERRUPTED,
        PlatformSiteStatus.SKIPPED,
        PlatformSiteStatus.SKIPPED,
        PlatformSiteStatus.SKIPPED,
    ]


@pytest.mark.asyncio
async def test_shutdown_transport_error_is_interrupted_not_failed():
    controller = ShutdownController()

    class DisconnectingBootstrap(FakeBootstrap):
        async def run(self, **kwargs):
            del kwargs
            controller.request_shutdown()
            raise RuntimeError("browser driver connection closed")

    async def preflight(plan, bootstrap):
        del plan, bootstrap
        return None

    result = await PlatformOrchestrator(
        bootstrap_factory=lambda options: DisconnectingBootstrap(options.site),
        site_options_factory=options_for,
        shutdown_controller=controller,
        site_preflight=preflight,
        site_workers=1,
    ).run("global_incremental")

    assert result.interrupted is True
    assert result.failed_sites == ()
    assert result.sites[0].status is PlatformSiteStatus.INTERRUPTED
    assert "browser driver connection closed" in result.sites[0].reason
    assert all(
        item.status is PlatformSiteStatus.SKIPPED
        for item in result.sites[1:]
    )


@pytest.mark.asyncio
async def test_recovery_only_skips_remote_preflight_and_crawl():
    preflight_calls = []
    bootstraps = []

    def factory(options):
        bootstrap = FakeBootstrap(options.site)
        bootstraps.append(bootstrap)
        return bootstrap

    async def preflight(plan, bootstrap):
        preflight_calls.append((plan, bootstrap))
        return None

    result = await PlatformOrchestrator(
        bootstrap_factory=factory,
        site_options_factory=options_for,
        site_preflight=preflight,
    ).run("global_incremental", recovery_only=True)

    assert result.success is True
    assert preflight_calls == []
    assert all(item.calls == 0 for item in bootstraps)


@pytest.mark.asyncio
async def test_site_workers_bound_real_concurrent_bootstrap_runs():
    active = 0
    max_active = 0

    class TimedBootstrap(FakeBootstrap):
        async def run(self, **kwargs):
            nonlocal active, max_active
            del kwargs
            active += 1
            max_active = max(max_active, active)
            try:
                await asyncio.sleep(0.02)
                return self.result
            finally:
                active -= 1

    async def preflight(plan, bootstrap):
        del plan, bootstrap
        return None

    result = await PlatformOrchestrator(
        bootstrap_factory=lambda options: TimedBootstrap(options.site),
        site_options_factory=options_for,
        site_preflight=preflight,
        site_workers=2,
    ).run("global_incremental")

    assert result.success is True
    assert max_active == 2


@pytest.mark.asyncio
async def test_startup_recovery_uses_shared_upload_and_catalog_budget():
    active_recovery = 0
    max_recovery = 0
    budget = GlobalStageBudget(
        writer_workers=4,
        upload_workers=2,
        catalog_workers=2,
    )

    class SlowRecoveryBootstrap(FakeBootstrap):
        def __init__(self, site_id):
            super().__init__(site_id)

            def run_recovery():
                nonlocal active_recovery, max_recovery
                active_recovery += 1
                max_recovery = max(max_recovery, active_recovery)
                try:
                    time.sleep(0.02)
                    return recovery()
                finally:
                    active_recovery -= 1

            self.recovery_orchestrator = SimpleNamespace(run=run_recovery)

    async def preflight(plan, bootstrap):
        del plan, bootstrap
        return None

    result = await PlatformOrchestrator(
        bootstrap_factory=lambda options: SlowRecoveryBootstrap(options.site),
        site_options_factory=options_for,
        site_preflight=preflight,
        site_workers=4,
        stage_budget=budget,
    ).run("global_incremental")

    assert result.success is True
    assert max_recovery == 2
    assert result.resource_budget["upload"]["max_active"] == 2
    assert result.resource_budget["catalog"]["max_active"] == 2
