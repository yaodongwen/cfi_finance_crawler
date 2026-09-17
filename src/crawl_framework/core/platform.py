from __future__ import annotations

import asyncio

from dataclasses import dataclass
from enum import Enum
from typing import Any, Awaitable, Callable, Iterable

from crawl_framework.core.bootstrap import BootstrapResult
from crawl_framework.core.concurrency import GlobalStageBudget
from crawl_framework.core.shutdown import ShutdownController
class PlatformSiteStatus(str, Enum):
    COMPLETE = "COMPLETE"
    BLOCKED = "BLOCKED"
    FAILED = "FAILED"
    INTERRUPTED = "INTERRUPTED"
    SKIPPED = "SKIPPED"


@dataclass(frozen=True, slots=True)
class PlatformSitePlan:
    site_id: str
    profile: str


GLOBAL_PROFILE_PLANS: dict[str, tuple[PlatformSitePlan, ...]] = {
    "global_full": (
        PlatformSitePlan("naver_finance", "naver_full"),
        PlatformSitePlan("tossinvest", "toss_full"),
        PlatformSitePlan("kabutan", "kabutan_free_full"),
        PlatformSitePlan("hkexnews", "hkex_reports_full"),
    ),
    "global_incremental": (
        PlatformSitePlan("naver_finance", "naver_incremental"),
        PlatformSitePlan("tossinvest", "toss_incremental"),
        PlatformSitePlan("kabutan", "kabutan_incremental"),
        PlatformSitePlan("hkexnews", "hkex_reports_incremental"),
    ),
}


@dataclass(frozen=True, slots=True)
class PlatformSiteResult:
    site_id: str
    profile: str
    status: PlatformSiteStatus
    result: BootstrapResult | None = None
    reason: str | None = None
    access_state: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "site_id": self.site_id,
            "profile": self.profile,
            "status": self.status.value,
            "reason": self.reason,
            "access_state": self.access_state,
            "success": self.result.success if self.result is not None else None,
            "crawler_started": (
                self.result.crawler_started if self.result is not None else False
            ),
            "runtime": self.result.runtime if self.result is not None else None,
        }


@dataclass(frozen=True, slots=True)
class PlatformRunResult:
    profile: str
    sites: tuple[PlatformSiteResult, ...]
    success: bool
    interrupted: bool
    site_workers: int
    resource_budget: dict[str, dict[str, int]]

    @property
    def blocked_sites(self) -> tuple[str, ...]:
        return tuple(
            item.site_id
            for item in self.sites
            if item.status is PlatformSiteStatus.BLOCKED
        )

    @property
    def failed_sites(self) -> tuple[str, ...]:
        return tuple(
            item.site_id
            for item in self.sites
            if item.status is PlatformSiteStatus.FAILED
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile": self.profile,
            "success": self.success,
            "interrupted": self.interrupted,
            "site_workers": self.site_workers,
            "resource_budget": self.resource_budget,
            "blocked_sites": list(self.blocked_sites),
            "failed_sites": list(self.failed_sites),
            "sites": [item.to_dict() for item in self.sites],
        }


SiteOptionsFactory = Callable[[PlatformSitePlan], Any]
BootstrapFactory = Callable[[Any], Any]
SitePreflight = Callable[[PlatformSitePlan, Any], Awaitable[str | None]]


class PlatformOrchestrator:
    """Run official site profiles with isolation over existing bootstraps."""

    def __init__(
        self,
        *,
        bootstrap_factory: BootstrapFactory,
        site_options_factory: SiteOptionsFactory,
        shutdown_controller: ShutdownController | None = None,
        site_preflight: SitePreflight | None = None,
        site_workers: int = 2,
        stage_budget: GlobalStageBudget | None = None,
    ) -> None:
        self.bootstrap_factory = bootstrap_factory
        self.site_options_factory = site_options_factory
        self.shutdown_controller = shutdown_controller or ShutdownController()
        self.site_preflight = site_preflight or production_site_preflight
        self.site_workers = int(site_workers)
        if self.site_workers < 1:
            raise ValueError("site_workers must be >= 1")
        self.stage_budget = stage_budget or GlobalStageBudget()

    async def run(
        self,
        profile: str,
        *,
        recovery_only: bool = False,
    ) -> PlatformRunResult:
        plans = resolve_global_profile(profile)
        site_semaphore = asyncio.Semaphore(self.site_workers)

        async def run_bounded(plan: PlatformSitePlan) -> PlatformSiteResult:
            async with site_semaphore:
                if self.shutdown_controller.stop_requested:
                    return self._skipped((plan,), "platform interrupted")[0]
                return await self._run_site(plan, recovery_only=recovery_only)

        tasks = [
            asyncio.create_task(run_bounded(plan), name=f"platform:{plan.site_id}")
            for plan in plans
        ]
        try:
            results = list(await asyncio.gather(*tasks))
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

        interrupted = self.shutdown_controller.stop_requested or any(
            item.status is PlatformSiteStatus.INTERRUPTED for item in results
        )
        failed = any(item.status is PlatformSiteStatus.FAILED for item in results)
        return PlatformRunResult(
            profile=profile,
            sites=tuple(results),
            success=not failed and not interrupted,
            interrupted=interrupted,
            site_workers=self.site_workers,
            resource_budget=self.stage_budget.snapshot().to_dict(),
        )

    async def _run_site(
        self,
        plan: PlatformSitePlan,
        *,
        recovery_only: bool,
    ) -> PlatformSiteResult:
        access_state = None
        try:
            options = self.site_options_factory(plan)
            bootstrap = self.bootstrap_factory(options)
            runtime = getattr(bootstrap, "runtime", None)
            if runtime is not None:
                runtime.shutdown_controller = self.shutdown_controller
                runtime.stage_budget = self.stage_budget
                attachment_pipeline = getattr(
                    getattr(runtime, "plugin", None),
                    "attachment_pipeline",
                    None,
                )
                if attachment_pipeline is not None:
                    attachment_pipeline.stage_budget = self.stage_budget

            async with self.stage_budget.recovery_slot():
                recovery = await asyncio.to_thread(
                    bootstrap.recovery_orchestrator.run
                )
            recovery_result = BootstrapResult(
                recovery=recovery,
                runtime=None,
                crawler_started=False,
                success=recovery.can_continue,
                message=(
                    "startup recovery completed"
                    if recovery.can_continue
                    else "startup recovery blocked"
                ),
            )
            if recovery_only:
                result = recovery_result
            elif not recovery.can_continue:
                result = recovery_result
            else:
                access_state = await self.site_preflight(plan, bootstrap)
                if access_state == "WAF_BLOCKED":
                    return PlatformSiteResult(
                        site_id=plan.site_id,
                        profile=plan.profile,
                        status=PlatformSiteStatus.BLOCKED,
                        result=recovery_result,
                        reason="waf_human_verification",
                        access_state=access_state,
                    )
                if access_state not in {None, "AVAILABLE"}:
                    return PlatformSiteResult(
                        site_id=plan.site_id,
                        profile=plan.profile,
                        status=PlatformSiteStatus.FAILED,
                        result=recovery_result,
                        reason=f"preflight access state: {access_state}",
                        access_state=access_state,
                    )
                result = await bootstrap.run(
                    datasets=options.datasets,
                    flush_at_end=options.flush_at_end,
                    startup_recovery_result=recovery,
                )
        except Exception as exc:
            if _is_kabutan_waf_block(exc):
                return PlatformSiteResult(
                    site_id=plan.site_id,
                    profile=plan.profile,
                    status=PlatformSiteStatus.BLOCKED,
                    reason="waf_human_verification",
                    access_state="WAF_BLOCKED",
                )
            if self.shutdown_controller.stop_requested:
                return PlatformSiteResult(
                    site_id=plan.site_id,
                    profile=plan.profile,
                    status=PlatformSiteStatus.INTERRUPTED,
                    reason=(
                        "transport/runtime stopped during platform shutdown: "
                        f"{type(exc).__name__}: {exc}"
                    ),
                    access_state=access_state,
                )
            return PlatformSiteResult(
                site_id=plan.site_id,
                profile=plan.profile,
                status=PlatformSiteStatus.FAILED,
                reason=f"{type(exc).__name__}: {exc}",
            )

        interrupted = (
            isinstance(result.runtime, dict)
            and bool(result.runtime.get("interrupted"))
        )
        if interrupted:
            if not self.shutdown_controller.stop_requested:
                self.shutdown_controller.request_shutdown()
            return PlatformSiteResult(
                site_id=plan.site_id,
                profile=plan.profile,
                status=PlatformSiteStatus.INTERRUPTED,
                result=result,
                reason=result.message,
                access_state=access_state,
            )
        return PlatformSiteResult(
            site_id=plan.site_id,
            profile=plan.profile,
            status=(
                PlatformSiteStatus.COMPLETE
                if result.success
                else PlatformSiteStatus.FAILED
            ),
            result=result,
            reason=None if result.success else result.message,
            access_state=access_state,
        )

    @staticmethod
    def _skipped(
        plans: Iterable[PlatformSitePlan],
        reason: str,
    ) -> list[PlatformSiteResult]:
        return [
            PlatformSiteResult(
                site_id=plan.site_id,
                profile=plan.profile,
                status=PlatformSiteStatus.SKIPPED,
                reason=reason,
            )
            for plan in plans
        ]


def resolve_global_profile(profile: str) -> tuple[PlatformSitePlan, ...]:
    try:
        return GLOBAL_PROFILE_PLANS[str(profile)]
    except KeyError as exc:
        raise ValueError(f"unknown global profile: {profile!r}") from exc


async def production_site_preflight(plan: PlatformSitePlan, bootstrap) -> str | None:
    if plan.site_id != "kabutan":
        return None

    from crawl_framework.sites.kabutan.market_news import probe_kabutan_access

    runtime = getattr(bootstrap, "runtime", None)
    context = getattr(runtime, "context", None)
    transport = getattr(context, "http", None)
    if transport is None:
        raise RuntimeError("Kabutan platform preflight requires HttpTransport")
    result = await probe_kabutan_access(transport)
    return result.access_state


def _is_kabutan_waf_block(exc: Exception) -> bool:
    from crawl_framework.sites.kabutan.market_news import KabutanWafBlockedError

    return isinstance(exc, KabutanWafBlockedError)
