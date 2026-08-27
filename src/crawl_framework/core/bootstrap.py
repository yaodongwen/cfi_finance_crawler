from __future__ import annotations

from dataclasses import (
    dataclass,
    field,
)
from typing import (
    Any,
    Iterable,
)

from crawl_framework.core.runtime import (
    CrawlRuntime,
    RuntimeStats,
)
from crawl_framework.storage.recovery_orchestrator import (
    RecoveryOrchestrator,
    StartupRecoveryResult,
)


# ============================================================
# Bootstrap errors
# ============================================================


class BootstrapError(
    RuntimeError
):
    """
    crawler 启动流程异常。
    """


class StartupRecoveryBlockedError(
    BootstrapError
):
    """
    Startup recovery 判定当前不允许启动 crawler。
    """


# ============================================================
# Bootstrap result
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class BootstrapResult:
    """
    一次 crawler bootstrap 的完整结果。

    recovery:
        启动前 RecoveryOrchestrator 的结果。

    runtime:
        CrawlRuntime.run() 的返回结果。

        如果 recovery 阻止启动，
        则 runtime=None。

    crawler_started:
        是否真正执行了 CrawlRuntime.run()。

    success:
        整个 bootstrap 是否成功。
    """

    recovery: StartupRecoveryResult

    runtime: Any | None

    crawler_started: bool

    success: bool

    message: str | None = None


# ============================================================
# Bootstrap configuration
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class BootstrapConfig:
    """
    Bootstrap 行为配置。

    raise_on_recovery_block:
        True:
            startup recovery 阻止启动时，
            抛 StartupRecoveryBlockedError。

        False:
            不抛异常，
            返回 BootstrapResult(
                crawler_started=False,
                success=False,
            )

    flush_at_end:
        传给 CrawlRuntime.run()。

    datasets:
        限定需要运行的数据集。

        None:
            运行 plugin 支持的全部 dataset。
    """

    raise_on_recovery_block: bool = False

    flush_at_end: bool = True

    datasets: tuple[
        str,
        ...,
    ] | None = None


# ============================================================
# Bootstrap
# ============================================================


class CrawlBootstrap:
    """
    crawler 的启动协调器。

    标准顺序：

        RecoveryOrchestrator.run()
            ↓
        recovery.can_continue ?
            ↓
        yes
            ↓
        CrawlRuntime.run()

    注意：

    RecoveryOrchestrator:
        负责启动前 crash recovery。

    CrawlRuntime:
        负责正常 crawler execution。

    CrawlBootstrap:
        只负责协调二者，
        不负责具体 recovery 和 crawl 逻辑。
    """

    def __init__(
        self,
        *,
        recovery_orchestrator: RecoveryOrchestrator,
        runtime: CrawlRuntime,
        config: BootstrapConfig | None = None,
    ) -> None:

        self.recovery_orchestrator = (
            recovery_orchestrator
        )

        self.runtime = runtime

        self.config = (
            config
            or BootstrapConfig()
        )


    async def run(
        self,
        *,
        datasets: Iterable[
            str
        ] | None = None,
        flush_at_end: bool | None = None,
    ) -> BootstrapResult:
        """
        执行完整 bootstrap。

        顺序严格固定：

            1. startup recovery
            2. 检查 can_continue
            3. crawler runtime

        recovery 未通过时，
        绝不调用 CrawlRuntime.run()。
        """

        # ====================================================
        # 1. Startup recovery
        # ====================================================

        recovery_result = (
            self.recovery_orchestrator
            .run()
        )

        # ====================================================
        # 2. Recovery blocked startup
        # ====================================================

        if not (
            recovery_result.can_continue
        ):

            message = (
                "crawler startup blocked by "
                "startup recovery"
            )

            if (
                self.config
                .raise_on_recovery_block
            ):

                raise (
                    StartupRecoveryBlockedError(
                        message
                    )
                )

            return BootstrapResult(
                recovery=recovery_result,
                runtime=None,
                crawler_started=False,
                success=False,
                message=message,
            )

        # ====================================================
        # 3. Resolve runtime options
        # ====================================================

        resolved_datasets = (
            tuple(
                datasets
            )
            if datasets is not None
            else self.config.datasets
        )

        resolved_flush_at_end = (
            flush_at_end
            if flush_at_end is not None
            else self.config.flush_at_end
        )

        # ====================================================
        # 4. Run crawler
        # ====================================================

        runtime_result = (
            await self.runtime.run(
                datasets=(
                    resolved_datasets
                ),
                flush_at_end=(
                    resolved_flush_at_end
                ),
            )
        )

        # ====================================================
        # 5. Success
        # ====================================================

        return BootstrapResult(
            recovery=recovery_result,
            runtime=runtime_result,
            crawler_started=True,
            success=True,
            message=(
                "startup recovery completed "
                "and crawler runtime finished"
            ),
        )


# ============================================================
# Convenience async entry point
# ============================================================


async def run_bootstrap(
    *,
    recovery_orchestrator: RecoveryOrchestrator,
    runtime: CrawlRuntime,
    config: BootstrapConfig | None = None,
    datasets: Iterable[
        str
    ] | None = None,
    flush_at_end: bool | None = None,
) -> BootstrapResult:
    """
    CrawlBootstrap 的便捷入口。
    """

    bootstrap = CrawlBootstrap(
        recovery_orchestrator=(
            recovery_orchestrator
        ),
        runtime=runtime,
        config=config,
    )

    return await bootstrap.run(
        datasets=datasets,
        flush_at_end=flush_at_end,
    )