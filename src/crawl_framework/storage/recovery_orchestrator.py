from __future__ import annotations

from dataclasses import (
    dataclass,
    field,
)

from crawl_framework.storage.recovery import (
    RecoveryManager,
    RecoveryManifest,
    RecoveryResult,
)


# ============================================================
# Startup recovery result
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class StartupRecoveryResult:
    """
    一次启动恢复的汇总结果。

    scanned:
        扫描到的 pending manifest 数量。

    attempted:
        实际调用 recover_one() 的数量。

    recovered:
        成功执行过恢复动作的数量。

    skipped:
        被安全跳过的数量。

    failed:
        本次恢复执行失败的数量。

    terminal_failed:
        已经明确不可自动恢复的 manifest 数量。

    remaining_pending:
        启动恢复后仍然没有进入 deleted 的数量。

    can_continue:
        是否允许继续启动正常 crawler。
    """

    scanned: int

    attempted: int

    recovered: int

    skipped: int

    failed: int

    terminal_failed: int

    remaining_pending: int

    can_continue: bool

    results: tuple[
        RecoveryResult,
        ...,
    ] = field(
        default_factory=tuple
    )


# ============================================================
# Policy
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class StartupRecoveryPolicy:
    """
    控制 startup recovery 是否阻塞 crawler 启动。

    block_on_failed:
        如果本次 recover_one() 出现失败，
        是否阻止 crawler 启动。

    block_on_terminal_failed:
        如果存在 terminal failed manifest，
        是否阻止 crawler 启动。

    block_on_remaining_pending:
        如果恢复结束后仍然存在 pending manifest，
        是否阻止 crawler 启动。

    默认采用比较实用的策略：

        transient recovery failure
            -> 阻止启动

        terminal failed
            -> 不阻止启动
               但必须保留并报警

        普通 remaining pending
            -> 不阻止启动

    原因：

    terminal failure 往往只影响某个历史 batch，
    不能因为一个坏 sidecar 就让整个长期爬虫永远无法启动。
    """

    block_on_failed: bool = True

    block_on_terminal_failed: bool = False

    block_on_remaining_pending: bool = False


# ============================================================
# Orchestrator
# ============================================================


class RecoveryOrchestrator:
    """
    crawler 启动前的 recovery coordinator。

    注意：

    它不实现具体 recovery。

    真正恢复逻辑仍然全部在 RecoveryManager。

    这里负责：

        scan
        classify
        invoke
        summarize
        decide startup
    """

    def __init__(
        self,
        *,
        manager: RecoveryManager,
        policy: StartupRecoveryPolicy | None = None,
    ) -> None:

        self.manager = manager

        self.policy = (
            policy
            or StartupRecoveryPolicy()
        )


    def run(
        self,
    ) -> StartupRecoveryResult:
        """
        执行一次完整 startup recovery。
        """

        pending = (
            self.manager
            .store
            .list_pending()
        )

        scanned = len(
            pending
        )

        attempted = 0

        recovered = 0

        skipped = 0

        failed = 0

        terminal_failed = 0

        results: list[
            RecoveryResult
        ] = []

        # ====================================================
        # Process manifests
        # ====================================================

        for manifest in pending:

            # ------------------------------------------------
            # 已经被明确标记为 terminal failure。
            #
            # 不再次调用 recover_one()。
            # ------------------------------------------------

            if self._is_terminal_failed(
                manifest
            ):

                terminal_failed += 1

                skipped += 1

                results.append(
                    RecoveryResult(
                        manifest_id=(
                            manifest.manifest_id
                        ),
                        original_stage=(
                            manifest.stage
                        ),
                        final_stage="failed",
                        action="skipped",
                        success=True,
                        message=(
                            "startup recovery skipped "
                            "terminal failure: "
                            f"{manifest.retry_reason or 'unknown'}"
                        ),
                    )
                )

                continue

            # ------------------------------------------------
            # 普通 pending / retryable failed
            # ------------------------------------------------

            attempted += 1

            result = (
                self.manager
                .recover_one(
                    manifest
                )
            )

            results.append(
                result
            )

            if not result.success:

                failed += 1

                continue

            if result.action == "skipped":

                skipped += 1

            else:

                recovered += 1

        # ====================================================
        # Re-scan after recovery
        # ====================================================

        remaining = (
            self.manager
            .store
            .list_pending()
        )

        remaining_pending = len(
            remaining
        )

        # ====================================================
        # Startup decision
        # ====================================================

        can_continue = True

        if (
            self.policy.block_on_failed
            and failed > 0
        ):

            can_continue = False

        if (
            self.policy.block_on_terminal_failed
            and terminal_failed > 0
        ):

            can_continue = False

        if (
            self.policy.block_on_remaining_pending
            and remaining_pending > 0
        ):

            can_continue = False

        return StartupRecoveryResult(
            scanned=scanned,
            attempted=attempted,
            recovered=recovered,
            skipped=skipped,
            failed=failed,
            terminal_failed=(
                terminal_failed
            ),
            remaining_pending=(
                remaining_pending
            ),
            can_continue=(
                can_continue
            ),
            results=tuple(
                results
            ),
        )


    @staticmethod
    def _is_terminal_failed(
        manifest: RecoveryManifest,
    ) -> bool:
        """
        是否已经明确属于 terminal failure。
        """

        return (
            manifest.stage
            == "failed"
            and manifest.retryable
            is False
        )