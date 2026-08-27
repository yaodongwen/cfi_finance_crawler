from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


# ============================================================
# Retry classification
# ============================================================


RetryDecision = Literal[
    "retry",
    "stop",
]


RetryReason = Literal[
    "retryable_error",
    "non_retryable_error",
    "max_retries_exceeded",
]


# ============================================================
# Error categories
# ============================================================


# 一般可以自动重试的错误关键词。
#
# 后面真正接入 uploader / postgres 时，
# 可以逐步升级为明确异常类型判断。
RETRYABLE_KEYWORDS: tuple[str, ...] = (
    # ========================================================
    # Network
    # ========================================================
    "timeout",
    "timed out",
    "connection reset",
    "connection refused",
    "connection aborted",
    "connection closed",
    "network is unreachable",
    "broken pipe",
    "could not connect",
    "server closed the connection",

    # ========================================================
    # Temporary / transient
    # ========================================================
    "temporary failure",
    "temporarily unavailable",
    "temporary seen store failure",
    "temporary database failure",
    "temporary network failure",
    "temporary upload failure",

    # ========================================================
    # Upload / SSH / rsync
    # ========================================================
    "rsync failed",
    "ssh failed",
    "remote unavailable",

    # ========================================================
    # Database
    # ========================================================
    "database is locked",
    "deadlock",
    "serialization failure",
    "too many connections",
)


# 这类错误通常意味着：
#
# 数据损坏 / schema 不兼容 / recovery 元数据不一致。
#
# 自动反复重试没有意义，反而可能掩盖问题。
NON_RETRYABLE_KEYWORDS: tuple[str, ...] = (
    "sha256 mismatch",
    "checksum mismatch",
    "row count mismatch",
    "record index row count mismatch",
    "invalid record index",
    "record index sidecar missing",
    "invalid parquet",
    "schema mismatch",
    "schema version",
    "corrupt",
    "malformed",
    "cannot reconstruct partition",
    "invalid partition date",
    "invalid recovery bucket",
)


# ============================================================
# Result
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class RetryPolicyResult:
    """
    一次 retry policy 判断结果。
    """

    decision: RetryDecision

    reason: RetryReason

    retry_count: int

    max_retries: int

    error_message: str


    @property
    def should_retry(
        self,
    ) -> bool:

        return (
            self.decision
            == "retry"
        )


# ============================================================
# Retry policy
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class RetryPolicy:
    """
    Recovery 自动重试策略。

    max_retries:

        允许失败的最大次数。

    示例：

        max_retries=3

    manifest.retry_count:

        0 -> 可以 retry
        1 -> 可以 retry
        2 -> 可以 retry
        3 -> stop

    即 retry_count >= max_retries 后停止。
    """

    max_retries: int = 3


    def __post_init__(
        self,
    ) -> None:

        if self.max_retries < 0:

            raise ValueError(
                "max_retries cannot be negative"
            )


    def classify(
        self,
        error: Exception | str,
        *,
        retry_count: int,
    ) -> RetryPolicyResult:
        """
        判断当前 recovery error 是否应该继续自动重试。
        """

        if retry_count < 0:

            raise ValueError(
                "retry_count cannot be negative"
            )

        message = str(
            error
        ).strip()

        normalized = (
            message.lower()
        )

        # ====================================================
        # 1. 明确不可重试错误
        #
        # 优先级最高。
        # ====================================================

        if self._contains_any(
            normalized,
            NON_RETRYABLE_KEYWORDS,
        ):

            return RetryPolicyResult(
                decision="stop",
                reason=(
                    "non_retryable_error"
                ),
                retry_count=retry_count,
                max_retries=(
                    self.max_retries
                ),
                error_message=message,
            )

        # ====================================================
        # 2. 已达到最大重试次数
        # ====================================================

        if (
            retry_count
            >= self.max_retries
        ):

            return RetryPolicyResult(
                decision="stop",
                reason=(
                    "max_retries_exceeded"
                ),
                retry_count=retry_count,
                max_retries=(
                    self.max_retries
                ),
                error_message=message,
            )

        # ====================================================
        # 3. 明确可重试错误
        # ====================================================

        if self._contains_any(
            normalized,
            RETRYABLE_KEYWORDS,
        ):

            return RetryPolicyResult(
                decision="retry",
                reason="retryable_error",
                retry_count=retry_count,
                max_retries=(
                    self.max_retries
                ),
                error_message=message,
            )

        # ====================================================
        # 4. 未知错误
        #
        # 当前采取保守策略：
        #
        #     unknown -> stop
        #
        # 不允许未知异常无限自动重试。
        # ====================================================

        return RetryPolicyResult(
            decision="stop",
            reason="non_retryable_error",
            retry_count=retry_count,
            max_retries=(
                self.max_retries
            ),
            error_message=message,
        )


    def can_retry(
        self,
        error: Exception | str,
        *,
        retry_count: int,
    ) -> bool:

        return (
            self.classify(
                error,
                retry_count=retry_count,
            )
            .should_retry
        )


    @staticmethod
    def _contains_any(
        message: str,
        keywords: tuple[
            str,
            ...,
        ],
    ) -> bool:

        return any(
            keyword
            in message
            for keyword
            in keywords
        )