import pytest

from crawl_framework.storage.retry_policy import (
    RetryPolicy,
)


# ============================================================
# Validation
# ============================================================


def test_negative_max_retries_rejected():

    with pytest.raises(
        ValueError
    ):

        RetryPolicy(
            max_retries=-1
        )


def test_negative_retry_count_rejected():

    policy = RetryPolicy()

    with pytest.raises(
        ValueError
    ):

        policy.classify(
            "timeout",
            retry_count=-1,
        )


# ============================================================
# Retryable
# ============================================================


@pytest.mark.parametrize(
    "message",
    [
        "connection reset by peer",
        "connection refused",
        "network is unreachable",
        "request timed out",
        "temporary failure",
        "rsync failed",
        "ssh failed",
        "database is locked",
        "deadlock detected",
        "too many connections",
    ],
)
def test_retryable_errors(
    message,
):

    policy = RetryPolicy(
        max_retries=3
    )

    result = policy.classify(
        message,
        retry_count=0,
    )

    assert (
        result.should_retry
        is True
    )

    assert (
        result.decision
        == "retry"
    )

    assert (
        result.reason
        == "retryable_error"
    )


# ============================================================
# Non retryable
# ============================================================


@pytest.mark.parametrize(
    "message",
    [
        "sha256 mismatch",
        "checksum mismatch",
        "record index row count mismatch",
        "invalid record index json",
        "record index sidecar missing",
        "schema mismatch",
        "corrupt parquet",
        "cannot reconstruct partition",
        "invalid partition date",
        "invalid recovery bucket",
    ],
)
def test_non_retryable_errors(
    message,
):

    policy = RetryPolicy(
        max_retries=3
    )

    result = policy.classify(
        message,
        retry_count=0,
    )

    assert (
        result.should_retry
        is False
    )

    assert (
        result.decision
        == "stop"
    )

    assert (
        result.reason
        == "non_retryable_error"
    )


# ============================================================
# Retry limit
# ============================================================


def test_retry_below_limit():

    policy = RetryPolicy(
        max_retries=3
    )

    result = policy.classify(
        "connection refused",
        retry_count=2,
    )

    assert (
        result.should_retry
        is True
    )


def test_retry_at_limit_stops():

    policy = RetryPolicy(
        max_retries=3
    )

    result = policy.classify(
        "connection refused",
        retry_count=3,
    )

    assert (
        result.should_retry
        is False
    )

    assert (
        result.reason
        == "max_retries_exceeded"
    )


def test_retry_above_limit_stops():

    policy = RetryPolicy(
        max_retries=3
    )

    result = policy.classify(
        "connection refused",
        retry_count=10,
    )

    assert (
        result.should_retry
        is False
    )

    assert (
        result.reason
        == "max_retries_exceeded"
    )


# ============================================================
# Priority
# ============================================================


def test_non_retryable_has_priority_over_retry_limit():

    policy = RetryPolicy(
        max_retries=3
    )

    result = policy.classify(
        "sha256 mismatch",
        retry_count=10,
    )

    assert (
        result.should_retry
        is False
    )

    assert (
        result.reason
        == "non_retryable_error"
    )


# ============================================================
# Unknown errors
# ============================================================


def test_unknown_error_is_not_retried():

    policy = RetryPolicy(
        max_retries=3
    )

    result = policy.classify(
        "some strange internal bug",
        retry_count=0,
    )

    assert (
        result.should_retry
        is False
    )

    assert (
        result.reason
        == "non_retryable_error"
    )


# ============================================================
# Exceptions
# ============================================================


def test_exception_object_supported():

    policy = RetryPolicy()

    error = ConnectionError(
        "connection refused"
    )

    result = policy.classify(
        error,
        retry_count=0,
    )

    assert (
        result.should_retry
        is True
    )


# ============================================================
# can_retry shortcut
# ============================================================


def test_can_retry_true():

    policy = RetryPolicy()

    assert (
        policy.can_retry(
            "network is unreachable",
            retry_count=0,
        )
        is True
    )


def test_can_retry_false():

    policy = RetryPolicy()

    assert (
        policy.can_retry(
            "schema mismatch",
            retry_count=0,
        )
        is False
    )


# ============================================================
# Zero retries
# ============================================================


def test_zero_max_retries_disables_retry():

    policy = RetryPolicy(
        max_retries=0
    )

    result = policy.classify(
        "connection refused",
        retry_count=0,
    )

    assert (
        result.should_retry
        is False
    )

    assert (
        result.reason
        == "max_retries_exceeded"
    )