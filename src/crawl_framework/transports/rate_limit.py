from __future__ import annotations

import random
import time

from dataclasses import dataclass


@dataclass(
    frozen=True,
    slots=True,
)
class AdaptiveRateLimitConfig:
    base_delay_seconds: float = 0.0

    throttle_delay_seconds: float = 1.0

    max_delay_seconds: float = 60.0

    jitter_seconds: float = 0.0

    failure_threshold: int = 3


@dataclass(
    slots=True,
)
class EndpointRateState:
    consecutive_failures: int = 0

    throttles: int = 0

    successes: int = 0

    current_delay_seconds: float = 0.0

    circuit_open_until: float | None = None


class AdaptiveRateLimiter:
    """
    Generic per-endpoint adaptive delay and circuit state.
    """

    def __init__(
        self,
        config: AdaptiveRateLimitConfig | None = None,
        *,
        sleep=time.sleep,
        monotonic=time.monotonic,
        rng=None,
    ) -> None:

        self.config = (
            config
            or AdaptiveRateLimitConfig()
        )

        if self.config.base_delay_seconds < 0:
            raise ValueError(
                "base_delay_seconds must be >= 0"
            )

        if self.config.throttle_delay_seconds < 0:
            raise ValueError(
                "throttle_delay_seconds must be >= 0"
            )

        if self.config.max_delay_seconds < 0:
            raise ValueError(
                "max_delay_seconds must be >= 0"
            )

        if self.config.jitter_seconds < 0:
            raise ValueError(
                "jitter_seconds must be >= 0"
            )

        if self.config.failure_threshold < 1:
            raise ValueError(
                "failure_threshold must be >= 1"
            )

        self._sleep = sleep
        self._monotonic = monotonic
        self._rng = rng or random.Random()
        self._states: dict[str, EndpointRateState] = {}


    def state_for(
        self,
        endpoint: str,
    ) -> EndpointRateState:

        return self._states.setdefault(
            endpoint,
            EndpointRateState(),
        )


    def before_request(
        self,
        endpoint: str,
    ) -> None:

        delay = self.delay_before_request(
            endpoint
        )

        if delay > 0:

            self._sleep(
                delay
            )


    def delay_before_request(
        self,
        endpoint: str,
    ) -> float:
        """Return the required delay without blocking the caller."""

        state = self.state_for(
            endpoint
        )

        now = self._monotonic()

        delay = 0.0

        if (
            state.circuit_open_until is not None
            and state.circuit_open_until > now
        ):

            delay += (
                state.circuit_open_until
                - now
            )

        delay += (
            self.config.base_delay_seconds
            + state.current_delay_seconds
        )

        if self.config.jitter_seconds:

            delay += self._rng.uniform(
                0,
                self.config.jitter_seconds,
            )

        return delay


    def record_success(
        self,
        endpoint: str,
    ) -> None:

        state = self.state_for(
            endpoint
        )

        state.successes += 1
        state.consecutive_failures = 0
        state.current_delay_seconds = 0.0
        state.circuit_open_until = None


    def record_failure(
        self,
        endpoint: str,
        *,
        throttled: bool = False,
    ) -> None:

        state = self.state_for(
            endpoint
        )

        state.consecutive_failures += 1

        if throttled:

            state.throttles += 1

        next_delay = max(
            self.config.throttle_delay_seconds,
            state.current_delay_seconds * 2,
        )

        state.current_delay_seconds = min(
            self.config.max_delay_seconds,
            next_delay,
        )

        if (
            state.consecutive_failures
            >= self.config.failure_threshold
        ):

            state.circuit_open_until = (
                self._monotonic()
                + state.current_delay_seconds
            )
