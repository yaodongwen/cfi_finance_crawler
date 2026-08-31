from __future__ import annotations

import random

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal


ProxyStrategy = Literal[
    "round_robin",
    "random",
]


@dataclass(
    frozen=True,
    slots=True,
)
class ProxyEndpoint:
    url: str


@dataclass(
    frozen=True,
    slots=True,
)
class ProxyPoolConfig:
    strategy: ProxyStrategy = "round_robin"

    failure_cooldown_seconds: float = 30.0


@dataclass(
    slots=True,
)
class ProxyState:
    endpoint: ProxyEndpoint

    failures: int = 0

    successes: int = 0

    cooldown_until: datetime | None = None


class ProxyPool:
    """
    Generic proxy selector with cooldown tracking.
    """

    def __init__(
        self,
        endpoints,
        *,
        config: ProxyPoolConfig | None = None,
        rng=None,
    ) -> None:

        self.config = (
            config
            or ProxyPoolConfig()
        )

        if self.config.strategy not in {
            "round_robin",
            "random",
        }:

            raise ValueError(
                "unsupported proxy strategy"
            )

        self.states = [
            ProxyState(
                endpoint=(
                    endpoint
                    if isinstance(
                        endpoint,
                        ProxyEndpoint,
                    )
                    else ProxyEndpoint(
                        str(
                            endpoint
                        )
                    )
                )
            )
            for endpoint in endpoints
        ]

        self._index = 0

        self._rng = (
            rng
            or random.Random()
        )


    def select(
        self,
        *,
        now: datetime | None = None,
    ) -> ProxyEndpoint | None:

        if not self.states:

            return None

        now = (
            now
            or datetime.now(
                timezone.utc
            )
        )

        available = [
            state
            for state in self.states
            if (
                state.cooldown_until is None
                or state.cooldown_until <= now
            )
        ]

        if not available:

            return None

        if self.config.strategy == "random":

            return self._rng.choice(
                available
            ).endpoint

        for _ in range(
            len(
                self.states
            )
        ):

            state = self.states[
                self._index
                %
                len(
                    self.states
                )
            ]

            self._index += 1

            if state in available:

                return state.endpoint

        return None


    def report_success(
        self,
        endpoint: ProxyEndpoint | str,
    ) -> None:

        state = self._state_for(
            endpoint
        )

        state.successes += 1
        state.cooldown_until = None


    def report_failure(
        self,
        endpoint: ProxyEndpoint | str,
        *,
        now: datetime | None = None,
    ) -> None:

        state = self._state_for(
            endpoint
        )

        now = (
            now
            or datetime.now(
                timezone.utc
            )
        )

        state.failures += 1
        state.cooldown_until = (
            now
            +
            timedelta(
                seconds=(
                    self.config
                    .failure_cooldown_seconds
                )
            )
        )


    def _state_for(
        self,
        endpoint: ProxyEndpoint | str,
    ) -> ProxyState:

        url = (
            endpoint.url
            if isinstance(
                endpoint,
                ProxyEndpoint,
            )
            else str(
                endpoint
            )
        )

        for state in self.states:

            if state.endpoint.url == url:

                return state

        raise KeyError(
            f"unknown proxy endpoint: {url}"
        )
