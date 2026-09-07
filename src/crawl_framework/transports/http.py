from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from crawl_framework.transports.proxy import (
    ProxyPool,
)
from crawl_framework.transports.rate_limit import (
    AdaptiveRateLimiter,
)


HttpRequester = Callable[
    [
        str,
        dict[
            str,
            Any,
        ],
    ],
    Awaitable[
        Any,
    ],
]


@dataclass(
    frozen=True,
    slots=True,
)
class HttpRequest:
    url: str

    method: str = "GET"

    params: dict[
        str,
        Any,
    ] | None = None

    headers: dict[
        str,
        str,
    ] = field(
        default_factory=dict
    )


class HttpTransport:
    """
    Generic HTTP transport facade.
    """

    def __init__(
        self,
        *,
        requester: HttpRequester,
        proxy_pool: ProxyPool | None = None,
        rate_limiter: AdaptiveRateLimiter | None = None,
    ) -> None:

        self.requester = requester
        self.proxy_pool = proxy_pool
        self.rate_limiter = rate_limiter


    async def request(
        self,
        request: HttpRequest,
    ):

        proxy = (
            self.proxy_pool.select()
            if self.proxy_pool is not None
            else None
        )

        if self.rate_limiter is not None:

            self.rate_limiter.before_request(
                request.url
            )

        kwargs = {
            "method": request.method,
            "params": request.params,
            "headers": request.headers,
        }

        if proxy is not None:

            kwargs[
                "proxy"
            ] = proxy.url

        try:

            response = await self.requester(
                request.url,
                kwargs,
            )

        except Exception:

            if self.rate_limiter is not None:

                self.rate_limiter.record_failure(
                    request.url
                )

            if (
                self.proxy_pool is not None
                and proxy is not None
            ):

                self.proxy_pool.report_failure(
                    proxy
                )

            raise

        status_code = int(
            getattr(
                response,
                "status_code",
                0,
            )
            or 0
        )

        if self.rate_limiter is not None:

            if status_code in {
                403,
                429,
                500,
                502,
                503,
                504,
            }:

                self.rate_limiter.record_failure(
                    request.url,
                    throttled=(
                        status_code
                        in {
                            403,
                            429,
                        }
                    ),
                )

            elif (
                status_code
                and status_code < 400
            ):

                self.rate_limiter.record_success(
                    request.url
                )

        if (
            self.proxy_pool is not None
            and proxy is not None
        ):

            self.proxy_pool.report_success(
                proxy
            )

        return response
