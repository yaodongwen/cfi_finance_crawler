from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from crawl_framework.transports.proxy import (
    ProxyPool,
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
    ) -> None:

        self.requester = requester
        self.proxy_pool = proxy_pool


    async def request(
        self,
        request: HttpRequest,
    ):

        proxy = (
            self.proxy_pool.select()
            if self.proxy_pool is not None
            else None
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

            if (
                self.proxy_pool is not None
                and proxy is not None
            ):

                self.proxy_pool.report_failure(
                    proxy
                )

            raise

        if (
            self.proxy_pool is not None
            and proxy is not None
        ):

            self.proxy_pool.report_success(
                proxy
            )

        return response
