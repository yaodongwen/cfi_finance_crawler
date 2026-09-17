from __future__ import annotations

import asyncio
import urllib.error
import urllib.parse
import urllib.request
import threading
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

import requests
from requests.adapters import HTTPAdapter

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

    data: dict[
        str,
        Any,
    ] | bytes | None = None

    headers: dict[
        str,
        str,
    ] = field(
        default_factory=dict
    )


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status_code: int
    content: bytes
    url: str
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def text(self) -> str:
        content_type = self.headers.get("Content-Type", "")
        charset = ""
        for part in content_type.split(";")[1:]:
            key, separator, value = part.strip().partition("=")
            if separator and key.lower() == "charset":
                charset = value.strip(" \"'")
                break
        for encoding in (charset, "utf-8", "cp932", "euc_jp"):
            if not encoding:
                continue
            try:
                return self.content.decode(encoding)
            except (LookupError, UnicodeDecodeError):
                continue
        return self.content.decode("utf-8", errors="replace")


@dataclass(frozen=True, slots=True)
class HttpRetryConfig:
    max_attempts: int = 5
    backoff_seconds: float = 0.5
    retry_statuses: frozenset[int] = frozenset({429, 500, 502, 503, 504})


class UrllibHttpRequester:
    def __init__(self, *, timeout_seconds: float = 20.0) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self.timeout_seconds = timeout_seconds

    async def __call__(self, url: str, kwargs: dict[str, Any]) -> HttpResponse:
        return await asyncio.to_thread(self._request_sync, url, kwargs)

    def _request_sync(self, url: str, kwargs: dict[str, Any]) -> HttpResponse:
        params = kwargs.get("params") or {}
        if params:
            query = urllib.parse.urlencode(params)
            separator = "&" if urllib.parse.urlsplit(url).query else "?"
            url = f"{url}{separator}{query}"

        data = kwargs.get("data")
        if isinstance(data, dict):
            data = urllib.parse.urlencode(data).encode("utf-8")

        request = urllib.request.Request(
            url,
            data=data,
            headers=dict(kwargs.get("headers") or {}),
            method=str(kwargs.get("method", "GET")),
        )
        proxy = kwargs.get("proxy")
        handlers = []
        if proxy:
            handlers.append(urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
        opener = urllib.request.build_opener(*handlers)

        try:
            response = opener.open(request, timeout=self.timeout_seconds)
            content = response.read()
            return HttpResponse(
                status_code=int(response.status),
                content=content,
                url=str(response.geturl()),
                headers=dict(response.headers.items()),
            )
        except urllib.error.HTTPError as exc:
            return HttpResponse(
                status_code=int(exc.code),
                content=exc.read(),
                url=str(exc.geturl()),
                headers=dict(exc.headers.items()) if exc.headers else {},
            )


class RequestsHttpRequester:
    """Thread-local pooled requester for async ``to_thread`` transports."""

    def __init__(
        self,
        *,
        timeout_seconds: float = 20.0,
        pool_size: int = 16,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if pool_size < 1:
            raise ValueError("pool_size must be >= 1")
        self.timeout_seconds = timeout_seconds
        self.pool_size = pool_size
        self._local = threading.local()

    async def __call__(self, url: str, kwargs: dict[str, Any]):
        return await asyncio.to_thread(self._request_sync, url, kwargs)

    def _session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            adapter = HTTPAdapter(
                pool_connections=self.pool_size,
                pool_maxsize=self.pool_size,
                max_retries=0,
                pool_block=True,
            )
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            self._local.session = session
        return session

    def _request_sync(self, url: str, kwargs: dict[str, Any]):
        proxy = kwargs.get("proxy")
        proxies = {"http": proxy, "https": proxy} if proxy else None
        return self._session().request(
            method=str(kwargs.get("method", "GET")),
            url=url,
            params=kwargs.get("params"),
            data=kwargs.get("data"),
            headers=kwargs.get("headers"),
            proxies=proxies,
            timeout=self.timeout_seconds,
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
        retry_config: HttpRetryConfig | None = None,
        max_concurrency: int = 1,
        sleep=asyncio.sleep,
    ) -> None:

        self.requester = requester
        self.proxy_pool = proxy_pool
        self.rate_limiter = rate_limiter
        # Keep direct/injected transport behavior backward-compatible; the
        # production composition root explicitly supplies its retry policy.
        self.retry_config = retry_config or HttpRetryConfig(max_attempts=1)
        self.sleep = sleep
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        self.max_concurrency = max_concurrency
        self._semaphore = asyncio.Semaphore(max_concurrency)

        if self.retry_config.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.retry_config.backoff_seconds < 0:
            raise ValueError("backoff_seconds must be >= 0")


    async def request(
        self,
        request: HttpRequest,
    ):

        async with self._semaphore:
            return await self._request_with_retries(request)


    async def _request_with_retries(self, request: HttpRequest):

        for attempt in range(1, self.retry_config.max_attempts + 1):
            proxy = self.proxy_pool.select() if self.proxy_pool is not None else None
            if self.rate_limiter is not None:
                delay_before_request = getattr(
                    self.rate_limiter,
                    "delay_before_request",
                    None,
                )
                if delay_before_request is None:
                    self.rate_limiter.before_request(request.url)
                else:
                    delay = delay_before_request(request.url)
                    if delay > 0:
                        await self.sleep(delay)

            kwargs = {
                "method": request.method,
                "params": request.params,
                "data": request.data,
                "headers": request.headers,
            }
            if proxy is not None:
                kwargs["proxy"] = proxy.url

            try:
                response = await self.requester(request.url, kwargs)
            except Exception:
                if self.rate_limiter is not None:
                    self.rate_limiter.record_failure(request.url)
                if self.proxy_pool is not None and proxy is not None:
                    self.proxy_pool.report_failure(proxy)
                if attempt >= self.retry_config.max_attempts:
                    raise
            else:
                status_code = int(getattr(response, "status_code", 0) or 0)
                retryable = status_code in self.retry_config.retry_statuses
                if self.rate_limiter is not None:
                    if retryable or status_code == 403:
                        self.rate_limiter.record_failure(
                            request.url,
                            throttled=status_code in {403, 429},
                        )
                    elif status_code and status_code < 400:
                        self.rate_limiter.record_success(request.url)
                if self.proxy_pool is not None and proxy is not None:
                    if retryable or status_code >= 400:
                        self.proxy_pool.report_failure(proxy)
                    else:
                        self.proxy_pool.report_success(proxy)
                if not retryable or attempt >= self.retry_config.max_attempts:
                    return response

            delay = self.retry_config.backoff_seconds * (2 ** (attempt - 1))
            if delay:
                await self.sleep(delay)

        raise AssertionError("HTTP retry loop exhausted")
