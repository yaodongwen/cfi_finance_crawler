from __future__ import annotations

import asyncio
import html
import random
import re
import time

from dataclasses import dataclass
from typing import (
    Any,
    AsyncIterator,
)
from urllib.parse import (
    parse_qs,
    urljoin,
    urlparse,
)

import requests

from bs4 import BeautifulSoup


NAVER_FINANCE_BASE_URL = "https://finance.naver.com"

NAVER_FINANCE_ROOT_URL = NAVER_FINANCE_BASE_URL

NAVER_STOCK_MAIN_URL = (
    NAVER_FINANCE_BASE_URL
    + "/item/main.naver"
)

NAVER_STOCK_NEWS_URL = (
    NAVER_FINANCE_BASE_URL
    + "/item/news_news.naver"
)

NAVER_NEWS_ARTICLE_URL = (
    "https://n.news.naver.com/mnews/article"
)

DEFAULT_TIMEOUT_SECONDS = 20

DEFAULT_MIN_BODY_LENGTH = 100

DEFAULT_STOP_REPEAT_PAGES = 3

DEFAULT_HTTP_RETRIES = 3

DEFAULT_REQUEST_DELAY_SECONDS = 0.0

USER_AGENTS = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 Chrome/131.0 Safari/537.36",
)


class NaverNewsError(
    RuntimeError
):
    """
    Base error for real Naver news crawling.
    """


class NaverNewsHTTPError(
    NaverNewsError
):
    """
    HTTP request failed.
    """


class NaverNewsParseError(
    NaverNewsError
):
    """
    Naver HTML could not be parsed into an article.
    """


@dataclass(
    frozen=True,
    slots=True,
)
class NaverNewsListItem:
    """
    One Naver Finance stock news list item.
    """

    code: str

    article_id: str

    office_id: str

    title: str

    url: str

    canonical_url: str

    provider: str | None = None

    listed_at: str | None = None

    page: int | None = None


    @property
    def source_id(
        self,
    ) -> str:

        return (
            f"{self.office_id}:{self.article_id}"
        )


    def to_raw(
        self,
    ) -> dict[
        str,
        Any,
    ]:

        return {
            "code": self.code,
            "article_id": self.article_id,
            "office_id": self.office_id,
            "title": self.title,
            "url": self.url,
            "canonical": self.canonical_url,
            "canonical_url": self.canonical_url,
            "provider": self.provider,
            "date": self.listed_at,
            "page": self.page,
        }


def clean_text(
    value: Any,
) -> str:

    if value is None:

        return ""

    text = html.unescape(
        str(
            value
        )
    )

    text = text.replace(
        "\xa0",
        " ",
    )

    text = re.sub(
        r"\s+",
        " ",
        text,
    )

    return text.strip()


def normalize_naver_stock_code(
    value: Any,
) -> str:

    code = str(
        value
    ).strip()

    if code.upper().startswith(
        "XKRX:"
    ):

        code = code.split(
            ":",
            1,
        )[1].strip()

    if not code:

        raise ValueError(
            "Naver stock code cannot be empty"
        )

    return code


def news_list_url(
    code: Any,
    page: int,
) -> str:

    code = normalize_naver_stock_code(
        code
    )

    page = int(
        page
    )

    if page <= 0:

        raise ValueError(
            "page must be positive"
        )

    return (
        f"{NAVER_STOCK_NEWS_URL}"
        f"?code={code}"
        f"&page={page}"
        f"&clusterId="
    )


def canonical_news_url(
    office_id: str,
    article_id: str,
) -> str:

    return (
        f"{NAVER_NEWS_ARTICLE_URL}"
        f"/{office_id}"
        f"/{article_id}"
    )


def parse_news_list(
    text: str,
    *,
    code: Any,
    page: int | None = None,
) -> list[
    NaverNewsListItem
]:
    """
    Parse /item/news_news.naver HTML.

    The stable identity is office_id + article_id.
    """

    code = normalize_naver_stock_code(
        code
    )

    soup = BeautifulSoup(
        text,
        "lxml",
    )

    result: list[
        NaverNewsListItem
    ] = []

    seen: set[
        tuple[
            str,
            str,
        ]
    ] = set()

    for anchor in soup.find_all(
        "a",
        href=True,
    ):

        href = str(
            anchor[
                "href"
            ]
        )

        if "news_read.naver" not in href:

            continue

        title = clean_text(
            anchor.get_text(
                " ",
                strip=True,
            )
        )

        if len(
            title
        ) < 5:

            continue

        parsed = urlparse(
            html.unescape(
                href
            )
        )

        query = parse_qs(
            parsed.query
        )

        office_id = str(
            (
                query.get(
                    "office_id",
                    [""],
                )[0]
            )
        ).strip()

        article_id = str(
            (
                query.get(
                    "article_id",
                    [""],
                )[0]
            )
        ).strip()

        if (
            not office_id
            or not article_id
        ):

            continue

        key = (
            office_id,
            article_id,
        )

        if key in seen:

            continue

        seen.add(
            key
        )

        provider = None

        listed_at = None

        row = anchor.find_parent(
            "tr"
        )

        if row is not None:

            provider_node = row.select_one(
                ".info"
            )

            if provider_node is not None:

                provider = clean_text(
                    provider_node.get_text(
                        " ",
                        strip=True,
                    )
                ) or None

            date_node = row.select_one(
                ".date"
            )

            if date_node is not None:

                listed_at = clean_text(
                    date_node.get_text(
                        " ",
                        strip=True,
                    )
                ) or None

        result.append(
            NaverNewsListItem(
                code=code,
                article_id=article_id,
                office_id=office_id,
                title=title,
                url=urljoin(
                    NAVER_FINANCE_BASE_URL,
                    href,
                ),
                canonical_url=canonical_news_url(
                    office_id,
                    article_id,
                ),
                provider=provider,
                listed_at=listed_at,
                page=page,
            )
        )

    return result


def parse_article_html(
    text: str,
    *,
    news: NaverNewsListItem | dict[
        str,
        Any,
    ],
    min_body_length: int = DEFAULT_MIN_BODY_LENGTH,
) -> dict[
    str,
    Any,
]:
    """
    Parse n.news.naver.com article HTML into framework raw data.
    """

    soup = BeautifulSoup(
        text,
        "lxml",
    )

    base = (
        news.to_raw()
        if isinstance(
            news,
            NaverNewsListItem,
        )
        else dict(
            news
        )
    )

    title = _first_text(
        soup,
        (
            "#title_area span",
            "#title_area",
            "h2.media_end_head_headline",
        ),
    )

    if not title:

        meta_title = soup.select_one(
            'meta[property="og:title"]'
        )

        if meta_title is not None:

            title = clean_text(
                meta_title.get(
                    "content",
                    "",
                )
            )

    if not title:

        title = clean_text(
            base.get(
                "title",
                "",
            )
        )

    body_node = None

    body_selector = ""

    for selector in (
        "#dic_area",
        "article#dic_area",
        "#newsct_article",
        "#articleBodyContents",
        "#articeBody",
        ".newsct_article",
        ".article_body",
    ):

        candidate = soup.select_one(
            selector
        )

        if candidate is not None:

            body_node = candidate

            body_selector = selector

            break

    if body_node is None:

        raise NaverNewsParseError(
            "Naver news article body was not found: "
            f"{base.get('canonical_url') or base.get('canonical')}"
        )

    for selector in (
        "script",
        "style",
        "iframe",
        "noscript",
        "button",
        ".end_photo_org",
        ".img_desc",
        ".media_end_head_autosummary",
        ".media_end_head_journalist",
        ".media_end_related",
        ".media_end_linked",
        ".copyright",
        ".reporter_area",
        ".journalistcard",
        ".promotion",
        ".article_footer",
    ):

        for node in body_node.select(
            selector
        ):

            node.decompose()

    for br in body_node.find_all(
        "br"
    ):

        br.replace_with(
            "\n"
        )

    for paragraph in body_node.find_all(
        [
            "p",
            "div",
            "section",
            "blockquote",
            "li",
        ]
    ):

        paragraph.append(
            "\n"
        )

    content = body_node.get_text(
        "\n",
        strip=False,
    )

    content = html.unescape(
        content
    )

    content = content.replace(
        "\xa0",
        " ",
    )

    content = content.replace(
        "\u200b",
        "",
    )

    content = content.replace(
        "\ufeff",
        "",
    )

    content = re.sub(
        r"[ \t]+",
        " ",
        content,
    )

    content = re.sub(
        r"\r\n?",
        "\n",
        content,
    )

    content = re.sub(
        r"\n[ \t]+",
        "\n",
        content,
    )

    content = re.sub(
        r"\n{3,}",
        "\n\n",
        content,
    ).strip()

    if len(
        content
    ) < min_body_length:

        raise NaverNewsParseError(
            "Naver news article body is too short: "
            f"length={len(content)}, "
            f"selector={body_selector}, "
            f"url={base.get('canonical_url') or base.get('canonical')}"
        )

    provider = _provider(
        soup
    ) or base.get(
        "provider"
    )

    published_at = _published_at(
        soup
    ) or base.get(
        "published_at"
    ) or base.get(
        "date"
    )

    author = _author(
        soup
    ) or base.get(
        "author"
    )

    canonical_url = (
        base.get(
            "canonical_url"
        )
        or base.get(
            "canonical"
        )
        or canonical_news_url(
            str(
                base.get(
                    "office_id",
                    "",
                )
            ),
            str(
                base.get(
                    "article_id",
                    "",
                )
            ),
        )
    )

    return {
        "code": base.get(
            "code"
        ),
        "article_id": base.get(
            "article_id"
        ),
        "office_id": base.get(
            "office_id"
        ),
        "title": title,
        "content": content,
        "body": content,
        "url": canonical_url,
        "canonical": canonical_url,
        "canonical_url": canonical_url,
        "finance_url": base.get(
            "url"
        ),
        "source": provider,
        "provider": provider,
        "author": author,
        "journalist": author,
        "published_at": published_at,
        "body_selector": body_selector,
        "page": base.get(
            "page"
        ),
    }


def _first_text(
    soup: BeautifulSoup,
    selectors: tuple[
        str,
        ...,
    ],
) -> str:

    for selector in selectors:

        node = soup.select_one(
            selector
        )

        if node is None:

            continue

        text = clean_text(
            node.get_text(
                " ",
                strip=True,
            )
        )

        if text:

            return text

    return ""


def _provider(
    soup: BeautifulSoup,
) -> str | None:

    logo_node = soup.select_one(
        ".media_end_head_top_logo img"
    )

    if logo_node is not None:

        value = clean_text(
            logo_node.get(
                "alt",
                "",
            )
        )

        if value:

            return value

    return (
        _first_text(
            soup,
            (
                ".media_end_head_top_logo_text",
            ),
        )
        or None
    )


def _published_at(
    soup: BeautifulSoup,
) -> str | None:

    node = soup.select_one(
        "span.media_end_head_info_datestamp_time"
    )

    if node is not None:

        value = clean_text(
            node.get(
                "data-date-time",
                "",
            )
            or node.get_text(
                " ",
                strip=True,
            )
        )

        if value:

            return value

    for selector in (
        'meta[property="article:published_time"]',
        'meta[property="og:article:published_time"]',
    ):

        meta = soup.select_one(
            selector
        )

        if meta is None:

            continue

        value = clean_text(
            meta.get(
                "content",
                "",
            )
        )

        if value:

            return value

    return None


def _author(
    soup: BeautifulSoup,
) -> str | None:

    return (
        _first_text(
            soup,
            (
                ".media_end_head_journalist_name",
                ".byline_s",
                ".journalistcard_summary_name",
            ),
        )
        or None
    )


class NaverNewsClient:
    """
    Real HTTP client for Naver Finance stock news.
    """

    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        min_body_length: int = DEFAULT_MIN_BODY_LENGTH,
        retries: int = DEFAULT_HTTP_RETRIES,
        request_delay_seconds: float = DEFAULT_REQUEST_DELAY_SECONDS,
        warmup_enabled: bool = True,
        stop_repeat_pages: int = DEFAULT_STOP_REPEAT_PAGES,
        verify_ssl: bool = True,
        rate_limiter=None,
    ) -> None:

        if timeout_seconds <= 0:

            raise ValueError(
                "timeout_seconds must be positive"
            )

        if min_body_length < 0:

            raise ValueError(
                "min_body_length cannot be negative"
            )

        if retries <= 0:

            raise ValueError(
                "retries must be positive"
            )

        if stop_repeat_pages <= 0:

            raise ValueError(
                "stop_repeat_pages must be positive"
            )

        self.session = (
            session
            or self.create_session()
        )

        self.timeout_seconds = timeout_seconds

        self.min_body_length = min_body_length

        self.retries = retries

        self.request_delay_seconds = (
            request_delay_seconds
        )

        self.warmup_enabled = warmup_enabled

        self.stop_repeat_pages = stop_repeat_pages

        self.verify_ssl = verify_ssl

        self.rate_limiter = rate_limiter

        self.detail_errors: list[
            dict[
                str,
                Any,
            ]
        ] = []

        self._warmed = False


    @staticmethod
    def create_session(
    ) -> requests.Session:

        session = requests.Session()

        session.headers.update(
            {
                "User-Agent": USER_AGENTS[0],
                "Accept-Language": (
                    "ko-KR,ko;q=0.9"
                ),
            }
        )

        adapter = requests.adapters.HTTPAdapter(
            pool_connections=16,
            pool_maxsize=16,
        )

        session.mount(
            "https://",
            adapter,
        )

        session.mount(
            "http://",
            adapter,
        )

        return session


    def warmup(
        self,
    ) -> None:

        if self._warmed:

            return

        if not self.warmup_enabled:

            self._warmed = True

            return

        try:

            self._get_text(
                NAVER_FINANCE_ROOT_URL,
            )

            self._sleep()

            self._get_text(
                NAVER_STOCK_MAIN_URL,
                params={
                    "code": "005930",
                },
            )

        except NaverNewsError:

            pass

        self._warmed = True


    def fetch_news_list(
        self,
        code: Any,
        *,
        page: int,
    ) -> list[
        NaverNewsListItem
    ]:

        code = normalize_naver_stock_code(
            code
        )

        self.warmup()

        url = news_list_url(
            code,
            page,
        )

        text = self._get_text(
            url,
            referer=(
                f"{NAVER_STOCK_MAIN_URL}"
                f"?code={code}"
            ),
        )

        return parse_news_list(
            text,
            code=code,
            page=page,
        )


    def fetch_article(
        self,
        news: NaverNewsListItem | dict[
            str,
            Any,
        ],
    ) -> dict[
        str,
        Any,
    ]:

        base = (
            news.to_raw()
            if isinstance(
                news,
                NaverNewsListItem,
            )
            else dict(
                news
            )
        )

        canonical_url = (
            base.get(
                "canonical_url"
            )
            or base.get(
                "canonical"
            )
        )

        if not canonical_url:

            canonical_url = canonical_news_url(
                str(
                    base.get(
                        "office_id",
                        "",
                    )
                ),
                str(
                    base.get(
                        "article_id",
                        "",
                    )
                ),
            )

        text = self._get_text(
            str(
                canonical_url
            ),
            referer=base.get(
                "url"
            ),
        )

        return parse_article_html(
            text,
            news=base,
            min_body_length=(
                self.min_body_length
            ),
        )


    async def crawl_pages(
        self,
        code: Any,
        *,
        start_page: int = 1,
        max_pages: int | None = None,
        mode: str = "incremental",
        seen_source_ids: set[
            str
        ] | None = None,
    ) -> AsyncIterator[
        dict[
            str,
            Any,
        ]
    ]:

        code = normalize_naver_stock_code(
            code
        )

        start_page = int(
            start_page
        )

        if start_page <= 0:

            raise ValueError(
                "start_page must be positive"
            )

        if max_pages is not None:

            max_pages = int(
                max_pages
            )

            if max_pages <= 0:

                raise ValueError(
                    "max_pages must be positive"
                )

        normalized_mode = str(
            mode
        ).strip().lower()

        if normalized_mode not in {
            "full",
            "incremental",
        }:

            raise ValueError(
                "mode must be full or incremental"
            )

        current_page = start_page

        processed_pages = 0

        repeat_pages = 0

        known_source_ids = (
            seen_source_ids
            if seen_source_ids is not None
            else set()
        )

        while True:

            if (
                max_pages is not None
                and processed_pages >= max_pages
            ):

                break

            items = await asyncio.to_thread(
                self.fetch_news_list,
                code,
                page=current_page,
            )

            if not items:

                break

            new_count = 0

            for item in items:

                if (
                    normalized_mode == "incremental"
                    and item.source_id in known_source_ids
                ):

                    continue

                try:

                    article = await asyncio.to_thread(
                        self.fetch_article,
                        item,
                    )

                except NaverNewsParseError as exc:

                    self.detail_errors.append(
                        {
                            "code": code,
                            "source_id": (
                                item.source_id
                            ),
                            "canonical_url": (
                                item.canonical_url
                            ),
                            "error": str(
                                exc
                            ),
                        }
                    )

                    known_source_ids.add(
                        item.source_id
                    )

                    continue

                if not article.get(
                    "code"
                ):

                    article[
                        "code"
                    ] = code

                if article.get(
                    "page"
                ) is None:

                    article[
                        "page"
                    ] = current_page

                known_source_ids.add(
                    item.source_id
                )

                new_count += 1

                yield article

            if normalized_mode == "incremental":

                if new_count == 0:

                    repeat_pages += 1

                else:

                    repeat_pages = 0

                if (
                    repeat_pages
                    >= self.stop_repeat_pages
                ):

                    break

            processed_pages += 1

            current_page += 1


    async def crawl_relation_pages(
        self,
        code: Any,
        *,
        start_page: int = 1,
        max_pages: int | None = None,
    ) -> AsyncIterator[
        dict[
            str,
            Any,
        ]
    ]:

        code = normalize_naver_stock_code(
            code
        )

        start_page = int(
            start_page
        )

        if start_page <= 0:

            raise ValueError(
                "start_page must be positive"
            )

        if max_pages is not None:

            max_pages = int(
                max_pages
            )

            if max_pages <= 0:

                raise ValueError(
                    "max_pages must be positive"
                )

        current_page = start_page

        processed_pages = 0

        while True:

            if (
                max_pages is not None
                and processed_pages >= max_pages
            ):

                break

            items = await asyncio.to_thread(
                self.fetch_news_list,
                code,
                page=current_page,
            )

            if not items:

                break

            for item in items:

                yield item.to_raw()

            processed_pages += 1

            current_page += 1


    def _get_text(
        self,
        url: str,
        *,
        params: dict[
            str,
            Any,
        ] | None = None,
        referer: str | None = None,
    ) -> str:

        headers = {
            "User-Agent": random.choice(
                USER_AGENTS
            ),
            "Accept": (
                "text/html,application/xhtml+xml,"
                "application/xml;q=0.9,*/*;q=0.8"
            ),
            "Accept-Language": (
                "ko-KR,ko;q=0.9"
            ),
            "Connection": "keep-alive",
        }

        if referer:

            headers[
                "Referer"
            ] = referer

        last_error: Exception | None = None

        for attempt in range(
            self.retries
        ):

            try:

                if self.rate_limiter is not None:

                    self.rate_limiter.before_request(
                        url
                    )

                response = self.session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.timeout_seconds,
                    verify=self.verify_ssl,
                )

                status_code = int(
                    getattr(
                        response,
                        "status_code",
                        0,
                    )
                )

                if status_code == 429:

                    if self.rate_limiter is not None:

                        self.rate_limiter.record_failure(
                            url,
                            throttled=True,
                        )

                    last_error = NaverNewsHTTPError(
                        f"Naver rate limited {url}"
                    )

                    self._sleep(
                        attempt=attempt,
                    )

                    continue

                if status_code >= 400:

                    if self.rate_limiter is not None:

                        self.rate_limiter.record_failure(
                            url,
                            throttled=(
                                status_code
                                == 403
                            ),
                        )

                    raise NaverNewsHTTPError(
                        f"Naver HTTP {status_code} for {url}"
                    )

                if hasattr(
                    response,
                    "raise_for_status",
                ):

                    response.raise_for_status()

                response_url = str(
                    getattr(
                        response,
                        "url",
                        url,
                    )
                )

                if "n.news.naver.com" in response_url:

                    response.encoding = "utf-8"

                elif "finance.naver.com" in response_url:

                    response.encoding = "euc-kr"

                elif not getattr(
                    response,
                    "encoding",
                    None,
                ):

                    response.encoding = getattr(
                        response,
                        "apparent_encoding",
                        "utf-8",
                    )

                if self.rate_limiter is not None:

                    self.rate_limiter.record_success(
                        url
                    )

                return str(
                    response.text
                )

            except Exception as exc:

                if self.rate_limiter is not None:

                    if not isinstance(
                        exc,
                        NaverNewsHTTPError,
                    ):

                        self.rate_limiter.record_failure(
                            url
                        )

                last_error = exc

                if (
                    attempt
                    + 1
                    >= self.retries
                ):

                    break

                self._sleep(
                    attempt=attempt,
                )

        raise NaverNewsHTTPError(
            f"Naver request failed for {url}: "
            f"{last_error}"
        )


    def _sleep(
        self,
        *,
        attempt: int = 0,
    ) -> None:

        delay = (
            self.request_delay_seconds
            * max(
                1,
                attempt + 1,
            )
        )

        if delay > 0:

            time.sleep(
                delay
            )
