from __future__ import annotations

import asyncio
import html
import re
import time

from dataclasses import dataclass
from datetime import date
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

NAVER_RESEARCH_HOME_URL = (
    NAVER_FINANCE_BASE_URL
    + "/research/"
)

RESEARCH_CATEGORIES = {
    "market": {
        "list_path": "/research/market_info_list.naver",
        "has_stock": False,
    },
    "invest": {
        "list_path": "/research/invest_list.naver",
        "has_stock": False,
    },
    "company": {
        "list_path": "/research/company_list.naver",
        "has_stock": True,
    },
    "industry": {
        "list_path": "/research/industry_list.naver",
        "has_stock": False,
    },
    "economy": {
        "list_path": "/research/economy_list.naver",
        "has_stock": False,
    },
    "debenture": {
        "list_path": "/research/debenture_list.naver",
        "has_stock": False,
    },
}

DEFAULT_RESEARCH_CATEGORIES = tuple(
    RESEARCH_CATEGORIES
)

DEFAULT_TIMEOUT_SECONDS = 30

DEFAULT_RETRIES = 4

DEFAULT_REQUEST_DELAY_SECONDS = 0.0

DEFAULT_STOP_EMPTY_PAGES = 2

DEFAULT_INCREMENTAL_STOP_EXISTING_PAGES = 3

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/150.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,"
        "application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": (
        "ko-KR,ko;q=0.9,zh-CN;q=0.8,en;q=0.7"
    ),
    "Connection": "keep-alive",
    "Referer": NAVER_RESEARCH_HOME_URL,
}


class NaverResearchError(
    RuntimeError
):
    """
    Base error for real Naver research crawling.
    """


class NaverResearchHTTPError(
    NaverResearchError
):
    """
    HTTP request failed.
    """


class NaverResearchParseError(
    NaverResearchError
):
    """
    Research HTML could not be parsed.
    """


@dataclass(
    frozen=True,
    slots=True,
)
class ResearchListItem:
    report_type: str
    report_id: str
    title: str
    institution: str
    published_at: str | None
    views: int | None
    detail_url: str
    list_url: str
    list_page: int
    stock_code: str | None = None
    stock_name: str | None = None
    classification: str | None = None
    pdf_hint: bool = False


    def to_raw(
        self,
    ) -> dict[
        str,
        Any,
    ]:

        instrument_ids = []

        if self.stock_code:

            instrument_ids.append(
                f"XKRX:{self.stock_code}"
            )

        return {
            "report_type": self.report_type,
            "category": self.report_type,
            "report_id": self.report_id,
            "title": self.title,
            "institution": self.institution,
            "published_at": self.published_at,
            "views": self.views,
            "source_url": self.detail_url,
            "detail_url": self.detail_url,
            "list_url": self.list_url,
            "page": self.list_page,
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "classification": self.classification,
            "pdf_hint": self.pdf_hint,
            "instrument_ids": instrument_ids,
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
        r"[ \t\r\f\v]+",
        " ",
        text,
    )

    text = re.sub(
        r"\n[ \t]+",
        "\n",
        text,
    )

    text = re.sub(
        r"\n{3,}",
        "\n\n",
        text,
    )

    return text.strip()


def parse_integer(
    value: Any,
) -> int | None:

    if value is None:

        return None

    match = re.search(
        r"(\d[\d,]*)",
        str(
            value
        ),
    )

    if match is None:

        return None

    try:

        return int(
            match.group(
                1
            ).replace(
                ",",
                "",
            )
        )

    except ValueError:

        return None


def normalize_research_date(
    value: Any,
) -> str | None:

    text = clean_text(
        value
    )

    if not text:

        return None

    patterns = (
        r"(?P<year>\d{4})[.\-/]\s*"
        r"(?P<month>\d{1,2})[.\-/]\s*"
        r"(?P<day>\d{1,2})",
        r"(?P<year>\d{4})\s*년\s*"
        r"(?P<month>\d{1,2})\s*월\s*"
        r"(?P<day>\d{1,2})\s*일",
        r"(?<!\d)(?P<year>\d{2})[.]"
        r"(?P<month>\d{1,2})[.]"
        r"(?P<day>\d{1,2})(?!\d)",
    )

    for pattern in patterns:

        match = re.search(
            pattern,
            text,
        )

        if match is None:

            continue

        try:

            year = int(
                match.group(
                    "year"
                )
            )

            if year < 100:

                year += 2000

            parsed = date(
                year,
                int(
                    match.group(
                        "month"
                    )
                ),
                int(
                    match.group(
                        "day"
                    )
                ),
            )

            return parsed.isoformat()

        except ValueError:

            continue

    return None


def research_list_url(
    category: str,
    page: int = 1,
) -> str:

    category = normalize_category(
        category
    )

    page = int(
        page
    )

    if page < 1:

        raise ValueError(
            "page must be positive"
        )

    return (
        f"{NAVER_FINANCE_BASE_URL}"
        f"{RESEARCH_CATEGORIES[category]['list_path']}"
        f"?page={page}"
    )


def normalize_category(
    value: Any,
) -> str:

    category = str(
        value
    ).strip().lower()

    if category not in RESEARCH_CATEGORIES:

        raise ValueError(
            "unsupported Naver research category: "
            f"{category!r}"
        )

    return category


def normalize_categories(
    value: Any,
) -> tuple[
    str,
    ...
]:

    if value is None:

        return DEFAULT_RESEARCH_CATEGORIES

    if isinstance(
        value,
        str,
    ):

        if value.strip().lower() == "all":

            return DEFAULT_RESEARCH_CATEGORIES

        values = (
            value,
        )

    else:

        values = tuple(
            value
        )

    result = []

    seen = set()

    for item in values:

        if str(
            item
        ).strip().lower() == "all":

            for category in DEFAULT_RESEARCH_CATEGORIES:

                if category in seen:

                    continue

                seen.add(
                    category
                )

                result.append(
                    category
                )

            continue

        category = normalize_category(
            item
        )

        if category in seen:

            continue

        seen.add(
            category
        )

        result.append(
            category
        )

    return tuple(
        result
    )


def extract_report_id(
    url: str,
) -> str | None:

    query = parse_qs(
        urlparse(
            url
        ).query
    )

    values = query.get(
        "nid"
    )

    if not values:

        return None

    report_id = str(
        values[0]
    ).strip()

    return report_id or None


def extract_stock_code(
    url: str | None,
) -> str | None:

    if not url:

        return None

    query = parse_qs(
        urlparse(
            url
        ).query
    )

    for key in (
        "code",
        "itemcode",
        "stock_code",
    ):

        values = query.get(
            key
        )

        if not values:

            continue

        code = str(
            values[0]
        ).strip()

        if re.fullmatch(
            r"\d{6}",
            code,
        ):

            return code

    return None


def parse_research_list(
    category: str,
    text: str,
    *,
    list_url: str,
    page: int,
) -> list[
    ResearchListItem
]:

    category = normalize_category(
        category
    )

    soup = BeautifulSoup(
        text,
        "lxml",
    )

    table = None

    for candidate in soup.find_all(
        "table",
        class_="type_1",
    ):

        if candidate.find(
            "a",
            href=re.compile(
                r"_read\.naver\?.*nid="
            ),
        ):

            table = candidate

            break

    if table is None:

        return []

    if category in {
        "industry",
        "company",
    }:

        extra_index = 0
        title_index = 1
        company_index = 2
        file_index = 3
        date_index = 4
        views_index = 5

    else:

        extra_index = None
        title_index = 0
        company_index = 1
        file_index = 2
        date_index = 3
        views_index = 4

    results: list[
        ResearchListItem
    ] = []

    seen_ids: set[
        str
    ] = set()

    for row in table.find_all(
        "tr"
    ):

        columns = row.find_all(
            "td",
            recursive=False,
        )

        if len(
            columns
        ) < views_index + 1:

            continue

        title_link = columns[
            title_index
        ].find(
            "a",
            href=True,
        )

        if title_link is None:

            continue

        href = str(
            title_link.get(
                "href",
                "",
            )
        ).strip()

        if (
            "_read.naver" not in href
            or "nid=" not in href
        ):

            continue

        detail_url = urljoin(
            list_url,
            href,
        )

        report_id = extract_report_id(
            detail_url
        )

        if (
            not report_id
            or report_id in seen_ids
        ):

            continue

        seen_ids.add(
            report_id
        )

        title = clean_text(
            title_link.get_text(
                " ",
                strip=True,
            )
        )

        if not title:

            continue

        stock_code = None
        stock_name = None
        classification = None

        if category == "company":

            assert extra_index is not None

            stock_cell = columns[
                extra_index
            ]

            stock_name = clean_text(
                stock_cell.get_text(
                    " ",
                    strip=True,
                )
            ) or None

            stock_link = stock_cell.find(
                "a",
                href=True,
            )

            if stock_link is not None:

                stock_code = extract_stock_code(
                    str(
                        stock_link.get(
                            "href",
                            "",
                        )
                    )
                )

        elif category == "industry":

            assert extra_index is not None

            classification = clean_text(
                columns[
                    extra_index
                ].get_text(
                    " ",
                    strip=True,
                )
            ) or None

        file_cell = columns[
            file_index
        ]

        pdf_hint = bool(
            file_cell.find(
                "a",
                href=re.compile(
                    r"(\.pdf|stock-research)",
                    re.IGNORECASE,
                ),
            )
            or file_cell.find(
                "img",
                alt=re.compile(
                    "pdf",
                    re.IGNORECASE,
                ),
            )
        )

        results.append(
            ResearchListItem(
                report_type=category,
                report_id=report_id,
                title=title,
                institution=clean_text(
                    columns[
                        company_index
                    ].get_text(
                        " ",
                        strip=True,
                    )
                ),
                published_at=normalize_research_date(
                    columns[
                        date_index
                    ].get_text(
                        " ",
                        strip=True,
                    )
                ),
                views=parse_integer(
                    columns[
                        views_index
                    ].get_text(
                        " ",
                        strip=True,
                    )
                ),
                detail_url=detail_url,
                list_url=list_url,
                list_page=page,
                stock_code=stock_code,
                stock_name=stock_name,
                classification=classification,
                pdf_hint=pdf_hint,
            )
        )

    return results


def parse_research_detail(
    category: str,
    report_id: str,
    text: str,
    *,
    detail_url: str,
    list_item: ResearchListItem | None = None,
) -> dict[
    str,
    Any,
]:

    category = normalize_category(
        category
    )

    soup = BeautifulSoup(
        text,
        "lxml",
    )

    base = (
        list_item.to_raw()
        if list_item is not None
        else {
            "report_type": category,
            "category": category,
            "report_id": report_id,
            "source_url": detail_url,
            "detail_url": detail_url,
            "instrument_ids": [],
        }
    )

    title = base.get(
        "title"
    ) or _detail_title(
        soup
    )

    source_meta = _source_metadata(
        soup
    )

    content = _detail_content(
        soup
    )

    pdf_url = _pdf_url(
        soup
    )

    investment_opinion = None

    target_price_text = None

    target_price = None

    if category == "company":

        investment_opinion = _field_value(
            soup,
            (
                "투자의견",
                "투자 의견",
            ),
        )

        target_price_text = _field_value(
            soup,
            (
                "목표가",
                "목표주가",
            ),
        )

        target_price = parse_integer(
            target_price_text
        )

    institution = (
        source_meta.get(
            "institution"
        )
        or base.get(
            "institution"
        )
    )

    published_at = (
        source_meta.get(
            "published_at"
        )
        or base.get(
            "published_at"
        )
    )

    views = (
        source_meta.get(
            "views"
        )
        if source_meta.get(
            "views"
        )
        is not None
        else base.get(
            "views"
        )
    )

    raw = dict(
        base
    )

    raw.update(
        {
            "report_type": category,
            "category": category,
            "report_id": report_id,
            "title": title,
            "institution": institution,
            "securities_company": institution,
            "published_at": published_at,
            "views": views,
            "summary": content,
            "abstract": content,
            "content": content,
            "source_url": detail_url,
            "detail_url": detail_url,
            "pdf_url": pdf_url,
            "pdf_filename": (
                f"{category}-{report_id}.pdf"
                if pdf_url
                else None
            ),
            "investment_opinion": (
                investment_opinion
            ),
            "target_price": target_price,
            "target_price_text": (
                target_price_text
            ),
        }
    )

    return raw


def _detail_title(
    soup: BeautifulSoup,
) -> str:

    for selector in (
        "th.view_sbj",
        "h3.sub_tlt",
        "div.box_type_m h3",
        "h3",
    ):

        node = soup.select_one(
            selector
        )

        if node is None:

            continue

        for source_node in node.select(
            "p.source, .source, .view_report"
        ):

            source_node.decompose()

        title = clean_text(
            node.get_text(
                " ",
                strip=True,
            )
        )

        if title:

            return title

    return ""


def _source_metadata(
    soup: BeautifulSoup,
) -> dict[
    str,
    Any,
]:

    node = (
        soup.select_one(
            "p.source"
        )
        or soup.select_one(
            ".source"
        )
    )

    if node is None:

        return {
            "institution": None,
            "published_at": None,
            "views": None,
        }

    text = clean_text(
        node.get_text(
            " ",
            strip=True,
        )
    )

    parts = [
        clean_text(
            part
        )
        for part in re.split(
            r"\s*[|｜]\s*",
            text,
        )
        if clean_text(
            part
        )
    ]

    institution = None

    for part in parts:

        if normalize_research_date(
            part
        ):

            continue

        if re.search(
            r"조회|views?",
            part,
            flags=re.IGNORECASE,
        ):

            continue

        institution = part

        break

    views = None

    match = re.search(
        r"(?:조회(?:수)?|views?)\s*[:：]?\s*([\d,]+)",
        text,
        flags=re.IGNORECASE,
    )

    if match:

        views = parse_integer(
            match.group(
                1
            )
        )

    return {
        "institution": institution,
        "published_at": normalize_research_date(
            text
        ),
        "views": views,
    }


def _detail_content(
    soup: BeautifulSoup,
) -> str:

    node = (
        soup.select_one(
            "td.view_cnt"
        )
        or soup.select_one(
            "div.view_cnt"
        )
    )

    if node is None:

        return ""

    copied = BeautifulSoup(
        str(
            node
        ),
        "lxml",
    )

    cleaned = (
        copied.select_one(
            "td.view_cnt"
        )
        or copied.select_one(
            "div.view_cnt"
        )
        or copied.body
    )

    if cleaned is None:

        return ""

    for selector in (
        "script",
        "style",
        "noscript",
        "iframe",
        "form",
        "button",
        "th.view_report",
    ):

        for child in cleaned.select(
            selector
        ):

            child.decompose()

    for br in cleaned.find_all(
        "br"
    ):

        br.replace_with(
            "\n"
        )

    for paragraph in cleaned.find_all(
        [
            "p",
            "div",
            "li",
            "tr",
        ]
    ):

        paragraph.append(
            "\n"
        )

    return clean_text(
        cleaned.get_text(
            "\n",
            strip=False,
        )
    )


def _pdf_url(
    soup: BeautifulSoup,
) -> str | None:

    for selector in (
        "th.view_report a[href]",
        "a[href$='.pdf']",
        "a[href*='.pdf?']",
        "a[href*='stock-research']",
    ):

        for anchor in soup.select(
            selector
        ):

            href = anchor.get(
                "href"
            )

            if not href:

                continue

            absolute = urljoin(
                NAVER_FINANCE_BASE_URL,
                str(
                    href
                ),
            )

            lowered = absolute.lower()

            if (
                ".pdf" in lowered
                or "stock-research" in lowered
            ):

                return absolute

    return None


def _field_value(
    soup: BeautifulSoup,
    labels: tuple[
        str,
        ...
    ],
) -> str | None:

    normalized_labels = {
        clean_text(
            label
        )
        for label in labels
    }

    for label_node in soup.find_all(
        [
            "th",
            "td",
        ]
    ):

        label_text = clean_text(
            label_node.get_text(
                " ",
                strip=True,
            )
        )

        if not any(
            label_text == label
            or label_text.startswith(
                label
            )
            for label in normalized_labels
        ):

            continue

        sibling = label_node.find_next_sibling(
            "td"
        ) or label_node.find_next_sibling()

        if sibling is None:

            continue

        value = clean_text(
            sibling.get_text(
                " ",
                strip=True,
            )
        )

        if value:

            return value

    for dt in soup.find_all(
        "dt"
    ):

        label_text = clean_text(
            dt.get_text(
                " ",
                strip=True,
            )
        )

        if label_text not in normalized_labels:

            continue

        dd = dt.find_next_sibling(
            "dd"
        )

        if dd is None:

            continue

        value = clean_text(
            dd.get_text(
                " ",
                strip=True,
            )
        )

        if value:

            return value

    return None


class NaverResearchClient:
    """
    Real HTTP client for Naver Finance research reports.
    """

    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        retries: int = DEFAULT_RETRIES,
        request_delay_seconds: float = DEFAULT_REQUEST_DELAY_SECONDS,
        stop_empty_pages: int = DEFAULT_STOP_EMPTY_PAGES,
        verify_ssl: bool = True,
        rate_limiter=None,
    ) -> None:

        if timeout_seconds <= 0:

            raise ValueError(
                "timeout_seconds must be positive"
            )

        if retries <= 0:

            raise ValueError(
                "retries must be positive"
            )

        self.session = (
            session
            or self.create_session()
        )

        self.timeout_seconds = timeout_seconds

        self.retries = retries

        self.request_delay_seconds = (
            request_delay_seconds
        )

        self.stop_empty_pages = int(
            stop_empty_pages
        )

        if self.stop_empty_pages <= 0:

            raise ValueError(
                "stop_empty_pages must be positive"
            )

        self.verify_ssl = verify_ssl

        self.rate_limiter = rate_limiter


    @staticmethod
    def create_session(
    ) -> requests.Session:

        session = requests.Session()

        session.headers.update(
            DEFAULT_HEADERS
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


    def fetch_list(
        self,
        category: str,
        *,
        page: int,
    ) -> list[
        ResearchListItem
    ]:

        category = normalize_category(
            category
        )

        url = research_list_url(
            category,
            page,
        )

        text = self._get_text(
            url,
            referer=NAVER_RESEARCH_HOME_URL,
        )

        return parse_research_list(
            category,
            text,
            list_url=url,
            page=page,
        )


    def fetch_detail(
        self,
        item: ResearchListItem,
    ) -> dict[
        str,
        Any,
    ]:

        text = self._get_text(
            item.detail_url,
            referer=item.list_url,
        )

        return parse_research_detail(
            item.report_type,
            item.report_id,
            text,
            detail_url=item.detail_url,
            list_item=item,
        )


    async def crawl_pages(
        self,
        *,
        categories: tuple[
            str,
            ...
        ] | str | None = None,
        start_page: int = 1,
        max_pages: int | None = None,
        mode: str = "incremental",
        seen_report_ids: set[
            str
        ] | None = None,
    ) -> AsyncIterator[
        dict[
            str,
            Any,
        ]
    ]:

        selected_categories = normalize_categories(
            categories
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

        known_report_ids = (
            seen_report_ids
            if seen_report_ids is not None
            else set()
        )

        for category in selected_categories:

            current_page = start_page

            processed_pages = 0

            empty_pages = 0

            existing_pages = 0

            while True:

                if (
                    max_pages is not None
                    and processed_pages >= max_pages
                ):

                    break

                items = await asyncio.to_thread(
                    self.fetch_list,
                    category,
                    page=current_page,
                )

                if not items:

                    empty_pages += 1

                    if (
                        empty_pages
                        >= self.stop_empty_pages
                    ):

                        break

                    current_page += 1

                    processed_pages += 1

                    continue

                empty_pages = 0

                new_count = 0

                for item in items:

                    if (
                        normalized_mode
                        == "incremental"
                        and item.report_id
                        in known_report_ids
                    ):

                        continue

                    raw = await asyncio.to_thread(
                        self.fetch_detail,
                        item,
                    )

                    known_report_ids.add(
                        item.report_id
                    )

                    new_count += 1

                    yield raw

                if normalized_mode == "incremental":

                    if new_count == 0:

                        existing_pages += 1

                    else:

                        existing_pages = 0

                    if (
                        existing_pages
                        >= DEFAULT_INCREMENTAL_STOP_EXISTING_PAGES
                    ):

                        break

                current_page += 1

                processed_pages += 1


    async def crawl_relation_pages(
        self,
        *,
        categories: tuple[
            str,
            ...
        ] | str | None = None,
        start_page: int = 1,
        max_pages: int | None = None,
        mode: str = "incremental",
        seen_report_ids: set[
            str
        ] | None = None,
    ) -> AsyncIterator[
        dict[
            str,
            Any,
        ]
    ]:

        async for raw in self.crawl_pages(
            categories=categories,
            start_page=start_page,
            max_pages=max_pages,
            mode=mode,
            seen_report_ids=seen_report_ids,
        ):

            instrument_ids = (
                raw.get(
                    "instrument_ids"
                )
                or []
            )

            for instrument_id in instrument_ids:

                instrument_id = str(
                    instrument_id
                ).strip()

                if not instrument_id:

                    continue

                relation = dict(
                    raw
                )

                relation[
                    "instrument_id"
                ] = instrument_id

                yield relation


    def _get_text(
        self,
        url: str,
        *,
        referer: str | None = None,
    ) -> str:

        headers = {}

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

                if status_code in {
                    429,
                    403,
                    500,
                    502,
                    503,
                    504,
                }:

                    if self.rate_limiter is not None:

                        self.rate_limiter.record_failure(
                            url,
                            throttled=(
                                status_code
                                in {
                                    403,
                                    429,
                                }
                            ),
                        )

                    last_error = NaverResearchHTTPError(
                        f"Naver research HTTP "
                        f"{status_code} for {url}"
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

                    raise NaverResearchHTTPError(
                        f"Naver research HTTP "
                        f"{status_code} for {url}"
                    )

                if hasattr(
                    response,
                    "raise_for_status",
                ):

                    response.raise_for_status()

                if self.rate_limiter is not None:

                    self.rate_limiter.record_success(
                        url
                    )

                return decode_naver_research_html(
                    response
                )

            except Exception as exc:

                if self.rate_limiter is not None:

                    if not isinstance(
                        exc,
                        NaverResearchHTTPError,
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

        raise NaverResearchHTTPError(
            f"Naver research request failed "
            f"for {url}: {last_error}"
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


def decode_naver_research_html(
    response,
) -> str:

    content = getattr(
        response,
        "content",
        None,
    )

    if content is None:

        return str(
            response.text
        )

    encodings = []

    if getattr(
        response,
        "encoding",
        None,
    ):

        encodings.append(
            response.encoding
        )

    if getattr(
        response,
        "apparent_encoding",
        None,
    ):

        encodings.append(
            response.apparent_encoding
        )

    encodings.extend(
        [
            "euc-kr",
            "cp949",
            "utf-8",
        ]
    )

    tested = set()

    for encoding in encodings:

        normalized = str(
            encoding
        ).lower()

        if normalized in tested:

            continue

        tested.add(
            normalized
        )

        try:

            text = content.decode(
                str(
                    encoding
                )
            )

        except (
            UnicodeDecodeError,
            LookupError,
        ):

            continue

        if (
            "<html" in text.lower()
            or "<table" in text.lower()
            or "<body" in text.lower()
        ):

            return text

    return content.decode(
        "utf-8",
        errors="replace",
    )
