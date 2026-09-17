from __future__ import annotations

import json
import hashlib
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Awaitable, Callable
from urllib.parse import urljoin, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup
from crawl_framework.transports.http import HttpRequest, HttpTransport


HKEXNEWS_BASE_URL = "https://www1.hkexnews.hk"
HKEXNEWS_PREFIX_URL = f"{HKEXNEWS_BASE_URL}/search/prefix.do"
HKEXNEWS_TITLE_SEARCH_URL = (
    f"{HKEXNEWS_BASE_URL}/search/titlesearch.xhtml?lang=en"
)
HKEX_REPORT_TYPES = {
    "annual": "40100",
    "interim": "40200",
    "quarterly": "40300",
}
HKEX_REPORT_TYPE_CODES = {
    code: name for name, code in HKEX_REPORT_TYPES.items()
}
HKEXNEWS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/140.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9,zh-HK;q=0.7",
}
_JSONP_RE = re.compile(
    r"^[A-Za-z_$][\w$]*\s*\(\s*(\{.*\})\s*\)\s*;?\s*$",
    re.DOTALL,
)
_RELEASE_TIME_PATTERNS = (
    re.compile(
        r"(?P<year>\d{4})\s*年\s*(?P<month>\d{1,2})\s*月\s*"
        r"(?P<day>\d{1,2})\s*日\s+(?P<hour>\d{1,2}):(?P<minute>\d{2})"
    ),
    re.compile(
        r"(?P<day>\d{1,2})/(?P<month>\d{1,2})/(?P<year>\d{4})\s+"
        r"(?P<hour>\d{1,2}):(?P<minute>\d{2})"
    ),
    re.compile(
        r"(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})"
        r"(?:\s+|T?)(?P<hour>\d{2}):?(?P<minute>\d{2})"
    ),
)


class HKEXNewsHTTPError(RuntimeError):
    pass


class HKEXNewsParserError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class HKEXStockInfo:
    stock_id: int
    code: str
    name: str


@dataclass(frozen=True, slots=True)
class HKEXReportListItem:
    release_time_raw: str
    stock_code: str
    stock_name: str
    title: str
    pdf_url: str
    report_type: str
    report_type_code: str

    def to_raw(self) -> dict[str, str]:
        return {
            "release_time_raw": self.release_time_raw,
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "title": self.title,
            "pdf_url": self.pdf_url,
            "report_type": self.report_type,
            "report_type_code": self.report_type_code,
        }


def normalize_hkex_stock_code(value: Any) -> str:
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    if not text.isdigit() or len(text) > 5:
        raise ValueError(f"invalid HKEX stock code: {value!r}")
    return text.zfill(5)


def canonical_hkex_instrument_id(value: Any) -> str:
    return f"XHKG:{normalize_hkex_stock_code(value)}"


def parse_hkex_stock_codes(value: Any) -> tuple[str, ...]:
    codes: list[str] = []
    for token in re.findall(r"(?<!\d)\d{1,5}(?!\d)", str(value)):
        code = normalize_hkex_stock_code(token)
        if code not in codes:
            codes.append(code)
    if not codes:
        raise ValueError(f"invalid HKEX stock code field: {value!r}")
    return tuple(codes)


def normalize_hkex_report_type(value: Any) -> tuple[str, str]:
    text = str(value).strip().lower()
    if text in HKEX_REPORT_TYPES:
        return text, HKEX_REPORT_TYPES[text]
    if text in HKEX_REPORT_TYPE_CODES:
        return HKEX_REPORT_TYPE_CODES[text], text
    raise ValueError(f"unsupported HKEX report type: {value!r}")


def normalize_hkex_search_date(value: date | datetime | str) -> str:
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    text = str(value).strip().replace("-", "")
    try:
        parsed = datetime.strptime(text, "%Y%m%d")
    except ValueError as exc:
        raise ValueError(f"invalid HKEX search date: {value!r}") from exc
    return parsed.strftime("%Y%m%d")


def clean_hkex_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def strip_hkex_field_label(value: Any, label: str) -> str:
    text = clean_hkex_text(value)
    return re.sub(
        rf"^{re.escape(label)}\s*:\s*",
        "",
        text,
        count=1,
        flags=re.IGNORECASE,
    ).strip()


def parse_hkex_release_time(value: Any) -> datetime | None:
    raw = clean_hkex_text(value)
    if not raw:
        return None
    for pattern in _RELEASE_TIME_PATTERNS:
        match = pattern.search(raw)
        if match is None:
            continue
        parts = {name: int(number) for name, number in match.groupdict().items()}
        try:
            local = datetime(
                parts["year"],
                parts["month"],
                parts["day"],
                parts["hour"],
                parts["minute"],
                tzinfo=ZoneInfo("Asia/Hong_Kong"),
            )
        except ValueError:
            return None
        return local.astimezone(timezone.utc)
    return None


def canonicalize_hkex_pdf_url(value: str) -> str:
    absolute = urljoin(HKEXNEWS_BASE_URL, clean_hkex_text(value))
    parsed = urlsplit(absolute)
    if parsed.scheme.lower() not in {"http", "https"}:
        raise ValueError(f"invalid HKEX PDF URL: {value!r}")
    if not parsed.path.lower().endswith(".pdf"):
        raise ValueError(f"HKEX report URL is not a PDF: {value!r}")
    host = (parsed.hostname or "").lower()
    if host not in {"hkexnews.hk", "www.hkexnews.hk", "www1.hkexnews.hk"}:
        raise ValueError(f"unexpected HKEX PDF host: {host!r}")
    netloc = host
    if parsed.port is not None:
        netloc = f"{host}:{parsed.port}"
    return urlunsplit((parsed.scheme.lower(), netloc, parsed.path, parsed.query, ""))


def build_hkex_report_source_id(pdf_url: str) -> str:
    canonical_url = canonicalize_hkex_pdf_url(pdf_url)
    return hashlib.sha256(canonical_url.encode("utf-8")).hexdigest()


def parse_report_results(
    html: str,
    report_type: str,
) -> list[HKEXReportListItem]:
    type_name, type_code = normalize_hkex_report_type(report_type)
    soup = BeautifulSoup(html or "", "html.parser")
    reports: list[HKEXReportListItem] = []
    seen_urls: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        href = clean_hkex_text(anchor.get("href"))
        if ".pdf" not in href.lower():
            continue
        row = anchor.find_parent("tr")
        if row is None:
            continue
        cells = row.find_all("td")
        if len(cells) < 4:
            continue
        try:
            pdf_url = canonicalize_hkex_pdf_url(href)
        except ValueError:
            continue
        if pdf_url in seen_urls:
            continue

        stock_code_raw = strip_hkex_field_label(
            cells[1].get_text(" ", strip=True), "Stock Code"
        )
        try:
            stock_code = normalize_hkex_stock_code(stock_code_raw)
        except ValueError:
            stock_code = stock_code_raw
        title = strip_hkex_field_label(
            cells[-1].get_text(" ", strip=True), "Document"
        )
        if not title:
            title = clean_hkex_text(anchor.get_text(" ", strip=True))
        if not title:
            continue

        seen_urls.add(pdf_url)
        reports.append(
            HKEXReportListItem(
                release_time_raw=strip_hkex_field_label(
                    cells[0].get_text(" ", strip=True), "Release Time"
                ),
                stock_code=stock_code,
                stock_name=strip_hkex_field_label(
                    cells[2].get_text(" ", strip=True), "Stock Short Name"
                ),
                title=title,
                pdf_url=pdf_url,
                report_type=type_name,
                report_type_code=type_code,
            )
        )
    return reports


def parse_jsonp(text: str) -> dict[str, Any]:
    match = _JSONP_RE.match(str(text).strip())
    if match is None:
        raise HKEXNewsParserError("invalid HKEXnews JSONP response")
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        raise HKEXNewsParserError("invalid HKEXnews JSONP payload") from exc
    if not isinstance(payload, dict):
        raise HKEXNewsParserError("HKEXnews JSONP payload must be an object")
    return payload


def select_exact_stock_info(
    payload: dict[str, Any],
    stock_code: Any,
) -> HKEXStockInfo | None:
    expected = normalize_hkex_stock_code(stock_code)
    candidates = payload.get("stockInfo") or []
    if not isinstance(candidates, list):
        raise HKEXNewsParserError("HKEXnews stockInfo must be a list")

    for item in candidates:
        if not isinstance(item, dict):
            continue
        try:
            candidate_code = normalize_hkex_stock_code(item.get("code", ""))
        except ValueError:
            continue
        if candidate_code != expected:
            continue
        try:
            stock_id = int(item["stockId"])
        except (KeyError, TypeError, ValueError) as exc:
            raise HKEXNewsParserError(
                f"exact HKEXnews match {expected} has invalid stockId"
            ) from exc
        return HKEXStockInfo(
            stock_id=stock_id,
            code=candidate_code,
            name=str(item.get("name", "")).strip(),
        )
    return None


StockLookupFetcher = Callable[[HttpRequest], Awaitable[str]]


class HKEXFilingsClient:
    def __init__(
        self,
        stock_lookup_fetcher: StockLookupFetcher,
        *,
        timestamp_ms: Callable[[], int] | None = None,
    ) -> None:
        self.stock_lookup_fetcher = stock_lookup_fetcher
        self.timestamp_ms = timestamp_ms or (lambda: int(time.time() * 1000))

    @classmethod
    def from_transport(cls, transport: HttpTransport) -> "HKEXFilingsClient":
        async def fetch(request: HttpRequest) -> str:
            response = await transport.request(request)
            status = int(getattr(response, "status_code", 0) or 0)
            if status != 200:
                raise HKEXNewsHTTPError(
                    f"HKEXnews HTTP {status}: {request.url}"
                )
            return str(response.text)

        return cls(fetch)

    def build_stock_lookup_request(self, stock_code: Any) -> HttpRequest:
        code = normalize_hkex_stock_code(stock_code)
        return HttpRequest(
            url=HKEXNEWS_PREFIX_URL,
            params={
                "callback": "callback",
                "lang": "EN",
                "type": "A",
                "name": code,
                "market": "SEHK",
                "_": str(self.timestamp_ms()),
            },
            headers=dict(HKEXNEWS_HEADERS),
        )

    async def resolve_stock(self, stock_code: Any) -> HKEXStockInfo | None:
        request = self.build_stock_lookup_request(stock_code)
        response_text = await self.stock_lookup_fetcher(request)
        return select_exact_stock_info(parse_jsonp(response_text), stock_code)

    def build_report_search_request(
        self,
        stock_id: int,
        report_type: str,
        *,
        date_from: date | datetime | str,
        date_to: date | datetime | str,
    ) -> HttpRequest:
        _, type_code = normalize_hkex_report_type(report_type)
        start = normalize_hkex_search_date(date_from)
        end = normalize_hkex_search_date(date_to)
        if start > end:
            raise ValueError("HKEX report date_from must not be after date_to")
        if int(stock_id) < 1:
            raise ValueError("HKEX stock_id must be positive")
        return HttpRequest(
            url=HKEXNEWS_TITLE_SEARCH_URL,
            method="POST",
            data={
                "lang": "EN",
                "category": "0",
                "market": "SEHK",
                "searchType": "1",
                "documentType": "-1",
                "t1code": "40000",
                "t2Gcode": "-2",
                "t2code": type_code,
                "stockId": str(int(stock_id)),
                "from": start,
                "to": end,
                "MB-Daterange": "0",
                "title": "",
            },
            headers={
                **HKEXNEWS_HEADERS,
                "Referer": HKEXNEWS_TITLE_SEARCH_URL,
            },
        )

    async def search_report_type(
        self,
        stock_id: int,
        report_type: str,
        *,
        date_from: date | datetime | str,
        date_to: date | datetime | str,
    ) -> str:
        request = self.build_report_search_request(
            stock_id,
            report_type,
            date_from=date_from,
            date_to=date_to,
        )
        return await self.stock_lookup_fetcher(request)

    async def search_report_types(
        self,
        stock_id: int,
        report_types: tuple[str, ...] | list[str],
        *,
        date_from: date | datetime | str,
        date_to: date | datetime | str,
    ) -> dict[str, str]:
        results: dict[str, str] = {}
        for value in report_types:
            name, _ = normalize_hkex_report_type(value)
            if name in results:
                continue
            results[name] = await self.search_report_type(
                stock_id,
                name,
                date_from=date_from,
                date_to=date_to,
            )
        return results

    async def search_reports(
        self,
        stock_id: int,
        report_types: tuple[str, ...] | list[str],
        *,
        date_from: date | datetime | str,
        date_to: date | datetime | str,
    ) -> list[HKEXReportListItem]:
        pages = await self.search_report_types(
            stock_id,
            report_types,
            date_from=date_from,
            date_to=date_to,
        )
        reports: list[HKEXReportListItem] = []
        seen_urls: set[str] = set()
        for report_type, html in pages.items():
            for report in parse_report_results(html, report_type):
                if report.pdf_url in seen_urls:
                    continue
                seen_urls.add(report.pdf_url)
                reports.append(report)
        return reports
