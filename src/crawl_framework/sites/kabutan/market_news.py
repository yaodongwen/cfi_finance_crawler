from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Awaitable, Callable
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup

from crawl_framework.transports.http import HttpRequest, HttpTransport


BASE_URL = "https://kabutan.jp"
LIST_URL = "https://kabutan.jp/news/marketnews/"
NEWS_ID_RE = re.compile(r"(n\d{12})")
HISTORY_START_MONTH = "2013-09"
KABUTAN_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/140.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja-JP,ja;q=0.9,en;q=0.6",
    "Referer": LIST_URL,
}


@dataclass(frozen=True, slots=True)
class KabutanListItem:
    news_id: str
    title: str
    category: str
    list_time: str
    url: str

    def to_raw(self) -> dict[str, str]:
        return {
            "news_id": self.news_id,
            "title": self.title,
            "category": self.category,
            "list_time": self.list_time,
            "url": self.url,
        }


@dataclass(frozen=True, slots=True)
class KabutanArticle:
    news_id: str
    title: str
    category: str
    published_at: str | None
    content: str
    url: str
    list_time: str
    article_found: bool
    body_found: bool

    def to_raw(self) -> dict[str, Any]:
        return {
            "news_id": self.news_id,
            "title": self.title,
            "category": self.category,
            "published_at": self.published_at,
            "content": self.content,
            "url": self.url,
            "list_time": self.list_time,
            "article_found": self.article_found,
            "body_found": self.body_found,
        }


@dataclass(frozen=True, slots=True)
class KabutanMonthState:
    year: int
    month: int
    last_completed_page: int
    last_news_id: str | None
    month_complete: bool
    free_access_complete: bool
    stop_reason: str
    archive_access_validated: bool
    access_state: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "year": self.year,
            "month": self.month,
            "last_completed_page": self.last_completed_page,
            "last_news_id": self.last_news_id,
            "month_complete": self.month_complete,
            "free_access_complete": self.free_access_complete,
            "stop_reason": self.stop_reason,
            "archive_access_validated": self.archive_access_validated,
            "access_state": self.access_state,
        }


@dataclass(frozen=True, slots=True)
class KabutanAccessProbeResult:
    timestamp: str
    url: str
    http_status: int
    kabutan_page_detected: bool
    waf_human_verification: bool
    access_state: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "url": self.url,
            "http_status": self.http_status,
            "kabutan_page_detected": self.kabutan_page_detected,
            "waf_human_verification": self.waf_human_verification,
            "access_state": self.access_state,
        }


class KabutanParserError(RuntimeError):
    pass


class KabutanHTTPError(RuntimeError):
    pass


class KabutanWafBlockedError(KabutanHTTPError):
    pass


def is_kabutan_waf_human_verification(status: int, body: str) -> bool:
    text = str(body or "")
    aws_marker = "awsWafCookieDomainList" in text or "gokuProps" in text
    return int(status) == 405 and "Human Verification" in text and aws_marker


def has_kabutan_market_news_dom(html: str) -> bool:
    soup = BeautifulSoup(html or "", "html.parser")
    return soup.select_one("table.s_news_list") is not None


def build_list_request(year: int, month: int, page: int = 1) -> HttpRequest:
    return HttpRequest(
        url=LIST_URL,
        params={
            "category": -1,
            "date": f"{year:04d}{month:02d}00",
            "page": page,
        },
        headers=dict(KABUTAN_HEADERS),
    )


async def probe_kabutan_access(
    transport: HttpTransport,
    *,
    now: datetime | None = None,
) -> KabutanAccessProbeResult:
    observed_at = now or datetime.now(timezone.utc)
    tokyo_now = observed_at.astimezone(ZoneInfo("Asia/Tokyo"))
    request = build_list_request(tokyo_now.year, tokyo_now.month)
    response = await transport.request(request)
    status = int(getattr(response, "status_code", 0) or 0)
    body = str(response.text)
    waf = is_kabutan_waf_human_verification(status, body)
    page_detected = status == 200 and has_kabutan_market_news_dom(body)
    if waf:
        access_state = "WAF_BLOCKED"
    elif page_detected:
        access_state = "AVAILABLE"
    elif status == 200:
        access_state = "UNEXPECTED_PAGE"
    else:
        access_state = "HTTP_ERROR"
    return KabutanAccessProbeResult(
        timestamp=observed_at.astimezone(timezone.utc).isoformat(),
        url=request.url,
        http_status=status,
        kabutan_page_detected=page_detected,
        waf_human_verification=waf,
        access_state=access_state,
    )


ListFetcher = Callable[[int, int, int], Awaitable[str]]
DetailFetcher = Callable[[str], Awaitable[str]]


class KabutanMarketNewsClient:
    def __init__(
        self,
        list_fetcher: ListFetcher,
        detail_fetcher: DetailFetcher,
    ) -> None:
        self.list_fetcher = list_fetcher
        self.detail_fetcher = detail_fetcher
        self.scope_states: dict[str, dict[str, Any]] = {}
        self._list_page_cache: dict[tuple[int, int, int], str] = {}

    @classmethod
    def from_transport(cls, transport: HttpTransport) -> "KabutanMarketNewsClient":
        async def fetch(request: HttpRequest) -> str:
            response = await transport.request(request)
            status = int(getattr(response, "status_code", 0) or 0)
            body = str(response.text)
            if is_kabutan_waf_human_verification(status, body):
                raise KabutanWafBlockedError(
                    f"Kabutan anonymous access blocked by AWS WAF: {request.url}"
                )
            if status != 200:
                raise KabutanHTTPError(f"Kabutan HTTP {status}: {request.url}")
            return body

        async def fetch_list(year: int, month: int, page: int) -> str:
            return await fetch(build_list_request(year, month, page))

        async def fetch_detail(url: str) -> str:
            return await fetch(HttpRequest(url=url, headers=dict(KABUTAN_HEADERS)))

        return cls(fetch_list, fetch_detail)

    async def probe_month(self, year: int, month: int) -> str:
        """Inspect and cache page one for free-full scope discovery."""
        html = await self.list_fetcher(year, month, 1)
        if not isinstance(html, str):
            raise KabutanParserError("Kabutan list fetch did not return HTML text")
        if not has_kabutan_market_news_dom(html):
            raise KabutanParserError("Kabutan Market News DOM was not detected")
        self._list_page_cache[(year, month, 1)] = html
        return "free_access_boundary" if has_locked_news_rows(html) else "public"

    async def _fetch_list_page(self, year: int, month: int, page: int) -> str:
        cached = self._list_page_cache.pop((year, month, page), None)
        if cached is not None:
            return cached
        return await self.list_fetcher(year, month, page)

    async def crawl_month(
        self,
        year: int,
        month: int,
        *,
        start_page: int = 1,
        max_pages: int = 500,
        prior_last_news_id: str | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        if start_page < 1:
            raise ValueError("start_page must be >= 1")
        if max_pages < 1:
            raise ValueError("max_pages must be >= 1")

        scope_key = f"{year:04d}{month:02d}"
        last_completed_page = start_page - 1
        last_news_id = prior_last_news_id
        seen_page_signatures: set[tuple[str, ...]] = set()
        seen_month_ids: set[str] = set()

        def save_state(
            *,
            complete: bool,
            reason: str,
            free_access_complete: bool = False,
            archive_access_validated: bool = False,
            access_state: str = "available",
        ) -> None:
            self.scope_states[scope_key] = KabutanMonthState(
                year=year,
                month=month,
                last_completed_page=last_completed_page,
                last_news_id=last_news_id,
                month_complete=complete,
                free_access_complete=free_access_complete,
                stop_reason=reason,
                archive_access_validated=archive_access_validated,
                access_state=access_state,
            ).to_dict()

        final_page = start_page + max_pages - 1
        for page in range(start_page, final_page + 1):
            try:
                html = await self._fetch_list_page(year, month, page)
            except asyncio.CancelledError:
                save_state(complete=False, reason="interrupted")
                raise
            except KabutanWafBlockedError:
                save_state(
                    complete=False,
                    reason="waf_human_verification",
                    access_state="temporarily_blocked",
                )
                raise
            except Exception:
                save_state(complete=False, reason="http_failure")
                raise
            if not isinstance(html, str):
                save_state(complete=False, reason="parser_failure")
                raise KabutanParserError("Kabutan list fetch did not return HTML text")

            items = parse_list_page(html)
            free_access_boundary = has_locked_news_rows(html)
            if free_access_boundary and not items:
                save_state(
                    complete=False,
                    free_access_complete=True,
                    reason="free_access_boundary",
                )
                return
            if not items:
                save_state(
                    complete=True,
                    reason="empty_page",
                    archive_access_validated=True,
                )
                return

            signature = tuple(item.news_id for item in items)
            if signature in seen_page_signatures:
                save_state(
                    complete=True,
                    reason="repeated_page_signature",
                    archive_access_validated=True,
                )
                return

            current_ids = set(signature)
            if current_ids and current_ids.issubset(seen_month_ids):
                save_state(
                    complete=True,
                    reason="repeated_page_ids",
                    archive_access_validated=True,
                )
                return

            seen_page_signatures.add(signature)
            seen_month_ids.update(current_ids)

            async def fetch_article(item: KabutanListItem) -> KabutanArticle:
                try:
                    detail_html = await self.detail_fetcher(item.url)
                except asyncio.CancelledError:
                    save_state(complete=False, reason="interrupted")
                    raise
                except Exception:
                    save_state(complete=False, reason="http_failure")
                    raise
                if not isinstance(detail_html, str):
                    save_state(complete=False, reason="parser_failure")
                    raise KabutanParserError("Kabutan detail fetch did not return HTML text")
                article = parse_article_page(detail_html, item)
                if not article.article_found or not article.body_found:
                    save_state(complete=False, reason="parser_failure")
                    raise KabutanParserError(
                        f"Kabutan article structure missing for {item.news_id}"
                    )
                return article

            detail_tasks = [
                asyncio.create_task(fetch_article(item))
                for item in items
            ]
            try:
                articles = await asyncio.gather(*detail_tasks)
            except BaseException:
                for task in detail_tasks:
                    task.cancel()
                await asyncio.gather(*detail_tasks, return_exceptions=True)
                raise
            for item, article in zip(items, articles, strict=True):
                last_news_id = item.news_id
                yield {**article.to_raw(), "page": page}

            last_completed_page = page
            if free_access_boundary:
                save_state(
                    complete=False,
                    free_access_complete=True,
                    reason="free_access_boundary",
                )
                return
            save_state(complete=False, reason="page_complete")

        save_state(complete=False, reason="safety_capped")


def clean_text(value: Any) -> str:
    if value is None:
        return ""

    text = str(value).replace("\xa0", " ").replace("\u3000", " ")
    lines = []
    for line in text.splitlines():
        line = re.sub(r"[ \t]+", " ", line).strip()
        if line:
            lines.append(line)
    return "\n".join(lines)


def extract_news_id(url: str | None) -> str | None:
    if not url:
        return None
    match = NEWS_ID_RE.search(str(url))
    return match.group(1) if match else None


def parse_month(value: str) -> tuple[int, int]:
    match = re.fullmatch(r"(\d{4})-(\d{2})", str(value).strip())
    if match is None:
        raise ValueError(f"invalid month: {value!r}")
    year, month = (int(part) for part in match.groups())
    if not 1 <= month <= 12:
        raise ValueError(f"invalid month: {value!r}")
    return year, month


def format_month(year: int, month: int) -> str:
    if year < 1 or not 1 <= month <= 12:
        raise ValueError(f"invalid year/month: {year}-{month}")
    return f"{year:04d}-{month:02d}"


def shift_month(value: str, delta: int) -> str:
    year, month = parse_month(value)
    offset = year * 12 + month - 1 + int(delta)
    if offset < 12:
        raise ValueError("shifted month is before year 1")
    return format_month(offset // 12, offset % 12 + 1)


def iter_months(start_month: str, end_month: str):
    start = parse_month(start_month)
    end = parse_month(end_month)
    if start > end:
        raise ValueError("start month must not be after end month")

    current = start_month
    while True:
        yield current
        if current == end_month:
            return
        current = shift_month(current, 1)


def current_tokyo_month(now: datetime | None = None) -> str:
    if now is None:
        now = datetime.now(ZoneInfo("Asia/Tokyo"))
    elif now.tzinfo is not None:
        now = now.astimezone(ZoneInfo("Asia/Tokyo"))
    return format_month(now.year, now.month)


def resolve_month_window(
    *,
    mode: str = "incremental",
    start_month: str | None = None,
    end_month: str | None = None,
    overlap_months: int = 1,
    now: datetime | None = None,
) -> tuple[str, ...]:
    end = end_month or current_tokyo_month(now)
    parse_month(end)

    if start_month:
        start = start_month
    elif mode in {"full", "free_full"}:
        start = HISTORY_START_MONTH
    elif mode == "incremental":
        if overlap_months < 0:
            raise ValueError("overlap_months must be >= 0")
        start = shift_month(end, -overlap_months)
    else:
        raise ValueError(f"unsupported Kabutan mode: {mode!r}")

    months = tuple(iter_months(start, end))
    if mode in {"full", "free_full"}:
        return tuple(reversed(months))
    return months


def parse_list_page(html: str) -> list[KabutanListItem]:
    soup = BeautifulSoup(html or "", "html.parser")
    unique: dict[str, KabutanListItem] = {}

    for table in soup.select("table.s_news_list"):
        rows = table.select("tbody > tr")
        if not rows:
            rows = table.find_all("tr", recursive=False)
        for row in rows:
            cells = row.find_all("td", recursive=False)
            if not cells:
                continue

            link = next(
                (
                    candidate
                    for candidate in row.select("a[href]")
                    if NEWS_ID_RE.search(str(candidate.get("href", "")))
                ),
                None,
            )
            if link is None:
                continue

            url = urljoin(BASE_URL, str(link.get("href", "")))
            news_id = extract_news_id(url)
            title = clean_text(link.get_text(" ", strip=True))
            if not news_id or not title:
                continue

            time_cell = row.select_one("td.news_time")
            list_time = clean_text(
                time_cell.get_text(" ", strip=True) if time_cell else ""
            )

            category = ""
            if len(cells) >= 2:
                candidate = clean_text(cells[1].get_text(" ", strip=True))
                if candidate and len(candidate) <= 10 and candidate != title:
                    category = candidate

            unique[news_id] = KabutanListItem(
                news_id=news_id,
                title=title,
                category=category,
                list_time=list_time,
                url=url,
            )

    return list(unique.values())


def has_locked_news_rows(html: str) -> bool:
    """Return true when Kabutan rendered news metadata without article links."""
    soup = BeautifulSoup(html or "", "html.parser")
    for table in soup.select("table.s_news_list"):
        for row in table.select("tr"):
            if row.select_one("td.news_time") is None:
                continue
            if any(
                NEWS_ID_RE.search(str(link.get("href", "")))
                for link in row.select("a[href]")
            ):
                continue
            if clean_text(row.get_text(" ", strip=True)):
                return True
    return False


def parse_article_page(
    html: str,
    fallback: KabutanListItem | dict[str, Any] | None = None,
) -> KabutanArticle:
    if isinstance(fallback, KabutanListItem):
        base = fallback.to_raw()
    else:
        base = dict(fallback or {})

    soup = BeautifulSoup(html or "", "html.parser")
    article = soup.select_one("article")
    if article is None:
        return KabutanArticle(
            news_id=str(base.get("news_id", "")),
            title=clean_text(base.get("title")),
            category=clean_text(base.get("category")),
            published_at=None,
            content="",
            url=str(base.get("url", "")),
            list_time=clean_text(base.get("list_time")),
            article_found=False,
            body_found=False,
        )

    title = clean_text(base.get("title"))
    heading = article.select_one("h1")
    if heading:
        parsed_title = clean_text(heading.get_text(" ", strip=True))
        if parsed_title:
            title = parsed_title

    published_at = None
    time_tag = article.select_one("time.s_news_date")
    if time_tag is None:
        time_tag = article.select_one("time[datetime]")
    if time_tag is not None:
        candidate = clean_text(time_tag.get("datetime"))
        try:
            datetime.fromisoformat(candidate)
        except (TypeError, ValueError):
            pass
        else:
            published_at = candidate

    category = clean_text(base.get("category"))
    if not category:
        for selector in (".news_category", ".category", ".s_news_category"):
            element = article.select_one(selector)
            if element is not None:
                category = clean_text(element.get_text(" ", strip=True))
                if category:
                    break

    body = article.select_one("div.body")
    if body is None:
        body = article.select_one("div.mono")
    content = ""
    if body is not None:
        for unwanted in body.select(
            "script, style, iframe, .ads_box, .ad, .sns, .related"
        ):
            unwanted.decompose()
        content = clean_text(body.get_text("\n", strip=True))

    return KabutanArticle(
        news_id=str(base.get("news_id", "")),
        title=title,
        category=category,
        published_at=published_at,
        content=content,
        url=str(base.get("url", "")),
        list_time=clean_text(base.get("list_time")),
        article_found=True,
        body_found=body is not None,
    )
