from __future__ import annotations

import asyncio
import hashlib
import json
import re

from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, unquote, urljoin, urlparse

from crawl_framework.sites.tossinvest.instruments import (
    TOSS_BASE_URL,
    TOSS_SCREENER_URL,
    clean_toss_text,
)
from crawl_framework.transports.playwright import (
    BrowserWorkerPool,
    find_scroll_parent,
    scroll_element_once,
)


def parse_news_id(url: str) -> str:
    try:
        raw = parse_qs(urlparse(url).query).get("contentParams", [""])[0]
        data = json.loads(unquote(raw)) if raw else None
        if isinstance(data, dict) and data.get("id"):
            return str(data["id"])
    except (ValueError, TypeError, json.JSONDecodeError):
        pass
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def published_date_from_news_id(news_id: str) -> str:
    match = re.search(r"(20\d{2})[.\-_]?(\d{2})[.\-_]?(\d{2})", news_id)
    if match is None:
        return ""
    return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"


@dataclass(frozen=True, slots=True)
class TossNewsListConfig:
    baseline_complete: bool = False
    max_scroll_rounds: int = 3_000
    stable_bottom_rounds: int = 5
    history_stop_rounds: int = 5
    scroll_pause_ms: int = 900


class TossInvestNewsClient:
    def __init__(self, browser_pool: BrowserWorkerPool) -> None:
        self.browser_pool = browser_pool
        self.scope_states: dict[str, dict[str, Any]] = {}
        self.list_states: dict[str, dict[str, Any]] = {}

    async def collect_links(
        self,
        stock_key: str,
        *,
        stock_url: str | None = None,
        existing_ids: set[str] | None = None,
        config: TossNewsListConfig | None = None,
    ) -> list[dict[str, str]]:
        policy = config or TossNewsListConfig()
        existing = existing_ids or set()
        incremental = policy.baseline_complete and bool(existing)
        base_stock_url = stock_url or f"{TOSS_BASE_URL}/stocks/{stock_key}/order"
        news_url = base_stock_url.rsplit("/", 1)[0] + "/news"

        async def collect_with_lease(lease: Any) -> list[dict[str, str]]:
            page = lease.page
            list_root = await self._open_news(page, news_url)
            if list_root is None:
                await page.goto(TOSS_SCREENER_URL, wait_until="domcontentloaded")
                await page.wait_for_timeout(3_000)
                list_root = await self._open_news(page, news_url)
            if list_root is None:
                return []
            scroll_handle = await find_scroll_parent(list_root)
            new_links: dict[str, dict[str, str]] = {}
            seen_this_run: set[str] = set()
            stable_rounds = history_only_rounds = 0
            previous_seen_count = -1
            reached_bottom = False
            for _ in range(policy.max_scroll_rounds):
                anchors = page.locator(
                    'a[data-tossinvest-log="NewsDetailModalLink"],'
                    'a[href*="contentType=news"][href*="contentParams"]'
                )
                round_fresh = round_new = round_history = 0
                for index in range(await anchors.count()):
                    anchor = anchors.nth(index)
                    href = await anchor.get_attribute("href")
                    if not href:
                        continue
                    url = urljoin(TOSS_BASE_URL, href)
                    news_id = parse_news_id(url)
                    if news_id in seen_this_run:
                        continue
                    seen_this_run.add(news_id)
                    round_fresh += 1
                    if news_id in existing:
                        round_history += 1
                        continue
                    text = clean_toss_text(await anchor.inner_text())
                    if not text:
                        text = clean_toss_text(await anchor.evaluate("""(node) => {
                          let el=node; for(let i=0;i<8&&el;i++,el=el.parentElement){
                            const t=(el.innerText||'').trim();
                            if(t.length>=8&&t.length<=800)return t;
                          } return '';
                        }"""))
                    new_links[news_id] = {
                        "news_id": news_id, "news_url": url, "list_text": text,
                        "stock_key": stock_key,
                    }
                    round_new += 1
                if incremental and round_fresh:
                    history_only_rounds = history_only_rounds + 1 if not round_new and round_history else 0
                if incremental and history_only_rounds >= policy.history_stop_rounds:
                    break
                info = await scroll_element_once(scroll_handle)
                await page.wait_for_timeout(policy.scroll_pause_ms)
                stable_rounds = stable_rounds + 1 if len(seen_this_run) == previous_seen_count else 0
                previous_seen_count = len(seen_this_run)
                if (stable_rounds >= policy.stable_bottom_rounds and
                        info["after"] + info["client"] >= info["height"] - 8):
                    reached_bottom = True
                    break
            self.list_states[stock_key] = {
                "reached_bottom": reached_bottom,
                "incremental_mode": incremental,
                "seen_count": len(seen_this_run),
            }
            return list(new_links.values())

        return await self.browser_pool.run("news_list", collect_with_lease)

    async def crawl_stock(
        self,
        stock_key: str,
        *,
        stock_name: str = "",
        stock_url: str | None = None,
        existing_ids: set[str] | None = None,
        config: TossNewsListConfig | None = None,
        detail_limit: int | None = None,
        pending_links: list[dict[str, str]] | None = None,
    ) -> list[dict[str, Any]]:
        links = await self.collect_links(
            stock_key,
            stock_url=stock_url,
            existing_ids=existing_ids,
            config=config,
        )
        combined: dict[str, dict[str, str]] = {}
        for item in [*(pending_links or []), *links]:
            if item.get("news_id"):
                combined[str(item["news_id"])] = item
        links = list(combined.values())
        if detail_limit is not None:
            links = links[:detail_limit]
        stock = {
            "stock_key": stock_key,
            "stock_code": stock_key.removeprefix("A"),
            "stock_name": stock_name,
            "country_market": "KR",
            "market": "",
            "stock_url": stock_url or f"{TOSS_BASE_URL}/stocks/{stock_key}/order",
        }
        output, pending = await self._crawl_details(stock, links)
        self.scope_states[stock_key] = {
            "news_baseline_complete": bool(
                self.list_states.get(stock_key, {}).get("reached_bottom")
                and not pending
                and detail_limit is None
            ),
            "pending_news_links": pending,
            "pending_news_count": len(pending),
        }
        return output

    async def crawl_relations(
        self,
        stock_key: str,
        *,
        stock_name: str = "",
        stock_url: str | None = None,
        existing_ids: set[str] | None = None,
        config: TossNewsListConfig | None = None,
    ) -> list[dict[str, Any]]:
        links = await self.collect_links(
            stock_key,
            stock_url=stock_url,
            existing_ids=existing_ids,
            config=config,
        )
        resolved_stock_url = stock_url or f"{TOSS_BASE_URL}/stocks/{stock_key}/order"
        self.scope_states[stock_key] = {
            "news_baseline_complete": bool(
                self.list_states.get(stock_key, {}).get("reached_bottom")
            ),
            "pending_news_links": [],
            "pending_news_count": 0,
        }
        return [
            {
                **item,
                "stock_key": stock_key,
                "stock_code": stock_key.removeprefix("A"),
                "stock_name": stock_name,
                "stock_url": resolved_stock_url,
                "country_market": "KR",
                "market": "",
                "published_at": published_date_from_news_id(item["news_id"]),
            }
            for item in links
        ]

    async def _crawl_details(
        self,
        stock: dict[str, str],
        links: list[dict[str, str]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
        if not links:
            return [], []

        worker_count = min(
            len(links),
            self.browser_pool.channel_capacity("news_detail"),
        )
        queue: asyncio.Queue[tuple[int, dict[str, str]] | None] = asyncio.Queue(
            maxsize=worker_count
        )
        results: list[dict[str, Any] | None] = [None] * len(links)

        async def worker() -> None:
            while True:
                job = await queue.get()
                try:
                    if job is None:
                        return
                    index, item = job
                    try:
                        results[index] = await self.parse_article(stock, item)
                    except Exception:
                        results[index] = None
                finally:
                    queue.task_done()

        workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
        try:
            for index, item in enumerate(links):
                await queue.put((index, item))
            for _ in workers:
                await queue.put(None)
            await queue.join()
            await asyncio.gather(*workers)
        finally:
            for task in workers:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

        output = [article for article in results if article is not None]
        pending = [item for item, article in zip(links, results) if article is None]
        return output, pending

    async def parse_article(
        self,
        stock: dict[str, str],
        item: dict[str, str],
    ) -> dict[str, Any] | None:

        async def parse_with_lease(lease: Any) -> dict[str, Any] | None:
            page = lease.page
            article = await self._open_article(page, item["news_url"])
            if article is None:
                await page.goto(TOSS_SCREENER_URL, wait_until="domcontentloaded")
                await page.wait_for_timeout(3_000)
                article = await self._open_article(page, item["news_url"])
            if article is None:
                return None
            raw_text = clean_toss_text(await article.inner_text())
            if not raw_text:
                return None
            content = _clean_article_content(raw_text)
            structured = await _structured_metadata(page, item["news_id"])
            title, title_source = structured.get("title", ""), structured.get("title_source", "")
            if not title:
                title = await _meta_content(page, (
                    'meta[property="og:title"]', 'meta[name="twitter:title"]',
                    'meta[name="title"]',
                ))
                if _is_generic_title(title, stock.get("stock_name", "")):
                    title = ""
                elif title:
                    title_source = "meta"
            if not title:
                for locator in (
                    page.locator('[role="dialog"] h1,[role="dialog"] h2,[role="dialog"] h3'),
                    article.locator("h1,h2,h3"),
                ):
                    for index in range(min(await locator.count(), 20)):
                        candidate = clean_toss_text(await locator.nth(index).inner_text())
                        if 5 <= len(candidate) <= 220 and not _is_generic_title(candidate, stock.get("stock_name", "")):
                            title, title_source = candidate, "article_nearby_heading"
                            break
                    if title:
                        break
            if not title:
                for line in item.get("list_text", "").splitlines():
                    candidate = clean_toss_text(line)
                    if 5 <= len(candidate) <= 220 and not _is_generic_title(candidate, stock.get("stock_name", "")):
                        title, title_source = candidate, "list_text"
                        break

            publisher = structured.get("publisher", "")
            publisher_source = "page_json" if publisher else ""
            if not publisher:
                publisher = await _meta_content(page, (
                    'meta[name="publisher"]', 'meta[property="article:publisher"]',
                ))
                if publisher and "toss" not in publisher.lower() and "토스증권" not in publisher:
                    publisher_source = "meta"
                else:
                    publisher = ""
            if not publisher:
                publisher = publisher_from_news_id(item["news_id"])
                publisher_source = "news_id_prefix" if publisher else ""

            author = structured.get("author", "")
            author_source = "page_json" if author else ""
            if not author:
                author = await _meta_content(page, ('meta[name="author"]', 'meta[property="article:author"]'))
                if author and "toss" not in author.lower():
                    author_source = "meta"
                else:
                    author = ""
            if not author:
                matches = list(re.finditer(r"([가-힣]{2,5})\s*기자", "\n".join(content.splitlines()[-15:])))
                if matches:
                    author, author_source = matches[-1].group(1), "content_tail_reporter"

            published_at = structured.get("published_at", "")
            published_at_source = "page_json" if published_at else ""
            if not published_at:
                published_at = await _meta_content(page, (
                    'meta[property="article:published_time"]', 'meta[name="date"]',
                    'meta[name="pubdate"]', 'meta[name="publish-date"]',
                ))
                if published_at:
                    published_at_source = "meta"
            if not published_at:
                published_at = published_date_from_news_id(item["news_id"])
                if published_at:
                    published_at_source = "news_id_date"

            return {
                "source": "tossinvest", "schema_version": 5, **stock,
                "news_id": item["news_id"], "news_url": item["news_url"],
                "title": title, "title_available": bool(title), "title_source": title_source,
                "publisher": publisher, "publisher_source": publisher_source,
                "published_at": published_at,
                "published_date": published_at[:10] if len(published_at) >= 10 else "",
                "published_at_source": published_at_source,
                "author": author, "author_source": author_source,
                "content": content, "raw_text": raw_text,
                "list_text": item.get("list_text", ""),
            }

        return await self.browser_pool.run("news_detail", parse_with_lease)

    async def _open_article(self, page: Any, url: str) -> Any | None:
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_timeout(1_000)
        article = page.locator("article").first
        try:
            await article.wait_for(state="attached", timeout=12_000)
        except Exception:
            return None
        return article

    async def _open_news(self, page: Any, news_url: str) -> Any | None:
        await page.goto(news_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(1_800)
        root = page.locator('[data-list-name="NewsContentResolved"]').first
        try:
            await root.wait_for(state="attached", timeout=15_000)
        except Exception:
            return None
        return root


def _clean_article_content(raw_text: str) -> str:
    content = raw_text
    for marker in ("\n관련 주식\n", "\n관련주식\n", "\n관련 종목\n", "\n관련종목\n", "\n주요 뉴스\n"):
        if marker in content:
            content = content.split(marker, 1)[0]
    return content.strip()


def _is_generic_title(value: str, stock_name: str = "") -> bool:
    text = clean_toss_text(value)
    if not text or text.lower() in {"tossinvest", "toss invest", "토스증권", "토스증권 - 주식 투자를 더 쉽게"}:
        return True
    if any(token in text for token in (
        "뉴스 · 공시",
        "뉴스·공시",
        "주요 뉴스",
        "관련 주식",
        "관련 종목",
    )):
        return True
    return bool(stock_name and text in {f"{stock_name} 뉴스", f"{stock_name} News"})


async def _meta_content(page: Any, selectors: tuple[str, ...]) -> str:
    for selector in selectors:
        locator = page.locator(selector).first
        if await locator.count():
            value = clean_toss_text(await locator.get_attribute("content"))
            if value:
                return value
    return ""


def extract_structured_news_metadata(data: Any, news_id: str) -> dict[str, str]:
    fields = {"title": "", "publisher": "", "published_at": "", "author": "", "title_source": ""}
    title_keys = ("title", "headline", "newsTitle", "articleTitle", "contentTitle", "subject")
    publisher_keys = ("publisher", "press", "pressName", "media", "mediaName", "provider", "providerName", "sourceName", "officeName")
    date_keys = ("publishedAt", "published_at", "publishDate", "publishedDate", "datePublished", "createdAt", "created_at", "articleDate")
    author_keys = ("author", "authorName", "reporter", "reporterName", "writer", "writerName", "journalist")

    def first(obj, keys):
        for key in keys:
            value = obj.get(key)
            if isinstance(value, str) and clean_toss_text(value):
                return clean_toss_text(value)
            if isinstance(value, dict):
                for subkey in ("name", "title", "label"):
                    if isinstance(value.get(subkey), str) and clean_toss_text(value[subkey]):
                        return clean_toss_text(value[subkey])
        return ""

    def walk(node):
        if isinstance(node, dict):
            ids = [clean_toss_text(node.get(key)) for key in ("id", "newsId", "news_id", "articleId", "article_id", "contentId") if key in node]
            exact = news_id in ids
            shaped = any(key in node for key in title_keys) and any(key in node for key in ("content", "body", "article", "description", "summary"))
            if exact or shaped:
                if not fields["title"]:
                    fields["title"] = first(node, title_keys)
                    fields["title_source"] = "page_json_exact" if exact else "page_json_article"
                fields["publisher"] = fields["publisher"] or first(node, publisher_keys)
                fields["published_at"] = fields["published_at"] or first(node, date_keys)
                fields["author"] = fields["author"] or first(node, author_keys)
            for value in node.values():
                if isinstance(value, (dict, list)):
                    walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)
    walk(data)
    return fields


async def _structured_metadata(page: Any, news_id: str) -> dict[str, str]:
    scripts = page.locator('script#__NEXT_DATA__,script[type="application/json"],script[type="application/ld+json"]')
    result = {"title": "", "publisher": "", "published_at": "", "author": "", "title_source": ""}
    for index in range(min(await scripts.count(), 80)):
        raw = clean_toss_text(await scripts.nth(index).text_content())
        if not raw or raw[0] not in "[{":
            continue
        try:
            candidate = extract_structured_news_metadata(json.loads(raw), news_id)
        except json.JSONDecodeError:
            continue
        for key, value in candidate.items():
            result[key] = result[key] or value
    return result


def publisher_from_news_id(news_id: str) -> str:
    prefixes = {
        "seokyung_": "서울경제", "sedaily_": "서울경제", "edaily_": "이데일리",
        "hankyung_": "한국경제", "moneytoday_": "머니투데이", "maekyung_": "매일경제",
        "ajukyung_": "아주경제", "financial_": "파이낸셜뉴스", "news1_": "뉴스1",
        "etoday_": "이투데이", "electronicnews_": "전자신문", "asiae_": "아시아경제",
        "chosunbiz_": "조선비즈", "yna_": "연합뉴스", "yonhap_": "연합뉴스",
        "newspim_": "뉴스핌",
    }
    lowered = news_id.lower()
    return next((name for prefix, name in prefixes.items() if lowered.startswith(prefix)), "")
