from __future__ import annotations

import hashlib
import re

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

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


@dataclass(frozen=True, slots=True)
class TossForumCrawlConfig:
    mode: str = "incremental"
    max_scroll_rounds: int = 3_000
    stable_bottom_rounds: int = 5
    history_stop_rounds: int = 5
    scroll_pause_ms: int = 900

    def __post_init__(self) -> None:
        if self.mode not in {"full", "incremental"}:
            raise ValueError("forum mode must be 'full' or 'incremental'")


def _stable_post_id(*parts: Any) -> str:
    value = "|".join(clean_toss_text(part) for part in parts)
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _count_from_text(value: Any) -> int:
    match = re.search(r"\d[\d,]*", clean_toss_text(value))
    return int(match.group(0).replace(",", "")) if match else 0


def parse_relative_comment_time(
    created_at_text: Any,
    crawl_time: Any,
) -> tuple[str | None, str]:
    text = clean_toss_text(created_at_text)
    raw_base = clean_toss_text(crawl_time)
    if not text or not raw_base:
        return None, "unknown"
    try:
        base = datetime.fromisoformat(raw_base.replace("Z", "+00:00"))
        if base.tzinfo is None:
            base = base.replace(tzinfo=timezone.utc)
    except ValueError:
        return None, "unknown"
    if text in {"방금", "刚刚", "剛剛"} or text.lower() in {"just now", "now"}:
        return base.astimezone(timezone.utc).isoformat(), "approximate_relative"
    units = [
        (r"(\d+)\s*(?:초|秒|seconds?|secs?)", "seconds"),
        (r"(\d+)\s*(?:분|分钟|分鐘|minutes?|mins?)", "minutes"),
        (r"(\d+)\s*(?:시간|小时|小時|hours?|hrs?)", "hours"),
        (r"(\d+)\s*(?:일|天|days?)", "days"),
    ]
    for pattern, unit in units:
        match = re.search(pattern, text, re.I)
        if match:
            event_time = base - timedelta(**{unit: int(match.group(1))})
            return event_time.astimezone(timezone.utc).isoformat(), "approximate_relative"
    return None, "unknown"


class TossInvestForumClient:
    def __init__(self, browser_pool: BrowserWorkerPool) -> None:
        self.browser_pool = browser_pool

    async def crawl(
        self,
        stock_key: str,
        *,
        stock_name: str = "",
        stock_url: str | None = None,
        known_ids: set[str] | None = None,
        config: TossForumCrawlConfig | None = None,
    ) -> list[dict[str, Any]]:
        policy = config or TossForumCrawlConfig()
        known = known_ids or set()
        url = stock_url or f"{TOSS_BASE_URL}/stocks/{stock_key}/order"
        stock = {"stock_key": stock_key, "stock_name": stock_name, "stock_url": url}

        async def crawl_with_lease(lease: Any) -> list[dict[str, Any]]:
            page = lease.page
            anchor = await self._open_stock_community(page, url)
            if anchor is None:
                # A fresh Toss profile can render only the application shell on
                # a direct stock deep-link. A Screener visit initializes the SPA.
                await page.goto(TOSS_SCREENER_URL, wait_until="domcontentloaded")
                await page.wait_for_timeout(3_000)
                anchor = await self._open_stock_community(page, url)
            if anchor is None:
                return []
            scroll_handle = await find_scroll_parent(anchor)
            output: dict[str, dict[str, Any]] = {}
            seen_this_run: set[str] = set()
            history_only_rounds = 0
            stable_rounds = 0
            previous_seen_count = -1
            for _ in range(policy.max_scroll_rounds):
                current = await self._extract_visible_comment_cards(page, stock)
                round_new = 0
                round_old = 0
                for record in current:
                    post_id = record["post_id"]
                    if post_id in seen_this_run:
                        continue
                    seen_this_run.add(post_id)
                    if post_id in known:
                        round_old += 1
                    else:
                        output[post_id] = record
                        round_new += 1
                if round_new:
                    history_only_rounds = 0
                elif policy.mode == "incremental" and round_old:
                    history_only_rounds += 1
                if policy.mode == "incremental" and history_only_rounds >= policy.history_stop_rounds:
                    break
                info = await scroll_element_once(scroll_handle)
                await page.wait_for_timeout(policy.scroll_pause_ms)
                if len(seen_this_run) == previous_seen_count:
                    stable_rounds += 1
                else:
                    stable_rounds = 0
                previous_seen_count = len(seen_this_run)
                at_bottom = info["after"] + info["client"] >= info["height"] - 8
                if stable_rounds >= policy.stable_bottom_rounds and at_bottom:
                    break
            return list(output.values())

        return await self.browser_pool.run("forum", crawl_with_lease)

    async def _open_stock_community(self, page: Any, stock_url: str) -> Any | None:
        await page.goto(stock_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(1_800)
        await self._open_community_and_sort_latest(page)
        await page.wait_for_timeout(1_100)
        anchor = page.locator("[data-post-anchor-id]").first
        try:
            await anchor.wait_for(state="attached", timeout=12_000)
        except Exception:
            return None
        return anchor

    async def _open_community_and_sort_latest(self, page: Any) -> None:
        candidates = [
            page.get_by_role("tab", name=re.compile(r"커뮤니티|社区")),
            page.get_by_role("button", name=re.compile(r"커뮤니티|社区")),
            page.get_by_text(re.compile(r"^커뮤니티$|^社区$")).first,
        ]
        for locator in candidates:
            try:
                if await locator.count():
                    await locator.first.click()
                    await page.wait_for_timeout(1_300)
                    break
            except Exception:
                continue
        section = page.locator(
            '[data-section-name="종목상세_커뮤니티"],[data-section-name*="커뮤니티"]'
        ).first
        sort_buttons = [
            page.get_by_role("button", name=re.compile(r"투자.*환영|환영.*투자|인기|최신")),
            section.locator("button").filter(has_text=re.compile(r"투자|환영|인기|최신")),
        ]
        for locator in sort_buttons:
            try:
                if not await locator.count():
                    continue
                await locator.first.click()
                await page.wait_for_timeout(500)
                for latest in [
                    page.get_by_role("menuitem", name=re.compile(r"^최신$|^最新$")),
                    page.get_by_text(re.compile(r"^최신$|^最新$")),
                ]:
                    if await latest.count():
                        await latest.first.click()
                        await page.wait_for_timeout(1_000)
                        return
            except Exception:
                continue

    async def _extract_visible_comment_cards(
        self, page: Any, stock: dict[str, str]
    ) -> list[dict[str, Any]]:
        cards = page.locator("[data-post-anchor-id]")
        output = []
        for index in range(await cards.count()):
            card = cards.nth(index)
            try:
                post_id = clean_toss_text(await card.get_attribute("data-post-anchor-id"))
                raw_text = (await card.inner_text()).strip()
                if not raw_text:
                    continue
                raw_lines = [clean_toss_text(line) for line in raw_text.splitlines() if clean_toss_text(line)]
                is_shareholder = any(line in {"주주", "股东", "股東"} for line in raw_lines[:3])
                author = created_at_raw = created_at_text = None
                follower_count = 0
                header = card.locator('label[for^="header::image::"]').first
                if await header.count():
                    spans = header.locator(":scope > span")
                    if await spans.count() >= 1:
                        names = [x for x in (clean_toss_text(v) for v in (await spans.nth(0).inner_text()).splitlines()) if x]
                        author = names[0] if names else None
                    if await spans.count() >= 2:
                        created_at_raw = clean_toss_text(await spans.nth(1).inner_text())
                if created_at_raw:
                    created_at_text = clean_toss_text(re.split(r"\s*[・·]\s*", created_at_raw, maxsplit=1)[0])
                    match = re.search(r"(?:팔로워|followers?|粉丝|粉絲)\s*([0-9][0-9,]*)", created_at_raw, re.I)
                    if match:
                        follower_count = int(match.group(1).replace(",", ""))
                content = clean_toss_text(await card.evaluate("""(root) => {
                  const body = Array.from(root.children).find(
                    el => el.querySelector('[data-content-value="좋아요 버튼"]'));
                  if (!body) return '';
                  const clone = body.cloneNode(true);
                  clone.querySelectorAll('button,svg,style,script,noscript').forEach(n => n.remove());
                  return (clone.innerText || clone.textContent || '').trim();
                }"""))
                content = re.sub(r"\[data-radix-scroll-area-viewport\][^}]*\}", "", content)
                noise = {x for x in (author, created_at_raw, created_at_text, "팔로우", "关注", "關注", "주주", "股东", "股東") if x}
                content = "\n".join(line for line in (clean_toss_text(v) for v in content.splitlines()) if line and line not in noise)
                like = card.locator('[data-content-value="좋아요 버튼"]').first
                reply = card.locator('[data-content-value="댓글 펼치기 버튼"]').first
                image = card.locator('button[data-content-value="프로필 이미지"] img').first
                profile_image = await image.get_attribute("src") if await image.count() else None
                if not post_id:
                    post_id = _stable_post_id(stock["stock_key"], author, created_at_raw, content)
                output.append({
                    "source": "tossinvest", "stock_key": stock["stock_key"],
                    "stock_code": stock["stock_key"].removeprefix("A"),
                    "stock_name": stock.get("stock_name", ""), "country_market": "KR",
                    "market": "", "stock_url": stock["stock_url"], "post_id": post_id,
                    "author": author, "author_profile_image": profile_image,
                    "created_at_text": created_at_text, "created_at_raw": created_at_raw,
                    "follower_count": follower_count, "is_shareholder": is_shareholder,
                    "content": content, "raw_text": raw_text,
                    "like_count": _count_from_text(await like.inner_text()) if await like.count() else 0,
                    "comment_count": _count_from_text(await reply.inner_text()) if await reply.count() else 0,
                    "crawl_time": datetime.now(timezone.utc).isoformat(),
                })
            except Exception:
                continue
        return output
