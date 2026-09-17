from __future__ import annotations

import re

from dataclasses import dataclass
from typing import Any

from crawl_framework.core.models import InstrumentRef
from crawl_framework.transports.playwright import (
    BrowserWorkerPool,
    find_scroll_parent,
    scroll_element_once,
)


TOSS_SCREENER_URL = "https://www.tossinvest.com/screener/3"
TOSS_BASE_URL = "https://www.tossinvest.com"
TOSS_STOCK_KEY_RE = re.compile(r"^A(\d{6})$")
PROTECTED_FILTERS = {
    "국내", "해외", "시장", "산업", "업종", "시가총액",
    "国内", "海外", "市场", "行业", "市值",
    "Domestic", "Overseas", "Market", "Industry", "Market Cap",
}


def clean_toss_text(value: Any) -> str:
    return re.sub(r"[ \t\r\f\v]+", " ", str(value or "").replace("\u200b", "")).strip()


@dataclass(frozen=True, slots=True)
class TossInvestInstrument:
    stock_key: str
    stock_code: str
    stock_name: str
    country_market: str
    market: str
    stock_url: str
    row_text: str

    @property
    def instrument_id(self) -> str:
        return f"XKRX:{self.stock_code}"

    def to_ref(self) -> InstrumentRef:
        return InstrumentRef(
            instrument_id=self.instrument_id,
            source_symbol=self.stock_key,
            name=self.stock_name or None,
            market=self.market or None,
            country="KR",
            source_url=self.stock_url,
        )


def build_toss_instrument(
    *, stock_key: Any, stock_name: Any = "", href: Any = "", row_text: Any = ""
) -> TossInvestInstrument | None:
    key = clean_toss_text(stock_key)
    match = TOSS_STOCK_KEY_RE.fullmatch(key)
    if match is None:
        return None
    path = clean_toss_text(href)
    if path.startswith("/"):
        url = TOSS_BASE_URL + path
    elif path:
        url = path
    else:
        url = f"{TOSS_BASE_URL}/stocks/{key}/order"
    return TossInvestInstrument(
        stock_key=key,
        stock_code=match.group(1),
        stock_name=clean_toss_text(stock_name),
        country_market="KR",
        market="",
        stock_url=url,
        row_text=clean_toss_text(row_text),
    )


class TossInvestInstrumentClient:
    def __init__(
        self,
        browser_pool: BrowserWorkerPool,
        *,
        screener_url: str = TOSS_SCREENER_URL,
        max_scroll_rounds: int = 1_000,
        stable_bottom_rounds: int = 5,
        scroll_pause_ms: int = 350,
    ) -> None:
        self.browser_pool = browser_pool
        self.screener_url = screener_url
        self.max_scroll_rounds = max_scroll_rounds
        self.stable_bottom_rounds = stable_bottom_rounds
        self.scroll_pause_ms = scroll_pause_ms

    async def discover(self, *, limit: int | None = None) -> list[TossInvestInstrument]:
        async with self.browser_pool.lease("instrument_discovery") as lease:
            page = lease.page
            await page.goto(self.screener_url, wait_until="domcontentloaded")
            await self._prepare_screener(page)
            grid = page.locator('[role="grid"]').first
            await grid.wait_for(state="attached")
            scroll_handle = await find_scroll_parent(grid)
            found: dict[str, TossInvestInstrument] = {}
            previous_count = -1
            stable_rounds = 0
            for _ in range(self.max_scroll_rounds):
                for item in await self._extract_visible_stocks(page):
                    found.setdefault(item.stock_key, item)
                if limit is not None and len(found) >= limit:
                    break
                info = await scroll_element_once(scroll_handle)
                await page.wait_for_timeout(self.scroll_pause_ms)
                if len(found) == previous_count:
                    stable_rounds += 1
                else:
                    stable_rounds = 0
                previous_count = len(found)
                at_bottom = info["after"] + info["client"] >= info["height"] - 8
                if stable_rounds >= self.stable_bottom_rounds and at_bottom:
                    break
            result = list(found.values())
            return result[:limit] if limit is not None else result

    async def _prepare_screener(self, page: Any) -> None:
        await self._wait_ready(page)
        await self._switch_to_domestic(page)
        await page.wait_for_timeout(500)
        await self._wait_ready(page)
        await self._remove_all_extra_filters(page)
        await page.wait_for_timeout(500)
        await self._wait_ready(page)

    async def _wait_ready(self, page: Any, timeout_ms: int = 45_000) -> None:
        stock_links = page.locator('a[href*="/stocks/"][href*="/order"]')
        skeletons = page.locator('[data-tds-wts-skeleton-box]')
        filter_buttons = page.locator('button[data-tossinvest-log="ModifyFilterChip"]')
        elapsed = 0
        while elapsed < timeout_ms:
            if await stock_links.count() > 0:
                return
            if await skeletons.count() == 0 and await filter_buttons.count() > 0:
                return
            await page.wait_for_timeout(500)
            elapsed += 500
        raise RuntimeError("Toss Screener did not become ready")

    async def _first_visible(self, locators: list[Any]) -> Any | None:
        for locator in locators:
            try:
                for index in range(await locator.count()):
                    candidate = locator.nth(index)
                    if await candidate.is_visible():
                        return candidate
            except Exception:
                continue
        return None

    async def _switch_to_domestic(self, page: Any) -> None:
        market_button = None
        for _ in range(30):
            market_button = await self._first_visible([
                page.locator('button[data-parent-name="국가_CONDITION"]'),
                page.locator('button[data-content-value="해외"]'),
                page.locator('button[data-content-value="국내"]'),
                page.locator("button").filter(has_text=re.compile(r"^\s*(해외|국내|海外|国内)\s*$")),
            ])
            if market_button is not None:
                break
            await page.wait_for_timeout(500)
        if market_button is None:
            raise RuntimeError("Toss Screener market selector not found")
        text = clean_toss_text(await market_button.inner_text())
        value = clean_toss_text(await market_button.get_attribute("data-content-value"))
        if value == "국내" or text in {"국내", "国内"}:
            return
        await market_button.click()
        await page.wait_for_timeout(500)
        domestic = await self._first_visible([
            page.locator('[data-tossinvest-log="ListRow"][data-parent-name="국가_CONDITION"]').filter(has_text=re.compile(r"국내|KOSPI|KOSDAQ|国内")),
            page.locator('[data-content-value="국내"]'),
            page.get_by_text(re.compile(r"^\s*(국내|国内)\s*$")),
        ])
        if domestic is None:
            raise RuntimeError("Toss Screener domestic market option not found")
        await domestic.click()
        apply_button = await self._first_visible([
            page.locator('button[data-parent-name*="Filter"]').filter(has_text=re.compile(r"^\s*보기\s*$")),
            page.get_by_role("button", name=re.compile(r"^(보기|看|查看)$")),
        ])
        if apply_button is None:
            raise RuntimeError("Toss Screener market apply button not found")
        await apply_button.click()

    async def _extra_filter_chips(self, page: Any) -> list[Any]:
        chips = page.locator('button[data-parent-name="FilterPopover"]')
        result = []
        for index in range(await chips.count()):
            chip = chips.nth(index)
            text = clean_toss_text(await chip.inner_text())
            value = clean_toss_text(await chip.get_attribute("data-content-value"))
            if (text or value) and text not in PROTECTED_FILTERS and value not in PROTECTED_FILTERS:
                result.append(chip)
        return result

    async def _remove_all_extra_filters(self, page: Any) -> None:
        for _ in range(50):
            chips = await self._extra_filter_chips(page)
            if not chips:
                return
            before = len(chips)
            await chips[0].click()
            await page.wait_for_timeout(300)
            remove = await self._first_visible([
                page.locator('button[data-parent-name="FilterEditDropdown"][data-content-value="필터제거"]'),
                page.locator('button[data-content-value="필터제거"]'),
                page.locator("button").filter(has_text=re.compile(r"필터\s*제거|过滤器移除|删除过滤器")),
            ])
            if remove is None:
                raise RuntimeError("Toss Screener filter remove action not found")
            await remove.click()
            await page.wait_for_timeout(700)
            if len(await self._extra_filter_chips(page)) >= before:
                await page.wait_for_timeout(1_000)
                if len(await self._extra_filter_chips(page)) >= before:
                    raise RuntimeError("Toss Screener extra filter was not removed")
        raise RuntimeError("Toss Screener exceeded filter removal limit")

    async def _extract_visible_stocks(self, page: Any) -> list[TossInvestInstrument]:
        rows = page.locator('div[role="row"][data-row-id]')
        result = []
        for index in range(await rows.count()):
            row = rows.nth(index)
            try:
                if not await row.is_visible():
                    continue
                link = row.locator('a[href*="/stocks/"][href*="/order"]').first
                href = await link.get_attribute("href") if await link.count() else ""
                item = build_toss_instrument(
                    stock_key=await row.get_attribute("data-row-id"),
                    stock_name=await row.get_attribute("data-contents-label"),
                    href=href,
                    row_text=await row.inner_text(),
                )
                if item is not None:
                    result.append(item)
            except Exception:
                continue
        return result
