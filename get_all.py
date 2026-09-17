# kabutan_marketnews_crawler.py
# 按月份获取网站所有新闻
import json
import re
import time
import random
import logging
from pathlib import Path
from datetime import datetime
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================
# 配置
# ============================================================

BASE_URL = "https://kabutan.jp"
LIST_URL = "https://kabutan.jp/news/marketnews/"

# 株探市场新闻截图显示历史从 2013 年 9 月开始
START_YEAR = 2013
START_MONTH = 9

# 当前爬取终点
END_YEAR = 2026
END_MONTH = 9

OUTPUT_DIR = Path("kabutan_data")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

NEWS_FILE = OUTPUT_DIR / "marketnews.jsonl"
FAILED_FILE = OUTPUT_DIR / "failed.jsonl"

# 不要调得太快
LIST_SLEEP = (1.0, 2.0)
ARTICLE_SLEEP = (1.0, 2.5)

TIMEOUT = 20
MAX_PAGES_PER_MONTH = 500

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/152.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Referer": "https://kabutan.jp/",
    "Connection": "keep-alive",
}


# ============================================================
# 日志
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ============================================================
# HTTP Session
# ============================================================

def create_session():
    session = requests.Session()
    session.headers.update(HEADERS)

    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=10,
        pool_maxsize=10
    )

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


SESSION = create_session()


# ============================================================
# 基础工具
# ============================================================

def sleep_random(interval):
    time.sleep(random.uniform(*interval))


def clean_text(text):
    if not text:
        return ""

    text = text.replace("\xa0", " ")
    text = text.replace("\u3000", " ")

    # 保留换行，但清理过多空白
    lines = []

    for line in text.splitlines():
        line = re.sub(r"[ \t]+", " ", line).strip()
        if line:
            lines.append(line)

    return "\n".join(lines)


def get_html(url, params=None):
    try:
        r = SESSION.get(
            url,
            params=params,
            timeout=TIMEOUT
        )

        if r.status_code != 200:
            logger.warning(
                "HTTP %s: %s",
                r.status_code,
                r.url
            )
            return None

        # 日文页面
        r.encoding = r.apparent_encoding or "utf-8"

        return r.text

    except Exception as e:
        logger.exception(
            "请求失败 %s: %s",
            url,
            e
        )
        return None


# ============================================================
# 新闻 ID
# ============================================================

NEWS_ID_RE = re.compile(r"(n\d{12})")


def extract_news_id(url):
    if not url:
        return None

    m = NEWS_ID_RE.search(url)

    if m:
        return m.group(1)

    return None


# ============================================================
# 解析列表页
# ============================================================

def parse_list_page(html):
    """
    返回：

    [
        {
            "news_id": "...",
            "title": "...",
            "category": "材料",
            "list_time": "08/07 15:40",
            "url": "..."
        }
    ]
    """

    soup = BeautifulSoup(html, "lxml")

    results = []

    # 比你提供的 nth-child selector 稳定得多
    tables = soup.select("table.s_news_list")

    for table in tables:

        for tr in table.select("tbody > tr"):

            tds = tr.find_all("td", recursive=False)

            if not tds:
                continue

            # ------------------------------------------------
            # 时间
            # ------------------------------------------------

            list_time = ""

            time_td = tr.select_one("td.news_time")

            if time_td:
                list_time = clean_text(
                    time_td.get_text(" ", strip=True)
                )

            # ------------------------------------------------
            # 新闻链接
            # ------------------------------------------------

            article_link = None

            # 你的截图里文章链接在 td 中的 <a>
            for a in tr.select("a[href]"):

                href = a.get("href", "")

                if NEWS_ID_RE.search(href):
                    article_link = a
                    break

            if article_link is None:
                continue

            href = article_link.get("href", "")
            url = urljoin(BASE_URL, href)

            news_id = extract_news_id(url)

            if not news_id:
                continue

            title = clean_text(
                article_link.get_text(" ", strip=True)
            )

            if not title:
                continue

            # ------------------------------------------------
            # 分类
            # ------------------------------------------------

            category = ""

            # 一般第二个 td 就是分类
            if len(tds) >= 2:

                candidate = clean_text(
                    tds[1].get_text(
                        " ",
                        strip=True
                    )
                )

                # 排除明显不是分类的内容
                if (
                    candidate
                    and len(candidate) <= 10
                    and candidate != title
                ):
                    category = candidate

            results.append({
                "news_id": news_id,
                "title": title,
                "category": category,
                "list_time": list_time,
                "url": url,
            })

    # 页面可能存在上下两个相同的 news table
    # 用 news_id 去重
    unique = {}

    for item in results:
        unique[item["news_id"]] = item

    return list(unique.values())


# ============================================================
# 详情页正文
# ============================================================

def parse_article_page(html, fallback=None):

    fallback = fallback or {}

    soup = BeautifulSoup(html, "lxml")

    result = {
        "news_id": fallback.get("news_id"),
        "title": fallback.get("title", ""),
        "category": fallback.get("category", ""),
        "published_at": None,
        "date": None,
        "content": "",
        "url": fallback.get("url", ""),
    }

    # ========================================================
    # article
    # ========================================================

    article = soup.select_one("article")

    if article is None:
        logger.warning(
            "没有找到 article: %s",
            fallback.get("url")
        )
        return result

    # ========================================================
    # 标题
    # ========================================================

    h1 = article.select_one("h1")

    if h1:
        title = clean_text(
            h1.get_text(" ", strip=True)
        )

        if title:
            result["title"] = title

    # ========================================================
    # 日期
    #
    # 你的截图：
    #
    # <time
    # class="s_news_date"
    # datetime="2026-08-07T08:00:00+09:00">
    # ========================================================

    time_tag = article.select_one(
        "time.s_news_date"
    )

    if not time_tag:
        time_tag = article.select_one(
            "time[datetime]"
        )

    if time_tag:

        dt = time_tag.get("datetime")

        if dt:

            result["published_at"] = dt

            try:
                parsed = datetime.fromisoformat(dt)

                result["date"] = (
                    parsed.date().isoformat()
                )

            except ValueError:
                pass

    # ========================================================
    # 分类
    # ========================================================

    # 如果列表页没有获取到分类，尝试在文章附近寻找
    if not result["category"]:

        possible_selectors = [
            ".news_category",
            ".category",
            ".s_news_category",
        ]

        for selector in possible_selectors:

            elem = article.select_one(selector)

            if elem:
                category = clean_text(
                    elem.get_text(
                        " ",
                        strip=True
                    )
                )

                if category:
                    result["category"] = category
                    break

    # ========================================================
    # 正文
    #
    # 你的截图：
    #
    # <article>
    #     ...
    #     <div class="body">
    #
    # ========================================================

    body = article.select_one(
        "div.body"
    )

    if body:

        # 去掉脚本、广告等
        for unwanted in body.select(
            "script, style, iframe, "
            ".ads_box, .ad, "
            ".sns, .related"
        ):
            unwanted.decompose()

        result["content"] = clean_text(
            body.get_text(
                "\n",
                strip=True
            )
        )

    return result


# ============================================================
# 存储
# ============================================================

def load_existing_ids():

    ids = set()

    if not NEWS_FILE.exists():
        return ids

    logger.info("读取已有数据，用于断点续爬……")

    with NEWS_FILE.open(
        "r",
        encoding="utf-8"
    ) as f:

        for line in f:

            line = line.strip()

            if not line:
                continue

            try:
                obj = json.loads(line)

                news_id = obj.get(
                    "news_id"
                )

                if news_id:
                    ids.add(news_id)

            except Exception:
                continue

    logger.info(
        "已有新闻 %d 篇",
        len(ids)
    )

    return ids


def save_news(record):

    with NEWS_FILE.open(
        "a",
        encoding="utf-8"
    ) as f:

        f.write(
            json.dumps(
                record,
                ensure_ascii=False
            )
            + "\n"
        )


def save_failed(data):

    data = dict(data)

    data["failed_at"] = (
        datetime.now()
        .isoformat()
    )

    with FAILED_FILE.open(
        "a",
        encoding="utf-8"
    ) as f:

        f.write(
            json.dumps(
                data,
                ensure_ascii=False
            )
            + "\n"
        )


# ============================================================
# 月份生成
# ============================================================

def iter_months(
    start_year,
    start_month,
    end_year,
    end_month
):

    year = start_year
    month = start_month

    while True:

        yield year, month

        if (
            year == end_year
            and month == end_month
        ):
            break

        month += 1

        if month == 13:
            month = 1
            year += 1


# ============================================================
# 爬某个月
# ============================================================

def crawl_month(
    year,
    month,
    existing_ids
):

    date_code = (
        f"{year:04d}"
        f"{month:02d}"
        "00"
    )

    logger.info(
        "======== %04d-%02d ========",
        year,
        month
    )

    total_new = 0

    page = 1

    seen_page_ids = set()

    while page <= MAX_PAGES_PER_MONTH:

        params = {
            "category": "-1",
            "date": date_code,
        }

        if page > 1:
            params["page"] = page

        logger.info(
            "%04d-%02d page=%d",
            year,
            month,
            page
        )

        html = get_html(
            LIST_URL,
            params=params
        )

        if not html:
            logger.warning(
                "列表页读取失败，结束本月"
            )
            break

        items = parse_list_page(
            html
        )

        # 没新闻，说明翻完了
        if not items:

            logger.info(
                "page=%d 无新闻，本月结束",
                page
            )

            break

        current_ids = {
            x["news_id"]
            for x in items
        }

        # ----------------------------------------------------
        # 很关键：
        # 如果下一页又出现完全相同的数据，
        # 说明 page 已经超范围
        # ----------------------------------------------------

        if (
            current_ids
            and current_ids.issubset(
                seen_page_ids
            )
        ):

            logger.info(
                "检测到重复分页，本月结束"
            )
            break

        seen_page_ids.update(
            current_ids
        )

        logger.info(
            "发现 %d 篇",
            len(items)
        )

        # ----------------------------------------------------
        # 逐篇抓正文
        # ----------------------------------------------------

        for i, item in enumerate(
            items,
            1
        ):

            news_id = item["news_id"]

            if news_id in existing_ids:

                logger.debug(
                    "跳过已有 %s",
                    news_id
                )

                continue

            logger.info(
                "[%d/%d] %s",
                i,
                len(items),
                item["title"][:50]
            )

            sleep_random(
                ARTICLE_SLEEP
            )

            article_html = get_html(
                item["url"]
            )

            if not article_html:

                save_failed(item)

                continue

            article = parse_article_page(
                article_html,
                fallback=item
            )

            # 如果正文为空，也记录失败
            if not article["content"]:

                logger.warning(
                    "正文为空: %s",
                    item["url"]
                )

                save_failed({
                    **item,
                    "reason":
                        "empty_content"
                })

                continue

            # 附加采集元数据
            article[
                "source"
            ] = "kabutan"

            article[
                "source_section"
            ] = "marketnews"

            article[
                "crawled_at"
            ] = (
                datetime.now()
                .astimezone()
                .isoformat()
            )

            save_news(
                article
            )

            existing_ids.add(
                news_id
            )

            total_new += 1

        page += 1

        sleep_random(
            LIST_SLEEP
        )

    logger.info(
        "%04d-%02d 完成，新增 %d 篇",
        year,
        month,
        total_new
    )

    return total_new


# ============================================================
# 主程序
# ============================================================

def main():

    existing_ids = (
        load_existing_ids()
    )

    grand_total = 0

    for year, month in iter_months(
        START_YEAR,
        START_MONTH,
        END_YEAR,
        END_MONTH
    ):

        try:

            added = crawl_month(
                year,
                month,
                existing_ids
            )

            grand_total += added

        except KeyboardInterrupt:

            logger.warning(
                "用户中断。数据已经保存，"
                "下次可以直接继续。"
            )

            break

        except Exception as e:

            logger.exception(
                "%04d-%02d 爬取异常: %s",
                year,
                month,
                e
            )

    logger.info(
        "全部结束，本次新增 %d 篇",
        grand_total
    )


if __name__ == "__main__":
    main()