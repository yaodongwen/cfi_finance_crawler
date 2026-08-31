from __future__ import annotations

import asyncio
import html
import json
import re
import time

from dataclasses import dataclass
from typing import (
    Any,
    AsyncIterator,
)
from urllib.parse import (
    parse_qs,
    urlencode,
    urljoin,
    urlparse,
)

import requests


# ============================================================
# Constants
# ============================================================


NAVER_FINANCE_BASE_URL = (
    "https://finance.naver.com"
)

NAVER_BOARD_URL = (
    NAVER_FINANCE_BASE_URL
    + "/item/board.naver"
)

NAVER_BOARD_READ_URL = (
    NAVER_FINANCE_BASE_URL
    + "/item/board_read.naver"
)

NAVER_DISCUSSION_DETAIL_URL = (
    "https://m.stock.naver.com/"
    "front-api/discussion/detail"
)


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 "
        "(Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 "
        "(KHTML, like Gecko) "
        "Chrome/151.0 Safari/537.36"
    ),
    "Accept-Language": (
        "ko-KR,ko;q=0.9,en;q=0.8"
    ),
}


# ============================================================
# Errors
# ============================================================


class NaverForumError(
    RuntimeError
):
    """
    Naver forum crawler base error.
    """


class NaverForumHTTPError(
    NaverForumError
):
    """
    HTTP request failed.
    """


class NaverForumPostMissingError(
    NaverForumHTTPError
):
    """
    A board list item pointed to a discussion detail that no longer exists.
    """


class NaverForumParseError(
    NaverForumError
):
    """
    Naver response could not be parsed.
    """


# ============================================================
# Models
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class ForumListItem:
    """
    Naver board 列表页的一条帖子。
    """

    nid: str

    code: str

    title: str | None = None

    nickname: str | None = None

    written_at: str | None = None

    view_count: int | None = None

    recommend: int | None = None

    dislike: int | None = None

    detail_url: str | None = None


    def to_raw(
        self,
    ) -> dict[
        str,
        Any,
    ]:

        return {
            "nid": self.nid,
            "code": self.code,
            "title": self.title,
            "nickname": self.nickname,
            "written_at": (
                self.written_at
            ),
            "view_count": (
                self.view_count
            ),
            "recommend": (
                self.recommend
            ),
            "dislike": (
                self.dislike
            ),
            "detail_url": (
                self.detail_url
            ),
        }


# ============================================================
# HTML helpers
# ============================================================


_TAG_RE = re.compile(
    r"<[^>]+>"
)


def strip_html(
    value: Any,
) -> str | None:
    """
    HTML -> plain text。
    """

    if value is None:

        return None

    text = str(
        value
    )

    text = re.sub(
        r"<br\s*/?>",
        "\n",
        text,
        flags=re.IGNORECASE,
    )

    text = _TAG_RE.sub(
        "",
        text,
    )

    text = html.unescape(
        text
    )

    text = text.replace(
        "\xa0",
        " ",
    )

    text = "\n".join(
        line.strip()
        for line
        in text.splitlines()
        if line.strip()
    )

    text = text.strip()

    return (
        text
        or None
    )


def parse_int(
    value: Any,
) -> int | None:
    """
    Naver 数值字符串：

        "1,234"
        " 55 "
        None

    -> int | None
    """

    if value is None:

        return None

    if isinstance(
        value,
        bool,
    ):

        return int(
            value
        )

    if isinstance(
        value,
        int,
    ):

        return value

    text = str(
        value
    ).strip()

    if not text:

        return None

    text = text.replace(
        ",",
        "",
    )

    match = re.search(
        r"-?\d+",
        text,
    )

    if match is None:

        return None

    try:

        return int(
            match.group(
                0
            )
        )

    except ValueError:

        return None


# ============================================================
# List parser
# ============================================================


_BOARD_LINK_RE = re.compile(
    r"""href=["'](?P<href>[^"']*board_read\.naver[^"']*)["']""",
    re.IGNORECASE,
)


_ROW_RE = re.compile(
    r"<tr\b[^>]*>(?P<body>.*?)</tr>",
    re.IGNORECASE
    | re.DOTALL,
)


_CELL_RE = re.compile(
    r"<td\b(?P<attrs>[^>]*)>(?P<body>.*?)</td>",
    re.IGNORECASE
    | re.DOTALL,
)


def parse_board_list(
    html_text: str,
    *,
    code: str,
) -> list[
    ForumListItem
]:
    """
    解析：

        /item/board.naver?code=005930&page=1

    返回帖子列表。

    解析策略故意保持宽松：
    不依赖完整 DOM class 层级，
    主要依赖 board_read.naver 链接中的 nid。

    这样 Naver 小幅 HTML 改版时更耐用。
    """

    code = str(
        code
    ).strip()

    if not code:

        raise ValueError(
            "code cannot be empty"
        )

    results: list[
        ForumListItem
    ] = []

    seen_nids: set[
        str
    ] = set()

    for row_match in (
        _ROW_RE.finditer(
            html_text
        )
    ):

        row_html = (
            row_match.group(
                "body"
            )
        )

        link_match = (
            _BOARD_LINK_RE.search(
                row_html
            )
        )

        if link_match is None:

            continue

        href = html.unescape(
            link_match.group(
                "href"
            )
        )

        parsed = urlparse(
            href
        )

        query = parse_qs(
            parsed.query
        )

        nid = (
            query.get(
                "nid",
                [None],
            )[0]
        )

        if nid is None:

            continue

        nid = str(
            nid
        ).strip()

        if (
            not nid
            or nid in seen_nids
        ):

            continue

        seen_nids.add(
            nid
        )

        cells = [
            cell_match
            for cell_match
            in _CELL_RE.finditer(
                row_html
            )
        ]

        title = _extract_title(
            row_html
        )

        nickname = (
            _extract_cell_by_class(
                cells,
                (
                    "author",
                    "writer",
                ),
            )
        )

        written_at = (
            _extract_cell_by_class(
                cells,
                (
                    "date",
                ),
            )
        )

        view_count = parse_int(
            _extract_cell_by_class(
                cells,
                (
                    "hit",
                    "view",
                ),
            )
        )

        recommend = parse_int(
            _extract_cell_by_class(
                cells,
                (
                    "recom",
                    "recommend",
                ),
            )
        )

        detail_url = urljoin(
            NAVER_FINANCE_BASE_URL,
            href,
        )

        results.append(
            ForumListItem(
                nid=nid,
                code=code,
                title=title,
                nickname=nickname,
                written_at=(
                    written_at
                ),
                view_count=(
                    view_count
                ),
                recommend=(
                    recommend
                ),
                detail_url=(
                    detail_url
                ),
            )
        )

    return results


def _extract_title(
    row_html: str,
) -> str | None:

    match = re.search(
        r"""
        <a\b
        [^>]*href=["']
        [^"']*board_read\.naver
        [^"']*
        ["'][^>]*
        >
        (?P<title>.*?)
        </a>
        """,
        row_html,
        flags=(
            re.IGNORECASE
            | re.DOTALL
            | re.VERBOSE
        ),
    )

    if match is None:

        return None

    return strip_html(
        match.group(
            "title"
        )
    )


def _extract_cell_by_class(
    cells: list[
        re.Match
    ],
    class_names: tuple[
        str,
        ...,
    ],
) -> str | None:

    for cell in cells:

        attrs = (
            cell.group(
                "attrs"
            )
            or ""
        )

        attrs_lower = (
            attrs.lower()
        )

        if not any(
            name.lower()
            in attrs_lower
            for name
            in class_names
        ):

            continue

        return strip_html(
            cell.group(
                "body"
            )
        )

    return None


# ============================================================
# Detail parser
# ============================================================


def parse_discussion_detail(
    payload: dict[
        str,
        Any,
    ],
) -> dict[
    str,
    Any,
]:
    """
    解析 Naver mobile discussion detail JSON。

    已知结构大致：

        {
            "isSuccess": true,
            "result": {
                "id": ...,
                "itemCode": ...,
                "writer": {
                    "nickname": ...
                },
                "writtenAt": ...,
                "title": ...,
                "contentHtml": ...,
                ...
            }
        }

    返回 framework raw dict。
    """

    if not isinstance(
        payload,
        dict,
    ):

        raise (
            NaverForumParseError(
                "discussion detail "
                "payload is not a dict"
            )
        )

    success = payload.get(
        "isSuccess"
    )

    if success is False:

        raise (
            NaverForumParseError(
                "Naver discussion API "
                "returned isSuccess=false"
            )
        )

    result = payload.get(
        "result"
    )

    if not isinstance(
        result,
        dict,
    ):

        raise (
            NaverForumParseError(
                "Naver discussion API "
                "has no result object"
            )
        )

    nid = (
        result.get(
            "id"
        )
        or result.get(
            "nid"
        )
    )

    if nid is None:

        raise (
            NaverForumParseError(
                "discussion detail "
                "has no id"
            )
        )

    writer = result.get(
        "writer"
    )

    nickname = None

    if isinstance(
        writer,
        dict,
    ):

        nickname = (
            writer.get(
                "nickname"
            )
            or writer.get(
                "name"
            )
        )

    content = (
        result.get(
            "contentHtml"
        )
        or result.get(
            "content"
        )
    )

    raw = {
        "nid": str(
            nid
        ).strip(),
        "code": _clean_text(
            result.get(
                "itemCode"
            )
        ),
        "title": _clean_text(
            result.get(
                "title"
            )
        ),
        "nickname": _clean_text(
            nickname
        ),
        "written_at": _clean_text(
            result.get(
                "writtenAt"
            )
        ),
        "content": strip_html(
            content
        ),
        "view_count": parse_int(
            result.get(
                "viewCount"
            )
            or result.get(
                "readCount"
            )
        ),
        "recommend": parse_int(
            result.get(
                "likeCount"
            )
            or result.get(
                "recommend"
            )
        ),
        "dislike": parse_int(
            result.get(
                "dislikeCount"
            )
            or result.get(
                "dislike"
            )
        ),
    }

    return raw


def _clean_text(
    value: Any,
) -> str | None:

    if value is None:

        return None

    text = str(
        value
    ).strip()

    return (
        text
        or None
    )


# ============================================================
# Merge
# ============================================================


def merge_forum_raw(
    list_item: ForumListItem,
    detail: dict[
        str,
        Any,
    ]
    | None,
) -> dict[
    str,
    Any,
]:
    """
    list + detail 合并。

    detail 优先，但 detail 缺失字段时
    保留 list page 数据。
    """

    raw = (
        list_item.to_raw()
    )

    if not detail:

        return raw

    for key, value in (
        detail.items()
    ):

        if value is not None:

            raw[
                key
            ] = value

    # code 不允许被 detail 空值覆盖。
    raw[
        "code"
    ] = (
        raw.get(
            "code"
        )
        or list_item.code
    )

    raw[
        "nid"
    ] = (
        raw.get(
            "nid"
        )
        or list_item.nid
    )

    raw[
        "detail_url"
    ] = (
        raw.get(
            "detail_url"
        )
        or list_item.detail_url
    )

    return raw


# ============================================================
# HTTP client
# ============================================================


class NaverForumClient:
    """
    Naver Finance forum HTTP client。

    网络访问使用 requests，
    async crawl 场景通过 asyncio.to_thread()
    避免阻塞 event loop。

    后续如果统一 transport/http.py 接口成熟，
    可以把这里的 HTTP 实现替换掉，
    parser 和 plugin 层不用变化。
    """

    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        timeout: float = 15.0,
        verify_ssl: bool = True,
        request_retries: int = 3,
        retry_sleep_seconds: float = 1.0,
    ) -> None:

        if timeout <= 0:

            raise ValueError(
                "timeout must be positive"
            )

        if request_retries < 1:

            raise ValueError(
                "request_retries must be >= 1"
            )

        if retry_sleep_seconds < 0:

            raise ValueError(
                "retry_sleep_seconds must be >= 0"
            )

        self.session = (
            session
            or requests.Session()
        )

        self.timeout = timeout

        self.verify_ssl = (
            verify_ssl
        )

        self.request_retries = request_retries

        self.retry_sleep_seconds = retry_sleep_seconds

        self.session.headers.update(
            DEFAULT_HEADERS
        )


    # ========================================================
    # URL builders
    # ========================================================

    @staticmethod
    def board_url(
        code: str,
        *,
        page: int = 1,
    ) -> str:

        if page <= 0:

            raise ValueError(
                "page must be positive"
            )

        query = urlencode(
            {
                "code": code,
                "page": page,
            }
        )

        return (
            f"{NAVER_BOARD_URL}"
            f"?{query}"
        )


    @staticmethod
    def detail_api_url(
        nid: str,
    ) -> str:

        query = urlencode(
            {
                "id": nid,
            }
        )

        return (
            f"{NAVER_DISCUSSION_DETAIL_URL}"
            f"?{query}"
        )


    # ========================================================
    # Sync HTTP
    # ========================================================

    def fetch_board_page(
        self,
        code: str,
        *,
        page: int = 1,
    ) -> list[
        ForumListItem
    ]:

        url = self.board_url(
            code,
            page=page,
        )

        response = (
            self._get_with_retries(
                url
            )
        )

        self._raise_for_status(
            response,
            url,
        )

        # Naver Finance PC pages historically
        # use EUC-KR.
        #
        # requests 有时自动识别失败，
        # 因此优先显式 euc-kr。
        try:

            text = (
                response.content
                .decode(
                    "euc-kr"
                )
            )

        except UnicodeDecodeError:

            text = (
                response.text
            )

        return parse_board_list(
            text,
            code=code,
        )


    def fetch_detail(
        self,
        nid: str,
    ) -> dict[
        str,
        Any,
    ]:

        url = self.detail_api_url(
            nid
        )

        response = (
            self._get_with_retries(
                url
            )
        )

        status_code = getattr(
            response,
            "status_code",
            None,
        )

        if status_code == 404:

            raise NaverForumPostMissingError(
                "Naver discussion detail "
                "is no longer available: "
                f"nid={nid}, url={url}"
            )

        self._raise_for_status(
            response,
            url,
        )

        try:

            payload = (
                response.json()
            )

        except Exception:

            try:

                payload = json.loads(
                    response.text
                )

            except Exception as exc:

                raise (
                    NaverForumParseError(
                        "invalid discussion "
                        f"detail JSON for nid={nid}"
                    )
                ) from exc

        return (
            parse_discussion_detail(
                payload
            )
        )


    def _get_with_retries(
        self,
        url: str,
    ):

        last_error: Exception | None = None

        for attempt in range(
            1,
            self.request_retries
            +
            1,
        ):

            try:

                return self.session.get(
                    url,
                    timeout=(
                        self.timeout
                    ),
                    verify=(
                        self.verify_ssl
                    ),
                )

            except requests.RequestException as exc:

                last_error = exc

                if attempt >= self.request_retries:

                    break

                if self.retry_sleep_seconds:

                    time.sleep(
                        self.retry_sleep_seconds
                    )

        assert last_error is not None

        raise last_error


    def fetch_post(
        self,
        item: ForumListItem,
        *,
        fetch_detail: bool = True,
    ) -> dict[
        str,
        Any,
    ]:

        detail = None

        if fetch_detail:

            try:

                detail = (
                    self.fetch_detail(
                        item.nid
                    )
                )

            except NaverForumPostMissingError:

                return {}

        return merge_forum_raw(
            item,
            detail,
        )


    # ========================================================
    # Async wrappers
    # ========================================================

    async def async_fetch_board_page(
        self,
        code: str,
        *,
        page: int = 1,
    ) -> list[
        ForumListItem
    ]:

        return await asyncio.to_thread(
            self.fetch_board_page,
            code,
            page=page,
        )


    async def async_fetch_detail(
        self,
        nid: str,
    ) -> dict[
        str,
        Any,
    ]:

        return await asyncio.to_thread(
            self.fetch_detail,
            nid,
        )


    async def async_fetch_post(
        self,
        item: ForumListItem,
        *,
        fetch_detail: bool = True,
    ) -> dict[
        str,
        Any,
    ]:

        return await asyncio.to_thread(
            self.fetch_post,
            item,
            fetch_detail=(
                fetch_detail
            ),
        )


    # ========================================================
    # Page iterator
    # ========================================================

    async def crawl_pages(
        self,
        code: str,
        *,
        start_page: int = 1,
        max_pages: int | None = None,
        fetch_detail: bool = True,
    ) -> AsyncIterator[
        dict[
            str,
            Any,
        ]
    ]:
        """
        顺序抓取多个 board pages。

        每条返回的 raw 都会带：

            code
            page

        停止条件：

            1. 当前页面没有帖子
            2. 达到 max_pages

        当前故意保持顺序抓取，
        暂不增加高并发。
        """

        if start_page <= 0:

            raise ValueError(
                "start_page must be positive"
            )

        if (
            max_pages is not None
            and max_pages <= 0
        ):

            raise ValueError(
                "max_pages must be positive"
            )

        page = start_page

        processed_pages = 0

        while True:

            # =================================================
            # Max pages guard
            # =================================================

            if (
                max_pages is not None
                and processed_pages
                >= max_pages
            ):

                return

            # =================================================
            # Fetch current board page
            # =================================================

            items = (
                await self
                .async_fetch_board_page(
                    code,
                    page=page,
                )
            )

            # =================================================
            # No more posts
            # =================================================

            if not items:

                return

            # =================================================
            # Fetch each post
            # =================================================

            for item in items:

                raw = (
                    await self
                    .async_fetch_post(
                        item,
                        fetch_detail=(
                            fetch_detail
                        ),
                    )
                )

                if not raw:

                    continue

                # =============================================
                # Ensure source instrument code
                # =============================================

                if not raw.get(
                    "code"
                ):

                    raw[
                        "code"
                    ] = code

                # =============================================
                # IMPORTANT:
                # Write the real board page for this record
                # =============================================

                raw[
                    "page"
                ] = page

                yield raw

            # =================================================
            # Advance to next page
            # =================================================

            processed_pages += 1

            page += 1

    # ========================================================
    # HTTP validation
    # ========================================================

    @staticmethod
    def _raise_for_status(
        response: Any,
        url: str,
    ) -> None:

        status_code = getattr(
            response,
            "status_code",
            None,
        )

        if (
            status_code is not None
            and 200
            <= int(
                status_code
            )
            < 300
        ):

            return

        raise (
            NaverForumHTTPError(
                "Naver request failed: "
                f"status={status_code}, "
                f"url={url}"
            )
        )
