import pytest

from crawl_framework.sites.naver_finance.forum_post import (
    ForumListItem,
    NaverForumClient,
    NaverForumHTTPError,
    NaverForumParseError,
    merge_forum_raw,
    parse_board_list,
    parse_discussion_detail,
    parse_int,
    strip_html,
)


# ============================================================
# Helpers
# ============================================================


class FakeResponse:

    def __init__(
        self,
        *,
        status_code=200,
        text="",
        content=None,
        json_data=None,
    ):

        self.status_code = (
            status_code
        )

        self.text = text

        self.content = (
            content
            if content is not None
            else text.encode(
                "utf-8"
            )
        )

        self._json_data = (
            json_data
        )


    def json(
        self,
    ):

        if isinstance(
            self._json_data,
            Exception,
        ):

            raise (
                self._json_data
            )

        return self._json_data


class FakeSession:

    def __init__(
        self,
        responses,
    ):

        self.responses = list(
            responses
        )

        self.calls = []

        self.headers = {}


    def get(
        self,
        url,
        **kwargs,
    ):

        self.calls.append(
            (
                url,
                kwargs,
            )
        )

        if not self.responses:

            raise AssertionError(
                "no fake responses left"
            )

        return self.responses.pop(
            0
        )


# ============================================================
# strip_html
# ============================================================


def test_strip_html():

    assert strip_html(
        "<p>Hello<br>World</p>"
    ) == (
        "Hello\nWorld"
    )


def test_parse_int():

    assert (
        parse_int(
            "1,234"
        )
        == 1234
    )

    assert (
        parse_int(
            None
        )
        is None
    )


# ============================================================
# Board parser
# ============================================================


def test_parse_board_list():

    html_text = """
    <table>
        <tr>
            <td class="title">
                <a href="/item/board_read.naver?code=005930&nid=12345&page=1">
                    삼성전자 게시글
                </a>
            </td>
            <td class="author">tester</td>
            <td class="date">2026.08.25 10:30</td>
            <td class="hit">1,234</td>
            <td class="recom">15</td>
        </tr>
    </table>
    """

    items = parse_board_list(
        html_text,
        code="005930",
    )

    assert (
        len(items)
        == 1
    )

    item = items[0]

    assert (
        item.nid
        == "12345"
    )

    assert (
        item.code
        == "005930"
    )

    assert (
        item.title
        == "삼성전자 게시글"
    )

    assert (
        item.nickname
        == "tester"
    )

    assert (
        item.view_count
        == 1234
    )

    assert (
        item.recommend
        == 15
    )


def test_parse_board_list_ignores_non_post_rows():

    html_text = """
    <table>
        <tr>
            <td>공지사항</td>
        </tr>
    </table>
    """

    items = parse_board_list(
        html_text,
        code="005930",
    )

    assert (
        items
        == []
    )


def test_parse_board_list_deduplicates_nid():

    html_text = """
    <table>
        <tr>
            <td>
                <a href="/item/board_read.naver?code=005930&nid=777">
                    A
                </a>
            </td>
        </tr>
        <tr>
            <td>
                <a href="/item/board_read.naver?code=005930&nid=777">
                    B
                </a>
            </td>
        </tr>
    </table>
    """

    items = parse_board_list(
        html_text,
        code="005930",
    )

    assert (
        len(items)
        == 1
    )


# ============================================================
# Detail parser
# ============================================================


def test_parse_discussion_detail():

    payload = {
        "isSuccess": True,
        "result": {
            "id": 999,
            "itemCode": "005930",
            "writer": {
                "nickname": "홍길동",
            },
            "writtenAt": (
                "2026-08-25T10:30:00+09:00"
            ),
            "title": "테스트",
            "contentHtml": (
                "<p>Hello<br>World</p>"
            ),
            "viewCount": 100,
            "likeCount": 3,
            "dislikeCount": 1,
        },
    }

    raw = (
        parse_discussion_detail(
            payload
        )
    )

    assert (
        raw["nid"]
        == "999"
    )

    assert (
        raw["code"]
        == "005930"
    )

    assert (
        raw["nickname"]
        == "홍길동"
    )

    assert (
        raw["content"]
        == "Hello\nWorld"
    )

    assert (
        raw["view_count"]
        == 100
    )

    assert (
        raw["recommend"]
        == 3
    )

    assert (
        raw["dislike"]
        == 1
    )


def test_detail_requires_result():

    with pytest.raises(
        NaverForumParseError
    ):

        parse_discussion_detail(
            {
                "isSuccess": True
            }
        )


def test_detail_false_success_rejected():

    with pytest.raises(
        NaverForumParseError
    ):

        parse_discussion_detail(
            {
                "isSuccess": False,
                "result": {},
            }
        )


# ============================================================
# Merge
# ============================================================


def test_merge_forum_raw():

    item = ForumListItem(
        nid="1",
        code="005930",
        title="list title",
        nickname="list user",
        view_count=10,
    )

    detail = {
        "nid": "1",
        "code": "005930",
        "title": "detail title",
        "nickname": "detail user",
        "content": "body",
        "view_count": 20,
    }

    raw = merge_forum_raw(
        item,
        detail,
    )

    assert (
        raw["title"]
        == "detail title"
    )

    assert (
        raw["nickname"]
        == "detail user"
    )

    assert (
        raw["content"]
        == "body"
    )

    assert (
        raw["view_count"]
        == 20
    )


def test_merge_without_detail():

    item = ForumListItem(
        nid="1",
        code="005930",
        title="hello",
    )

    raw = merge_forum_raw(
        item,
        None,
    )

    assert (
        raw["nid"]
        == "1"
    )

    assert (
        raw["title"]
        == "hello"
    )


# ============================================================
# URL builders
# ============================================================


def test_board_url():

    url = (
        NaverForumClient
        .board_url(
            "005930",
            page=2,
        )
    )

    assert (
        "code=005930"
        in url
    )

    assert (
        "page=2"
        in url
    )


def test_detail_url():

    url = (
        NaverForumClient
        .detail_api_url(
            "12345"
        )
    )

    assert (
        "id=12345"
        in url
    )


def test_bad_page_rejected():

    with pytest.raises(
        ValueError
    ):

        NaverForumClient.board_url(
            "005930",
            page=0,
        )


# ============================================================
# Client
# ============================================================


def test_fetch_board_page():

    html_text = """
    <table>
        <tr>
            <td class="title">
                <a href="/item/board_read.naver?code=005930&nid=123">
                    Test
                </a>
            </td>
        </tr>
    </table>
    """

    response = FakeResponse(
        content=(
            html_text.encode(
                "euc-kr"
            )
        )
    )

    session = FakeSession(
        [
            response
        ]
    )

    client = NaverForumClient(
        session=session
    )

    items = (
        client.fetch_board_page(
            "005930"
        )
    )

    assert (
        len(items)
        == 1
    )

    assert (
        items[0].nid
        == "123"
    )


def test_fetch_detail():

    response = FakeResponse(
        json_data={
            "isSuccess": True,
            "result": {
                "id": "321",
                "itemCode": "005930",
                "title": "hello",
                "contentHtml": "body",
            },
        }
    )

    session = FakeSession(
        [
            response
        ]
    )

    client = NaverForumClient(
        session=session
    )

    raw = client.fetch_detail(
        "321"
    )

    assert (
        raw["nid"]
        == "321"
    )

    assert (
        raw["title"]
        == "hello"
    )


def test_http_error():

    session = FakeSession(
        [
            FakeResponse(
                status_code=500
            )
        ]
    )

    client = NaverForumClient(
        session=session
    )

    with pytest.raises(
        NaverForumHTTPError
    ):

        client.fetch_board_page(
            "005930"
        )


# ============================================================
# Async page crawling
# ============================================================


@pytest.mark.asyncio
async def test_crawl_pages():

    client = NaverForumClient(
        session=FakeSession(
            []
        )
    )

    calls = []


    async def fake_page(
        code,
        *,
        page=1,
    ):

        calls.append(
            page
        )

        if page == 1:

            return [
                ForumListItem(
                    nid="1",
                    code=code,
                    title="hello",
                )
            ]

        return []


    async def fake_post(
        item,
        *,
        fetch_detail=True,
    ):

        return {
            "nid": item.nid,
            "code": item.code,
            "title": item.title,
            "content": "body",
        }


    client.async_fetch_board_page = (
        fake_page
    )

    client.async_fetch_post = (
        fake_post
    )

    rows = []

    async for raw in (
        client.crawl_pages(
            "005930"
        )
    ):

        rows.append(
            raw
        )

    assert (
        len(rows)
        == 1
    )

    assert (
        rows[0]["nid"]
        == "1"
    )

    assert calls == [
        1,
        2,
    ]


@pytest.mark.asyncio
async def test_crawl_pages_max_pages():

    client = NaverForumClient(
        session=FakeSession(
            []
        )
    )


    async def fake_page(
        code,
        *,
        page=1,
    ):

        return [
            ForumListItem(
                nid=str(
                    page
                ),
                code=code,
            )
        ]


    async def fake_post(
        item,
        *,
        fetch_detail=True,
    ):

        return {
            "nid": item.nid,
            "code": item.code,
        }


    client.async_fetch_board_page = (
        fake_page
    )

    client.async_fetch_post = (
        fake_post
    )

    rows = []

    async for raw in (
        client.crawl_pages(
            "005930",
            max_pages=2,
        )
    ):

        rows.append(
            raw
        )

    assert [
        row["nid"]
        for row
        in rows
    ] == [
        "1",
        "2",
    ]

def test_crawl_pages_attaches_real_page():
    import asyncio

    from crawl_framework.sites.naver_finance.forum_post import (
        NaverForumClient,
        ForumListItem,
    )

    class FakeClient(
        NaverForumClient
    ):

        async def async_fetch_board_page(
            self,
            code,
            *,
            page=1,
        ):

            if page == 1:

                return [
                    ForumListItem(
                        nid="1001",
                        code=code,
                        title="page1",
                        nickname="user1",
                        written_at="2026.08.26",
                        view_count=1,
                        recommend=0,
                        dislike=0,
                    )
                ]

            if page == 2:

                return [
                    ForumListItem(
                        nid="2001",
                        code=code,
                        title="page2",
                        nickname="user2",
                        written_at="2026.08.26",
                        view_count=1,
                        recommend=0,
                        dislike=0,
                    )
                ]

            return []

        async def async_fetch_post(
            self,
            item,
            *,
            fetch_detail=True,
        ):

            return {
                "nid": item.nid,
                "code": item.code,
                "title": item.title,
            }

    async def run():

        client = FakeClient()

        rows = []

        async for raw in (
            client.crawl_pages(
                "005930",
                start_page=1,
                max_pages=2,
                fetch_detail=False,
            )
        ):

            rows.append(
                raw
            )

        return rows

    rows = asyncio.run(
        run()
    )

    assert len(
        rows
    ) == 2

    assert (
        rows[0]["page"]
        == 1
    )

    assert (
        rows[1]["page"]
        == 2
    )

    assert (
        rows[0]["code"]
        == "005930"
    )

    assert (
        rows[1]["code"]
        == "005930"
    )
    