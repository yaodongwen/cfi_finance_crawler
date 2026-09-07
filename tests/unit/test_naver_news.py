from __future__ import annotations

import pytest

from crawl_framework.sites.naver_finance.news import (
    NaverNewsClient,
    NaverNewsHTTPError,
    NaverNewsParseError,
    canonical_news_url,
    news_list_url,
    parse_article_html,
    parse_news_list,
)


class FakeResponse:

    def __init__(
        self,
        *,
        url,
        status_code=200,
        content="",
    ):

        self.url = url

        self.status_code = status_code

        self._content = content

        self.encoding = None

        self.apparent_encoding = "utf-8"


    @property
    def text(
        self,
    ):

        if isinstance(
            self._content,
            bytes,
        ):

            return self._content.decode(
                self.encoding
                or "utf-8"
            )

        return self._content


    def raise_for_status(
        self,
    ):

        if self.status_code >= 400:

            raise RuntimeError(
                f"HTTP {self.status_code}"
            )


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
            {
                "url": url,
                **kwargs,
            }
        )

        response = self.responses.pop(
            0
        )

        return response


class FakeRateLimiter:

    def __init__(
        self,
    ):

        self.events = []


    def before_request(
        self,
        endpoint,
    ):

        self.events.append(
            (
                "before",
                endpoint,
            )
        )


    def record_failure(
        self,
        endpoint,
        *,
        throttled=False,
    ):

        self.events.append(
            (
                "failure",
                endpoint,
                throttled,
            )
        )


    def record_success(
        self,
        endpoint,
    ):

        self.events.append(
            (
                "success",
                endpoint,
            )
        )


def test_news_list_url_preserves_legacy_shape():

    assert (
        news_list_url(
            "XKRX:005930",
            2,
        )
        ==
        (
            "https://finance.naver.com/"
            "item/news_news.naver"
            "?code=005930&page=2&clusterId="
        )
    )


def test_canonical_news_url_uses_office_and_article():

    assert (
        canonical_news_url(
            "001",
            "000123",
        )
        ==
        (
            "https://n.news.naver.com/"
            "mnews/article/001/000123"
        )
    )


def test_parse_news_list_extracts_identity_and_metadata():

    html = """
    <table>
      <tr>
        <td class="title">
          <a href="/item/news_read.naver?article_id=000123&office_id=001&code=005930">
            삼성전자 새 뉴스
          </a>
        </td>
        <td class="info">연합뉴스</td>
        <td class="date">2026.08.31 09:10</td>
      </tr>
      <tr>
        <td>
          <a href="/item/news_read.naver?article_id=000123&office_id=001&code=005930">
            삼성전자 새 뉴스
          </a>
        </td>
      </tr>
    </table>
    """

    rows = parse_news_list(
        html,
        code="005930",
        page=3,
    )

    assert len(
        rows
    ) == 1

    row = rows[0]

    assert row.article_id == "000123"

    assert row.office_id == "001"

    assert row.source_id == "001:000123"

    assert row.provider == "연합뉴스"

    assert row.listed_at == "2026.08.31 09:10"

    assert row.page == 3

    assert row.canonical_url == (
        "https://n.news.naver.com/"
        "mnews/article/001/000123"
    )


def test_parse_article_html_uses_legacy_body_selector_fallbacks():

    long_body = (
        "삼성전자 뉴스 본문입니다. "
        * 12
    )

    html = f"""
    <html>
      <head>
        <meta property="og:title" content="메타 제목">
      </head>
      <body>
        <div class="media_end_head_top_logo_text">테스트신문</div>
        <span class="media_end_head_info_datestamp_time"
              data-date-time="2026-08-31 09:10:00">ignored</span>
        <span class="media_end_head_journalist_name">홍길동 기자</span>
        <div id="newsct_article">
          <script>remove()</script>
          <p>{long_body}</p>
          <div class="copyright">저작권 문구</div>
        </div>
      </body>
    </html>
    """

    article = parse_article_html(
        html,
        news={
            "code": "005930",
            "article_id": "000123",
            "office_id": "001",
            "title": "목록 제목",
            "url": (
                "https://finance.naver.com/"
                "item/news_read.naver"
            ),
        },
        min_body_length=20,
    )

    assert article["title"] == "메타 제목"

    assert article["body_selector"] == "#newsct_article"

    assert article["source"] == "테스트신문"

    assert article["author"] == "홍길동 기자"

    assert article["published_at"] == "2026-08-31 09:10:00"

    assert "저작권" not in article["content"]

    assert article["url"] == (
        "https://n.news.naver.com/"
        "mnews/article/001/000123"
    )


def test_parse_article_html_rejects_missing_body():

    with pytest.raises(
        NaverNewsParseError
    ):

        parse_article_html(
            "<html><body></body></html>",
            news={
                "article_id": "1",
                "office_id": "001",
            },
            min_body_length=0,
        )


def test_client_decodes_finance_as_euc_kr_and_sets_referer():

    html = (
        """
        <a href="/item/news_read.naver?article_id=000123&office_id=001">
          삼성전자 새 뉴스
        </a>
        """
    ).encode(
        "euc-kr"
    )

    session = FakeSession(
        [
            FakeResponse(
                url=(
                    "https://finance.naver.com/"
                    "item/news_news.naver"
                ),
                content=html,
            )
        ]
    )

    client = NaverNewsClient(
        session=session,
        warmup_enabled=False,
        verify_ssl=False,
    )

    rows = client.fetch_news_list(
        "005930",
        page=1,
    )

    assert rows[0].title == "삼성전자 새 뉴스"

    assert session.calls[0]["headers"]["Referer"] == (
        "https://finance.naver.com/"
        "item/main.naver?code=005930"
    )


def test_client_decodes_article_as_utf8_and_uses_finance_referer():

    body = (
        "뉴스 본문 내용입니다. "
        * 12
    )

    html = f"""
    <html>
      <body>
        <h2 class="media_end_head_headline">뉴스 제목</h2>
        <article id="dic_area">{body}</article>
      </body>
    </html>
    """.encode(
        "utf-8"
    )

    session = FakeSession(
        [
            FakeResponse(
                url=(
                    "https://n.news.naver.com/"
                    "mnews/article/001/000123"
                ),
                content=html,
            )
        ]
    )

    client = NaverNewsClient(
        session=session,
        warmup_enabled=False,
        verify_ssl=False,
        min_body_length=20,
    )

    article = client.fetch_article(
        {
            "code": "005930",
            "article_id": "000123",
            "office_id": "001",
            "url": (
                "https://finance.naver.com/"
                "item/news_read.naver"
            ),
            "canonical_url": (
                "https://n.news.naver.com/"
                "mnews/article/001/000123"
            ),
        }
    )

    assert article["title"] == "뉴스 제목"

    assert session.calls[0]["headers"]["Referer"] == (
        "https://finance.naver.com/"
        "item/news_read.naver"
    )


def test_client_retries_429_then_fails():

    session = FakeSession(
        [
            FakeResponse(
                url="https://finance.naver.com/x",
                status_code=429,
            ),
            FakeResponse(
                url="https://finance.naver.com/x",
                status_code=429,
            ),
        ]
    )

    client = NaverNewsClient(
        session=session,
        retries=2,
        request_delay_seconds=0,
        warmup_enabled=False,
    )

    with pytest.raises(
        NaverNewsHTTPError
    ):

        client._get_text(
            "https://finance.naver.com/x"
        )

    assert len(
        session.calls
    ) == 2


def test_client_reports_429_to_rate_limiter():

    session = FakeSession(
        [
            FakeResponse(
                url="https://finance.naver.com/x",
                status_code=429,
            )
        ]
    )

    limiter = FakeRateLimiter()

    client = NaverNewsClient(
        session=session,
        retries=1,
        request_delay_seconds=0,
        warmup_enabled=False,
        rate_limiter=limiter,
    )

    with pytest.raises(
        NaverNewsHTTPError
    ):

        client._get_text(
            "https://finance.naver.com/x"
        )

    assert limiter.events == [
        (
            "before",
            "https://finance.naver.com/x",
        ),
        (
            "failure",
            "https://finance.naver.com/x",
            True,
        ),
    ]


@pytest.mark.asyncio
async def test_crawl_pages_skips_article_parse_errors():

    list_html = """
    <html>
      <body>
        <a href="/item/news_read.naver?article_id=000001&office_id=001">
          missing body
        </a>
        <a href="/item/news_read.naver?article_id=000002&office_id=001">
          good body
        </a>
      </body>
    </html>
    """.encode(
        "euc-kr"
    )

    good_body = (
        "정상 뉴스 본문입니다. "
        * 12
    )

    article_without_body = b"<html><body></body></html>"

    article_with_body = f"""
    <html>
      <body>
        <h2 class="media_end_head_headline">good</h2>
        <article id="dic_area">{good_body}</article>
      </body>
    </html>
    """.encode(
        "utf-8"
    )

    session = FakeSession(
        [
            FakeResponse(
                url=(
                    "https://finance.naver.com/"
                    "item/news_news.naver"
                ),
                content=list_html,
            ),
            FakeResponse(
                url=(
                    "https://n.news.naver.com/"
                    "mnews/article/001/000001"
                ),
                content=article_without_body,
            ),
            FakeResponse(
                url=(
                    "https://n.news.naver.com/"
                    "mnews/article/001/000002"
                ),
                content=article_with_body,
            ),
        ]
    )

    client = NaverNewsClient(
        session=session,
        warmup_enabled=False,
        verify_ssl=False,
        min_body_length=20,
    )

    rows = [
        row
        async for row in client.crawl_pages(
            "005930",
            max_pages=1,
        )
    ]

    assert len(rows) == 1
    assert rows[0]["article_id"] == "000002"
    assert len(client.detail_errors) == 1
    assert (
        client.detail_errors[0]["source_id"]
        == "001:000001"
    )
