from __future__ import annotations

import pytest

from crawl_framework.sites.naver_finance.research import (
    NaverResearchClient,
    NaverResearchHTTPError,
    ResearchListItem,
    build_research_list_signature,
    normalize_categories,
    normalize_research_date,
    parse_research_detail,
    parse_research_list,
    research_list_url,
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

        self.content = content

        self.encoding = None

        self.apparent_encoding = "utf-8"


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

        return self.responses.pop(
            0
        )


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


def make_research_list_item(
    report_id,
    *,
    category="market",
    page=1,
):

    return ResearchListItem(
        report_type=category,
        report_id=str(
            report_id
        ),
        title=f"report {report_id}",
        institution="test",
        published_at="2026-09-07",
        views=None,
        detail_url=(
            "https://finance.naver.com/"
            f"research/{category}_read.naver?nid={report_id}"
        ),
        list_url=research_list_url(
            category,
            page,
        ),
        list_page=page,
    )


class PagingResearchClient(
    NaverResearchClient
):

    def __init__(
        self,
        pages,
        *,
        stop_empty_pages=1,
    ):

        super().__init__(
            stop_empty_pages=stop_empty_pages,
            request_delay_seconds=0,
            verify_ssl=False,
        )

        self.pages = pages

        self.list_calls = []

        self.detail_calls = []


    def fetch_list(
        self,
        category,
        *,
        page,
    ):

        self.list_calls.append(
            (
                category,
                page,
            )
        )

        return list(
            self.pages.get(
                (
                    category,
                    page,
                ),
                [],
            )
        )


    def fetch_detail(
        self,
        item,
    ):

        self.detail_calls.append(
            (
                item.report_type,
                item.report_id,
                item.list_page,
            )
        )

        return item.to_raw()


async def collect_research_pages(
    client,
    **kwargs,
):

    return [
        row
        async for row in client.crawl_pages(
            **kwargs
        )
    ]


def test_research_list_url_supports_all_categories():

    assert research_list_url(
        "market",
        2,
    ) == (
        "https://finance.naver.com/"
        "research/market_info_list.naver?page=2"
    )

    assert normalize_categories(
        "all"
    ) == (
        "market",
        "invest",
        "company",
        "industry",
        "economy",
        "debenture",
    )

    assert normalize_categories(
        (
            "market",
            "all",
            "market",
        )
    ) == (
        "market",
        "invest",
        "company",
        "industry",
        "economy",
        "debenture",
    )


def test_build_research_list_signature_uses_report_ids():

    assert build_research_list_signature(
        [
            make_research_list_item(
                " 101 ",
            ),
            make_research_list_item(
                "",
            ),
            make_research_list_item(
                100,
            ),
        ]
    ) == (
        "101",
        "100",
    )


def test_crawl_pages_stops_on_empty_page_after_processing_pages():

    client = PagingResearchClient(
        {
            (
                "market",
                1,
            ): [
                make_research_list_item(
                    101,
                    page=1,
                ),
                make_research_list_item(
                    100,
                    page=1,
                ),
            ],
            (
                "market",
                2,
            ): [
                make_research_list_item(
                    99,
                    page=2,
                ),
                make_research_list_item(
                    98,
                    page=2,
                ),
            ],
            (
                "market",
                3,
            ): [],
        }
    )

    import asyncio

    rows = asyncio.run(
        collect_research_pages(
            client,
            categories="market",
            mode="full",
            max_pages=10,
        )
    )

    assert [
        row[
            "report_id"
        ]
        for row in rows
    ] == [
        "101",
        "100",
        "99",
        "98",
    ]

    assert client.list_calls == [
        (
            "market",
            1,
        ),
        (
            "market",
            2,
        ),
        (
            "market",
            3,
        ),
    ]


def test_crawl_pages_stops_repeated_final_page_before_fetch_detail():

    client = PagingResearchClient(
        {
            (
                "market",
                1,
            ): [
                make_research_list_item(
                    101,
                    page=1,
                ),
                make_research_list_item(
                    100,
                    page=1,
                ),
            ],
            (
                "market",
                2,
            ): [
                make_research_list_item(
                    99,
                    page=2,
                ),
                make_research_list_item(
                    98,
                    page=2,
                ),
            ],
            (
                "market",
                3,
            ): [
                make_research_list_item(
                    99,
                    page=3,
                ),
                make_research_list_item(
                    98,
                    page=3,
                ),
            ],
            (
                "market",
                4,
            ): [
                make_research_list_item(
                    99,
                    page=4,
                ),
                make_research_list_item(
                    98,
                    page=4,
                ),
            ],
        }
    )

    import asyncio

    rows = asyncio.run(
        collect_research_pages(
            client,
            categories="market",
            mode="full",
            max_pages=10,
        )
    )

    assert [
        row[
            "report_id"
        ]
        for row in rows
    ] == [
        "101",
        "100",
        "99",
        "98",
    ]

    assert client.list_calls == [
        (
            "market",
            1,
        ),
        (
            "market",
            2,
        ),
        (
            "market",
            3,
        ),
    ]

    assert client.detail_calls == [
        (
            "market",
            "101",
            1,
        ),
        (
            "market",
            "100",
            1,
        ),
        (
            "market",
            "99",
            2,
        ),
        (
            "market",
            "98",
            2,
        ),
    ]


def test_crawl_pages_repeated_signature_state_is_per_category():

    client = PagingResearchClient(
        {
            (
                "market",
                1,
            ): [
                make_research_list_item(
                    101,
                    category="market",
                    page=1,
                ),
                make_research_list_item(
                    100,
                    category="market",
                    page=1,
                ),
            ],
            (
                "market",
                2,
            ): [],
            (
                "company",
                1,
            ): [
                make_research_list_item(
                    101,
                    category="company",
                    page=1,
                ),
                make_research_list_item(
                    100,
                    category="company",
                    page=1,
                ),
            ],
            (
                "company",
                2,
            ): [],
        }
    )

    import asyncio

    rows = asyncio.run(
        collect_research_pages(
            client,
            categories=(
                "market",
                "company",
            ),
            mode="full",
            max_pages=10,
        )
    )

    assert [
        (
            row[
                "category"
            ],
            row[
                "report_id"
            ],
        )
        for row in rows
    ] == [
        (
            "market",
            "101",
        ),
        (
            "market",
            "100",
        ),
        (
            "company",
            "101",
        ),
        (
            "company",
            "100",
        ),
    ]

    assert client.list_calls == [
        (
            "market",
            1,
        ),
        (
            "market",
            2,
        ),
        (
            "company",
            1,
        ),
        (
            "company",
            2,
        ),
    ]


def test_crawl_pages_incremental_existing_pages_stop_still_applies():

    client = PagingResearchClient(
        {
            (
                "market",
                1,
            ): [
                make_research_list_item(
                    101,
                    page=1,
                ),
                make_research_list_item(
                    100,
                    page=1,
                ),
            ],
            (
                "market",
                2,
            ): [
                make_research_list_item(
                    99,
                    page=2,
                ),
                make_research_list_item(
                    98,
                    page=2,
                ),
            ],
            (
                "market",
                3,
            ): [
                make_research_list_item(
                    97,
                    page=3,
                ),
            ],
            (
                "market",
                4,
            ): [
                make_research_list_item(
                    96,
                    page=4,
                ),
            ],
        }
    )

    import asyncio

    rows = asyncio.run(
        collect_research_pages(
            client,
            categories="market",
            mode="incremental",
            max_pages=10,
            seen_report_ids={
                "101",
                "100",
                "99",
                "98",
                "97",
            },
        )
    )

    assert rows == []

    assert client.detail_calls == []

    assert client.list_calls == [
        (
            "market",
            1,
        ),
        (
            "market",
            2,
        ),
        (
            "market",
            3,
        ),
    ]


def test_normalize_research_date_accepts_legacy_formats():

    assert normalize_research_date(
        "26.08.07"
    ) == "2026-08-07"

    assert normalize_research_date(
        "2026년8월7일"
    ) == "2026-08-07"


def test_parse_research_list_company_extracts_stock_and_pdf_hint():

    html = """
    <table class="type_1">
      <tr>
        <td><a href="/item/main.naver?code=005930">삼성전자</a></td>
        <td><a href="/research/company_read.naver?nid=123&page=1">기업 리포트</a></td>
        <td>신한투자증권</td>
        <td><img alt="pdf"></td>
        <td>26.08.07</td>
        <td>1,234</td>
      </tr>
    </table>
    """

    rows = parse_research_list(
        "company",
        html,
        list_url=research_list_url(
            "company",
            1,
        ),
        page=1,
    )

    assert len(
        rows
    ) == 1

    row = rows[0]

    assert row.report_id == "123"

    assert row.stock_code == "005930"

    assert row.stock_name == "삼성전자"

    assert row.institution == "신한투자증권"

    assert row.published_at == "2026-08-07"

    assert row.views == 1234

    assert row.pdf_hint is True


def test_parse_research_detail_extracts_metadata_content_and_pdf():

    list_item = parse_research_list(
        "company",
        """
        <table class="type_1">
          <tr>
            <td><a href="/item/main.naver?code=005930">삼성전자</a></td>
            <td><a href="/research/company_read.naver?nid=123&page=1">기업 리포트</a></td>
            <td>신한투자증권</td>
            <td></td>
            <td>26.08.07</td>
            <td>10</td>
          </tr>
        </table>
        """,
        list_url=research_list_url(
            "company",
            1,
        ),
        page=1,
    )[0]

    html = """
    <table>
      <tr>
        <th class="view_sbj">
          기업 리포트
          <p class="source">한국투자증권 | 2026년8월7일 | 조회수: 66</p>
        </th>
      </tr>
      <tr>
        <th class="view_report">
          <a href="/research/sample.pdf">PDF</a>
        </th>
      </tr>
      <tr>
        <td>투자의견</td><td>Buy</td>
      </tr>
      <tr>
        <td>목표주가</td><td>100,000</td>
      </tr>
      <tr>
        <td class="view_cnt">
          <script>remove()</script>
          <p>본문 내용입니다.</p>
        </td>
      </tr>
    </table>
    """

    detail = parse_research_detail(
        "company",
        "123",
        html,
        detail_url=(
            "https://finance.naver.com/"
            "research/company_read.naver?nid=123"
        ),
        list_item=list_item,
    )

    assert detail["report_id"] == "123"

    assert detail["institution"] == "한국투자증권"

    assert detail["published_at"] == "2026-08-07"

    assert detail["views"] == 66

    assert detail["stock_code"] == "005930"

    assert detail["instrument_ids"] == [
        "XKRX:005930",
    ]

    assert detail["summary"] == "본문 내용입니다."

    assert detail["pdf_url"] == (
        "https://finance.naver.com/"
        "research/sample.pdf"
    )

    assert detail["investment_opinion"] == "Buy"

    assert detail["target_price"] == 100000


def test_client_fetch_list_decodes_euc_kr_and_sets_referer():

    html = """
    <table class="type_1">
      <tr>
        <td><a href="/research/market_info_read.naver?nid=77&page=1">시장 리포트</a></td>
        <td>NH투자증권</td>
        <td></td>
        <td>26.08.07</td>
        <td>5</td>
      </tr>
    </table>
    """.encode(
        "euc-kr"
    )

    session = FakeSession(
        [
            FakeResponse(
                url=research_list_url(
                    "market",
                    1,
                ),
                content=html,
            )
        ]
    )

    client = NaverResearchClient(
        session=session,
        verify_ssl=False,
    )

    rows = client.fetch_list(
        "market",
        page=1,
    )

    assert rows[0].title == "시장 리포트"

    assert session.calls[0]["headers"]["Referer"] == (
        "https://finance.naver.com/research/"
    )


def test_client_reports_403_to_rate_limiter():

    url = research_list_url(
        "market",
        1,
    )

    session = FakeSession(
        [
            FakeResponse(
                url=url,
                status_code=403,
            )
        ]
    )

    limiter = FakeRateLimiter()

    client = NaverResearchClient(
        session=session,
        retries=1,
        request_delay_seconds=0,
        verify_ssl=False,
        rate_limiter=limiter,
    )

    with pytest.raises(
        NaverResearchHTTPError
    ):

        client._get_text(
            url
        )

    assert limiter.events == [
        (
            "before",
            url,
        ),
        (
            "failure",
            url,
            True,
        ),
    ]


def test_client_crawl_relation_pages_yields_only_instrument_relations():

    class FakeResearchClient(NaverResearchClient):

        async def crawl_pages(
            self,
            **kwargs,
        ):

            del kwargs

            yield {
                "report_id": "market-1",
                "category": "market",
                "instrument_ids": [],
            }

            yield {
                "report_id": "company-1",
                "category": "company",
                "instrument_ids": [
                    "XKRX:005930",
                    "XKRX:000660",
                ],
            }

    client = FakeResearchClient()

    async def collect():

        return [
            row
            async for row in client.crawl_relation_pages(
                categories=(
                    "company",
                ),
                max_pages=1,
            )
        ]

    import asyncio

    rows = asyncio.run(
        collect()
    )

    assert [
        row[
            "instrument_id"
        ]
        for row in rows
    ] == [
        "XKRX:005930",
        "XKRX:000660",
    ]
