from crawl_framework.web.naver.market_sum import (
    NAVER_MARKET_SUM_URL,
    NaverMarketSumClient,
    naver_canonical_instrument_id,
    parse_market_sum_page,
)

import requests


def page(
    *codes,
):

    links = "\n".join(
        (
            f'<a class="tltle" '
            f'href="/item/main.naver?code={code}">'
            f"Name {code}</a>"
        )
        for code in codes
    )

    return f"<html><body>{links}</body></html>"


def test_parse_market_sum_page_extracts_canonical_instruments():

    instruments = parse_market_sum_page(
        page(
            "005930",
            "000660",
        ),
        market="KOSPI",
    )

    assert [
        instrument.instrument_id
        for instrument in instruments
    ] == [
        "XKRX:005930",
        "XKRX:000660",
    ]

    assert (
        instruments[0].market
        == "KOSPI"
    )

    assert (
        instruments[0].main_url
        == (
            "https://finance.naver.com/"
            "item/main.naver?code=005930"
        )
    )


def test_parse_market_sum_page_deduplicates_within_page():

    instruments = parse_market_sum_page(
        page(
            "005930",
            "005930",
        ),
        market="KOSPI",
    )

    assert [
        instrument.instrument_id
        for instrument in instruments
    ] == [
        "XKRX:005930",
    ]


def test_naver_canonical_instrument_id_requires_six_digits():

    assert (
        naver_canonical_instrument_id(
            "005930"
        )
        == "XKRX:005930"
    )


def test_discover_market_paginates_until_empty_page():

    calls = []

    def get_html(
        url,
        params,
    ):

        calls.append(
            (
                url,
                dict(
                    params
                ),
            )
        )

        if params[
            "page"
        ] == 1:

            return page(
                "005930",
                "000660",
            )

        return ""

    client = NaverMarketSumClient(
        get_html=get_html,
        page_size_stop_threshold=2,
    )

    instruments = client.discover_market(
        market="KOSPI",
        sosok=0,
    )

    assert [
        instrument.instrument_id
        for instrument in instruments
    ] == [
        "XKRX:005930",
        "XKRX:000660",
    ]

    assert calls == [
        (
            NAVER_MARKET_SUM_URL,
            {
                "sosok": 0,
                "page": 1,
            },
        ),
        (
            NAVER_MARKET_SUM_URL,
            {
                "sosok": 0,
                "page": 2,
            },
        ),
    ]


def test_discover_all_preserves_market_order_and_deduplicates():

    def get_html(
        url,
        params,
    ):

        if params[
            "sosok"
        ] == 0:

            return page(
                "005930",
                "000660",
            )

        return page(
            "000660",
            "035720",
        )

    client = NaverMarketSumClient(
        get_html=get_html,
        page_size_stop_threshold=10,
    )

    instruments = client.discover_all()

    assert [
        instrument.instrument_id
        for instrument in instruments
    ] == [
        "XKRX:005930",
        "XKRX:000660",
        "XKRX:035720",
    ]


def test_requests_get_html_retries_transient_request_error():

    class Response:

        encoding = None
        text = page(
            "005930"
        )

        def raise_for_status(
            self,
        ):

            return None

    class Session:

        def __init__(
            self,
        ):

            self.calls = 0

        def get(
            self,
            *args,
            **kwargs,
        ):

            self.calls += 1

            if self.calls == 1:

                raise requests.exceptions.SSLError(
                    "temporary eof"
                )

            return Response()

    client = NaverMarketSumClient(
        request_retries=2,
        retry_sleep_seconds=0,
    )

    client.session = Session()

    html = client._requests_get_html(
        NAVER_MARKET_SUM_URL,
        {
            "sosok": 0,
            "page": 1,
        },
    )

    assert (
        "005930"
        in html
    )

    assert (
        client.session.calls
        == 2
    )
