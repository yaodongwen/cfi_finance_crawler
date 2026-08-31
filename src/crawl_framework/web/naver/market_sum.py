from __future__ import annotations

import re
import time

from dataclasses import dataclass
from typing import Callable, Iterable
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


NAVER_FINANCE_ROOT = "https://finance.naver.com"
NAVER_MARKET_SUM_URL = (
    NAVER_FINANCE_ROOT
    + "/sise/sise_market_sum.naver"
)

NAVER_MARKETS: tuple[
    tuple[
        str,
        int,
    ],
    ...
] = (
    (
        "KOSPI",
        0,
    ),
    (
        "KOSDAQ",
        1,
    ),
)


@dataclass(
    frozen=True,
    slots=True,
)
class NaverListedInstrument:
    code: str

    name: str

    market: str

    main_url: str

    instrument_id: str


def naver_canonical_instrument_id(
    code: str,
) -> str:

    value = str(
        code
    ).strip()

    if not re.fullmatch(
        r"\d{6}",
        value,
    ):

        raise ValueError(
            "Naver instrument code must be 6 digits"
        )

    return f"XKRX:{value}"


def parse_market_sum_page(
    html: str,
    *,
    market: str,
) -> tuple[
    NaverListedInstrument,
    ...
]:
    """
    Parse Naver market-cap list rows from one page.
    """

    soup = BeautifulSoup(
        html,
        "html.parser",
    )

    instruments: list[
        NaverListedInstrument
    ] = []

    seen: set[
        str
    ] = set()

    for anchor in soup.select(
        "a.tltle"
    ):

        href = anchor.get(
            "href",
            "",
        )

        match = re.search(
            r"code=(\d{6})",
            href,
        )

        if match is None:

            continue

        code = match.group(
            1
        )

        if code in seen:

            continue

        seen.add(
            code
        )

        instruments.append(
            NaverListedInstrument(
                code=code,
                name=anchor.get_text(
                    " ",
                    strip=True,
                ),
                market=market,
                main_url=urljoin(
                    NAVER_FINANCE_ROOT,
                    href,
                ),
                instrument_id=(
                    naver_canonical_instrument_id(
                        code
                    )
                ),
            )
        )

    return tuple(
        instruments
    )


HttpGetter = Callable[
    [
        str,
        dict[
            str,
            int,
        ],
    ],
    str,
]


class NaverMarketSumClient:
    """
    Naver-specific listed instrument discovery.
    """

    def __init__(
        self,
        *,
        get_html: HttpGetter | None = None,
        page_size_stop_threshold: int = 10,
        request_retries: int = 3,
        retry_sleep_seconds: float = 1.0,
        verify_ssl: bool = True,
    ) -> None:

        self.get_html = (
            get_html
            or self._requests_get_html
        )

        self.session = requests.Session()

        self.page_size_stop_threshold = (
            page_size_stop_threshold
        )

        if request_retries < 1:

            raise ValueError(
                "request_retries must be >= 1"
            )

        if retry_sleep_seconds < 0:

            raise ValueError(
                "retry_sleep_seconds must be >= 0"
            )

        self.request_retries = request_retries
        self.retry_sleep_seconds = retry_sleep_seconds
        self.verify_ssl = verify_ssl


    def discover_market(
        self,
        *,
        market: str,
        sosok: int,
    ) -> tuple[
        NaverListedInstrument,
        ...
    ]:

        page = 1

        result: list[
            NaverListedInstrument
        ] = []

        while True:

            html = self.get_html(
                NAVER_MARKET_SUM_URL,
                {
                    "sosok": sosok,
                    "page": page,
                },
            )

            instruments = parse_market_sum_page(
                html,
                market=market,
            )

            if not instruments:

                break

            result.extend(
                instruments
            )

            if (
                len(
                    instruments
                )
                < self.page_size_stop_threshold
            ):

                break

            page += 1

        return tuple(
            result
        )


    def discover_all(
        self,
        markets: Iterable[
            tuple[
                str,
                int,
            ]
        ] = NAVER_MARKETS,
    ) -> tuple[
        NaverListedInstrument,
        ...
    ]:

        result: list[
            NaverListedInstrument
        ] = []

        seen: set[
            str
        ] = set()

        for market, sosok in markets:

            for instrument in self.discover_market(
                market=market,
                sosok=sosok,
            ):

                if instrument.instrument_id in seen:

                    continue

                seen.add(
                    instrument.instrument_id
                )

                result.append(
                    instrument
                )

        return tuple(
            result
        )


    def _requests_get_html(
        self,
        url: str,
        params: dict[
            str,
            int,
        ],
    ) -> str:

        last_error: Exception | None = None

        for attempt in range(
            1,
            self.request_retries
            +
            1,
        ):

            try:

                response = self.session.get(
                    url,
                    params=params,
                    timeout=20,
                    verify=self.verify_ssl,
                    headers={
                        "User-Agent": (
                            "Mozilla/5.0 "
                            "(Macintosh; Intel Mac OS X 10_15_7) "
                            "AppleWebKit/537.36 "
                            "(KHTML, like Gecko) "
                            "Chrome/120 Safari/537.36"
                        ),
                        "Accept-Language": "ko-KR,ko;q=0.9",
                    },
                )

                response.raise_for_status()

                if not response.encoding:

                    response.encoding = "euc-kr"

                return response.text

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
