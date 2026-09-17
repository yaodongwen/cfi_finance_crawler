from datetime import datetime, timezone

import pytest

from crawl_framework.sites.hkexnews import (
    HKEXFilingsClient,
    HKEXNewsParserError,
    canonical_hkex_instrument_id,
    canonicalize_hkex_pdf_url,
    build_hkex_report_source_id,
    normalize_hkex_stock_code,
    parse_hkex_stock_codes,
    normalize_hkex_report_type,
    normalize_hkex_search_date,
    parse_jsonp,
    parse_hkex_release_time,
    parse_report_results,
    select_exact_stock_info,
)


def test_normalize_hkex_stock_code_and_canonical_id():
    assert normalize_hkex_stock_code("5") == "00005"
    assert normalize_hkex_stock_code("700.0") == "00700"


def test_parse_hkex_stock_codes_supports_dual_counter_field():
    assert parse_hkex_stock_codes("00016 80016") == ("00016", "80016")
    assert parse_hkex_stock_codes("16 / 80016 / 16") == ("00016", "80016")
    assert canonical_hkex_instrument_id("1288") == "XHKG:01288"


@pytest.mark.parametrize("value", ("", "ABC", "123456", None))
def test_normalize_hkex_stock_code_rejects_invalid_values(value):
    with pytest.raises(ValueError, match="invalid HKEX stock code"):
        normalize_hkex_stock_code(value)


def test_parse_jsonp_accepts_callback_and_optional_semicolon():
    assert parse_jsonp('callback({"stockInfo": []});') == {"stockInfo": []}
    assert parse_jsonp('jQuery_1 ( {"ok": true} )') == {"ok": True}


@pytest.mark.parametrize("text", ("", "{}", "callback([])", "callback({bad})"))
def test_parse_jsonp_rejects_invalid_payload(text):
    with pytest.raises(HKEXNewsParserError):
        parse_jsonp(text)


def test_select_stock_info_requires_exact_normalized_code():
    payload = {
        "stockInfo": [
            {"stockId": 1, "code": "00050", "name": "Wrong prefix"},
            {"stockId": "5", "code": "5", "name": " HSBC "},
            {"stockId": 6, "code": "00005", "name": "Duplicate"},
        ]
    }

    result = select_exact_stock_info(payload, "00005")

    assert result is not None
    assert result.stock_id == 5
    assert result.code == "00005"
    assert result.name == "HSBC"


def test_select_stock_info_does_not_take_first_prefix_suggestion():
    payload = {"stockInfo": [{"stockId": 50, "code": "00050"}]}

    assert select_exact_stock_info(payload, "00005") is None


@pytest.mark.asyncio
async def test_client_builds_legacy_lookup_request_and_resolves_exact_match():
    requests = []

    async def fetch(request):
        requests.append(request)
        return 'callback({"stockInfo":[{"stockId":49784,"code":"01288","name":"ABC"}]})'

    client = HKEXFilingsClient(fetch, timestamp_ms=lambda: 1234)

    result = await client.resolve_stock("1288")

    assert result is not None
    assert result.stock_id == 49784
    assert requests[0].params == {
        "callback": "callback",
        "lang": "EN",
        "type": "A",
        "name": "01288",
        "market": "SEHK",
        "_": "1234",
    }


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        ("annual", ("annual", "40100")),
        ("40200", ("interim", "40200")),
        ("quarterly", ("quarterly", "40300")),
    ),
)
def test_normalize_hkex_report_type(value, expected):
    assert normalize_hkex_report_type(value) == expected


def test_normalize_hkex_search_date_validates_calendar_date():
    assert normalize_hkex_search_date("1999-04-01") == "19990401"
    with pytest.raises(ValueError, match="invalid HKEX search date"):
        normalize_hkex_search_date("2026-02-30")


def test_build_report_search_request_preserves_legacy_form():
    async def unused(request):
        raise AssertionError(request)

    client = HKEXFilingsClient(unused)
    request = client.build_report_search_request(
        49784,
        "interim",
        date_from="1999-04-01",
        date_to="2026-09-11",
    )

    assert request.method == "POST"
    assert request.params is None
    assert request.data == {
        "lang": "EN",
        "category": "0",
        "market": "SEHK",
        "searchType": "1",
        "documentType": "-1",
        "t1code": "40000",
        "t2Gcode": "-2",
        "t2code": "40200",
        "stockId": "49784",
        "from": "19990401",
        "to": "20260911",
        "MB-Daterange": "0",
        "title": "",
    }
    assert request.headers["Referer"].endswith("titlesearch.xhtml?lang=en")


@pytest.mark.asyncio
async def test_search_report_types_uses_each_native_code_once():
    requests = []

    async def fetch(request):
        requests.append(request)
        return request.data["t2code"]

    client = HKEXFilingsClient(fetch)
    results = await client.search_report_types(
        1,
        ["annual", "40100", "interim", "quarterly"],
        date_from="20260101",
        date_to="20260911",
    )

    assert results == {
        "annual": "40100",
        "interim": "40200",
        "quarterly": "40300",
    }
    assert [request.data["t2code"] for request in requests] == [
        "40100",
        "40200",
        "40300",
    ]


def test_report_search_rejects_reversed_dates():
    client = HKEXFilingsClient(lambda request: None)
    with pytest.raises(ValueError, match="date_from"):
        client.build_report_search_request(
            1,
            "annual",
            date_from="20260911",
            date_to="20260101",
        )


def test_parse_report_results_uses_full_row_title_and_taxonomy():
    html = """
    <table class="result">
      <tr>
        <td>11/09/2026 16:33</td><td>5</td><td>HSBC Holdings</td>
        <td><a href="/listedco/listconews/sehk/2026/0911/a.pdf">Annual</a>
            Report 2025</td>
      </tr>
    </table>
    """

    reports = parse_report_results(html, "40100")

    assert len(reports) == 1
    report = reports[0]
    assert report.release_time_raw == "11/09/2026 16:33"
    assert report.stock_code == "00005"
    assert report.stock_name == "HSBC Holdings"
    assert report.title == "Annual Report 2025"
    assert report.report_type == "annual"
    assert report.report_type_code == "40100"
    assert report.pdf_url == (
        "https://www1.hkexnews.hk/listedco/listconews/sehk/2026/0911/a.pdf"
    )


def test_parse_report_results_strips_real_hkex_cell_labels():
    html = """
    <table><tr>
      <td>Release Time: 27/03/2026 07:30</td>
      <td>Stock Code: 00005</td>
      <td>Stock Short Name: HSBC HOLDINGS</td>
      <td>Document: Financial Statements - <a href="/docs/a.pdf">Annual</a></td>
    </tr></table>
    """

    report = parse_report_results(html, "annual")[0]

    assert report.release_time_raw == "27/03/2026 07:30"
    assert report.stock_code == "00005"
    assert report.stock_name == "HSBC HOLDINGS"
    assert report.title == "Financial Statements - Annual"


def test_parse_report_results_preserves_dual_counter_stock_codes():
    html = """
    <table><tr>
      <td>Release Time: 27/03/2026 07:30</td>
      <td>Stock Code: 00016 80016</td>
      <td>Stock Short Name: SHK PPT</td>
      <td>Document: <a href="/docs/a.pdf">Annual Report</a></td>
    </tr></table>
    """

    report = parse_report_results(html, "annual")[0]

    assert report.stock_code == "00016 80016"


def test_parse_report_results_deduplicates_duplicate_anchors():
    html = """
    <table><tr><td>date</td><td>00700</td><td>Tencent</td><td>
      <a href="/docs/a.pdf#page=1">Part one</a>
      <a href="https://www1.hkexnews.hk/docs/a.pdf">Part two</a>
    </td></tr></table>
    """

    reports = parse_report_results(html, "interim")

    assert len(reports) == 1
    assert reports[0].title == "Part one Part two"


def test_parse_report_results_ignores_pdf_outside_result_row_and_wrong_host():
    html = """
    <a href="/orphan.pdf">orphan</a>
    <table><tr><td>d</td><td>5</td><td>n</td><td>
      <a href="https://example.com/not-hkex.pdf">wrong host</a>
    </td></tr></table>
    """

    assert parse_report_results(html, "quarterly") == []


def test_canonicalize_hkex_pdf_url_preserves_query_but_removes_fragment():
    assert canonicalize_hkex_pdf_url("/docs/A.PDF?x=1#page=2") == (
        "https://www1.hkexnews.hk/docs/A.PDF?x=1"
    )


def test_report_source_id_is_sha256_of_canonical_url():
    first = build_hkex_report_source_id("/docs/a.pdf#page=1")
    second = build_hkex_report_source_id(
        "https://www1.hkexnews.hk/docs/a.pdf"
    )

    assert first == second
    assert len(first) == 64


@pytest.mark.parametrize(
    ("report_type", "expected_name", "expected_code"),
    (
        ("annual", "annual", "40100"),
        ("40200", "interim", "40200"),
        ("quarterly", "quarterly", "40300"),
    ),
)
def test_report_parser_taxonomy_matrix(report_type, expected_name, expected_code):
    html = """
    <table><tr><td>11/09/2026 16:33</td><td>5</td><td>HSBC</td><td>
      <a href="/docs/report.pdf">Report</a>
    </td></tr></table>
    """

    report = parse_report_results(html, report_type)[0]

    assert report.report_type == expected_name
    assert report.report_type_code == expected_code


def test_exact_stock_match_with_invalid_stock_id_is_parser_error():
    payload = {"stockInfo": [{"code": "00005", "stockId": "bad"}]}

    with pytest.raises(HKEXNewsParserError, match="invalid stockId"):
        select_exact_stock_info(payload, "00005")


@pytest.mark.asyncio
async def test_search_reports_deduplicates_same_pdf_across_categories():
    html = """
    <table><tr><td>d</td><td>5</td><td>HSBC</td><td>
      <a href="/docs/shared.pdf">Shared report</a>
    </td></tr></table>
    """

    async def fetch(request):
        return html

    reports = await HKEXFilingsClient(fetch).search_reports(
        1,
        ["annual", "interim"],
        date_from="20260101",
        date_to="20260911",
    )

    assert len(reports) == 1
    assert reports[0].report_type == "annual"


@pytest.mark.parametrize(
    "raw",
    (
        "2026年4月23日 16:33",
        "23/04/2026 16:33",
        "20260423 16:33",
        "202604231633",
    ),
)
def test_parse_hkex_release_time_returns_aware_utc(raw):
    assert parse_hkex_release_time(raw) == datetime(
        2026,
        4,
        23,
        8,
        33,
        tzinfo=timezone.utc,
    )


@pytest.mark.parametrize(
    "raw",
    (
        "",
        None,
        "2026年4月23日",
        "23/04/2026",
        "20260230 16:33",
        "unknown",
    ),
)
def test_parse_hkex_release_time_does_not_invent_missing_time(raw):
    assert parse_hkex_release_time(raw) is None
