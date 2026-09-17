from datetime import datetime, timezone

import pytest

from crawl_framework.core.plugin import CrawlCheckpoint, CrawlContext, CrawlScope
from crawl_framework.sites.kabutan import (
    KabutanHTTPError,
    KabutanPlugin,
    KabutanMarketNewsClient,
    KabutanWafBlockedError,
    clean_text,
    current_tokyo_month,
    extract_news_id,
    has_locked_news_rows,
    iter_months,
    parse_article_page,
    parse_list_page,
    probe_kabutan_access,
    resolve_month_window,
)
from crawl_framework.transports.http import HttpResponse, HttpTransport


WAF_HTML = """
<!doctype html><html><head><title>Human Verification</title></head>
<body><script>window.awsWafCookieDomainList = []; window.gokuProps = {};</script>
</body></html>
"""


LIST_HTML = """
<html><body>
  <table class="s_news_list"><tbody>
    <tr>
      <td class="news_time">09/10 12:34</td>
      <td>市況</td>
      <td><a href="/news/marketnews/?b=n202609101234"> 日経平均\u3000反発 </a></td>
    </tr>
    <tr><td></td><td><a href="/not-news">ignored</a></td></tr>
  </tbody></table>
  <table class="s_news_list"><tbody>
    <tr>
      <td class="news_time">09/10 12:34</td>
      <td>市況</td>
      <td><a href="/news/marketnews/?b=n202609101234">日経平均 反発</a></td>
    </tr>
  </tbody></table>
</body></html>
"""


def test_clean_text_and_extract_news_id():
    assert clean_text(" a\xa0 b\n\t c \u3000d ") == "a b\nc d"
    assert extract_news_id("/news/?b=n202609101234&page=9") == "n202609101234"
    assert extract_news_id("/news/no-id") is None


def test_parse_list_page_deduplicates_duplicate_tables():
    items = parse_list_page(LIST_HTML)

    assert len(items) == 1
    assert items[0].news_id == "n202609101234"
    assert items[0].title == "日経平均 反発"
    assert items[0].category == "市況"
    assert items[0].list_time == "09/10 12:34"
    assert items[0].url == "https://kabutan.jp/news/marketnews/?b=n202609101234"


def test_parse_list_page_ignores_missing_id_or_title():
    html = """
    <table class="s_news_list"><tbody>
      <tr><td></td><td><a href="/news/?b=n202609101111"></a></td></tr>
      <tr><td></td><td><a href="/other">Other</a></td></tr>
    </tbody></table>
    """
    assert parse_list_page(html) == []


def test_parse_list_page_supports_real_table_rows_without_tbody():
    html = """
    <table class="s_news_list mgbt0">
      <tr>
        <td class="news_time"><time datetime="2026-09-10T15:50:41+09:00">
          26/09/10&nbsp;15:50</time></td>
        <td><div class="newslist_ctg newsctg9_b">注目</div></td>
        <td><a href="/news/marketnews/?&amp;b=n202609100904">夕刊</a></td>
      </tr>
    </table>
    """

    items = parse_list_page(html)

    assert len(items) == 1
    assert items[0].news_id == "n202609100904"
    assert items[0].category == "注目"
    assert items[0].list_time == "26/09/10 15:50"


def test_locked_archive_rows_are_distinct_from_natural_empty_page():
    html = """
    <table class="s_news_list mgbt0">
      <tr>
        <td class="news_time"><time datetime="2026-07-31T23:59:00+09:00">
          26/07/31 23:59</time></td>
        <td><div class="newslist_ctg newsctg2_b">材料</div></td>
        <td><span class="fin_modal vtlink">会員向け記事</span></td>
      </tr>
    </table>
    """

    assert parse_list_page(html) == []
    assert has_locked_news_rows(html) is True
    assert has_locked_news_rows("<table class='s_news_list'></table>") is False


def test_same_news_id_across_month_and_page_has_same_identity():
    plugin = KabutanPlugin()
    raw = parse_list_page(LIST_HTML)[0].to_raw()
    raw["page"] = 99

    august = plugin.normalize(
        "news_article",
        raw,
        CrawlScope("month", "202608", "2026-08"),
    )
    september = plugin.normalize(
        "news_article",
        {**raw, "page": 1},
        CrawlScope("month", "202609", "2026-09"),
    )

    assert august.source_id == "n202609101234"
    assert august.scope_type == "month"
    assert august.scope_id == "2026-08"
    assert august.record_uid == september.record_uid
    assert august.instrument_id is None
    assert august.relations == []


def test_parse_article_page_prefers_detail_fields_and_cleans_body():
    fallback = parse_list_page(LIST_HTML)[0]
    html = """
    <article>
      <h1> 詳細\u3000タイトル </h1>
      <time class="s_news_date" datetime="2026-09-10T08:00:00+09:00"></time>
      <div class="body">
        第一段\n  本文
        <script>bad()</script><div class="ads_box">ad</div>
        <div class="sns">share</div><div class="related">related</div>
        <p>第二段</p>
      </div>
    </article>
    """

    article = parse_article_page(html, fallback)

    assert article.article_found is True
    assert article.body_found is True
    assert article.title == "詳細 タイトル"
    assert article.published_at == "2026-09-10T08:00:00+09:00"
    assert article.content == "第一段\n本文\n第二段"
    assert "bad" not in article.content
    assert "share" not in article.content


def test_parse_article_page_uses_fallback_datetime_selector_and_category():
    article = parse_article_page(
        """
        <article>
          <h1>Title</h1>
          <time datetime="2026-09-09T23:30:00+09:00"></time>
          <span class="news_category">決算</span>
          <div class="body">Body</div>
        </article>
        """,
        {"news_id": "n202609091111", "url": "https://example.test"},
    )

    assert article.published_at == "2026-09-09T23:30:00+09:00"
    assert article.category == "決算"


def test_parse_article_page_does_not_infer_datetime_from_list_time():
    article = parse_article_page(
        "<article><time datetime='not-a-date'></time><div class='body'>Body</div></article>",
        {
            "news_id": "n202609091111",
            "title": "Fallback",
            "list_time": "09/09 12:34",
        },
    )

    assert article.published_at is None
    assert article.title == "Fallback"


def test_parse_article_page_marks_missing_article_and_body():
    missing_article = parse_article_page(
        "<html><body>maintenance</body></html>",
        {"news_id": "n202609091111", "title": "Fallback"},
    )
    missing_body = parse_article_page(
        "<article><h1>Title</h1></article>",
        {"news_id": "n202609091111"},
    )

    assert missing_article.article_found is False
    assert missing_article.body_found is False
    assert missing_article.title == "Fallback"
    assert missing_body.article_found is True
    assert missing_body.body_found is False


def test_parse_article_page_supports_real_mono_market_template():
    article = parse_article_page(
        """
        <article>
          <time class="s_news_date" datetime="2026-08-31T22:00:51+09:00"></time>
          <h1><span>【市況】</span>日経225先物</h1>
          <div class="mono">
            <div class="newsimg_leftbox">chart caption</div>
            本文一行<br><br>株探ニュース
          </div>
        </article>
        """,
        {"news_id": "n202608311023"},
    )

    assert article.body_found is True
    assert article.content == "chart caption\n本文一行\n株探ニュース"
    assert article.published_at == "2026-08-31T22:00:51+09:00"


def test_normalize_uses_detail_datetime_without_instrument_relations():
    plugin = KabutanPlugin()
    article = parse_article_page(
        """
        <article><h1>Title</h1>
        <time class="s_news_date" datetime="2026-09-10T08:00:00+09:00"></time>
        <div class="body">Body</div></article>
        """,
        {"news_id": "n202609101234", "url": "https://kabutan.jp/news/x"},
    )

    record = plugin.normalize(
        "news_article",
        article.to_raw(),
        CrawlScope("month", "202609", "2026-09"),
    )

    assert record.event_time.isoformat() == "2026-09-09T23:00:00+00:00"
    assert record.content == "Body"
    assert record.instrument_id is None
    assert record.relations == []


def test_month_iteration_starts_at_legacy_boundary_and_crosses_year():
    assert tuple(iter_months("2013-09", "2013-11")) == (
        "2013-09",
        "2013-10",
        "2013-11",
    )
    assert tuple(iter_months("2025-11", "2026-02")) == (
        "2025-11",
        "2025-12",
        "2026-01",
        "2026-02",
    )


def test_month_window_is_dynamic_and_incremental_overlaps_previous_month():
    now = datetime(2026, 1, 31, 16, 0, tzinfo=timezone.utc)

    assert current_tokyo_month(now) == "2026-02"
    assert resolve_month_window(now=now, overlap_months=1) == (
        "2026-01",
        "2026-02",
    )


def test_full_month_window_starts_at_2013_09():
    months = resolve_month_window(
        mode="full",
        now=datetime(2013, 10, 1),
    )
    assert months == ("2013-10", "2013-09")


def test_free_full_month_window_scans_from_current_toward_history():
    assert resolve_month_window(
        mode="free_full",
        start_month="2025-12",
        end_month="2026-02",
    ) == ("2026-02", "2026-01", "2025-12")


def test_invalid_month_window_is_rejected():
    with pytest.raises(ValueError, match="invalid month"):
        tuple(iter_months("2026-13", "2027-01"))
    with pytest.raises(ValueError, match="after end"):
        tuple(iter_months("2026-02", "2026-01"))


@pytest.mark.asyncio
async def test_plugin_discovers_month_scopes_without_instruments():
    plugin = KabutanPlugin()
    context = CrawlContext(
        extra={
            "kabutan_mode": "incremental",
            "kabutan_overlap_months": 1,
            "kabutan_now": datetime(2026, 1, 15),
        }
    )

    scopes = [scope async for scope in plugin.discover("news_article", context)]

    assert [(scope.scope_type, scope.scope_id, scope.source_key) for scope in scopes] == [
        ("month", "2025-12", "202512"),
        ("month", "2026-01", "202601"),
    ]
    assert scopes[0].metadata == {"year": 2025, "month": 12}


@pytest.mark.asyncio
async def test_free_full_discovery_stops_after_first_locked_month():
    requested = []

    async def fetch_list(year, month, page):
        requested.append((year, month, page))
        if month == 1:
            return """
            <table class="s_news_list"><tr><td class="news_time">26/01/31</td>
            <td><span class="fin_modal vtlink">locked</span></td></tr></table>
            """
        return _list_html(f"n{year:04d}{month:02d}010001")

    async def fetch_detail(url):
        raise AssertionError("discovery does not fetch details")

    plugin = KabutanPlugin(client=KabutanMarketNewsClient(fetch_list, fetch_detail))
    scopes = [scope async for scope in plugin.discover(
        "news_article",
        CrawlContext(extra={
            "kabutan_mode": "free_full",
            "kabutan_start_month": "2025-12",
            "kabutan_end_month": "2026-02",
        }),
    )]

    assert [scope.scope_id for scope in scopes] == ["2026-02", "2026-01"]
    assert requested == [(2026, 2, 1), (2026, 1, 1)]


@pytest.mark.asyncio
async def test_free_full_preflight_waf_stops_before_first_scope():
    async def fetch_list(year, month, page):
        raise KabutanWafBlockedError("verified WAF")

    async def fetch_detail(url):
        raise AssertionError("preflight WAF must not fetch details")

    plugin = KabutanPlugin(client=KabutanMarketNewsClient(fetch_list, fetch_detail))
    scopes = plugin.discover(
        "news_article",
        CrawlContext(extra={
            "kabutan_mode": "free_full",
            "kabutan_start_month": "2026-08",
            "kabutan_end_month": "2026-09",
        }),
    )

    with pytest.raises(KabutanWafBlockedError, match="verified WAF"):
        await anext(scopes)


def _list_html(*news_ids: str) -> str:
    rows = "".join(
        f"""
        <tr><td class="news_time">09/10 12:34</td><td>市況</td>
        <td><a href="/news/?b={news_id}">{news_id}</a></td></tr>
        """
        for news_id in news_ids
    )
    return f"<table class='s_news_list'><tbody>{rows}</tbody></table>"


def _detail_html(news_id: str) -> str:
    return f"""
    <article><h1>{news_id}</h1>
    <time class="s_news_date" datetime="2026-09-10T08:00:00+09:00"></time>
    <div class="body">body {news_id}</div></article>
    """


@pytest.mark.asyncio
async def test_month_crawl_stops_on_empty_page():
    pages = {1: ("n202609100101",), 2: ("n202609100102",), 3: ()}
    requested_pages = []
    detail_ids = []

    async def fetch_list(year, month, page):
        requested_pages.append(page)
        return _list_html(*pages[page])

    async def fetch_detail(url):
        news_id = extract_news_id(url)
        detail_ids.append(news_id)
        return _detail_html(news_id)

    client = KabutanMarketNewsClient(fetch_list, fetch_detail)
    records = [row async for row in client.crawl_month(2026, 9, max_pages=9)]

    assert [row["news_id"] for row in records] == [
        "n202609100101",
        "n202609100102",
    ]
    assert requested_pages == [1, 2, 3]
    assert detail_ids == ["n202609100101", "n202609100102"]
    assert client.scope_states["202609"]["month_complete"] is True
    assert client.scope_states["202609"]["stop_reason"] == "empty_page"


@pytest.mark.asyncio
async def test_repeated_signature_stops_before_detail_fetch():
    pages = {
        1: ("n202609100101", "n202609100102"),
        2: ("n202609100099", "n202609100098"),
        3: ("n202609100099", "n202609100098"),
        4: ("n202609100099", "n202609100098"),
    }
    requested_pages = []
    detail_ids = []

    async def fetch_list(year, month, page):
        requested_pages.append(page)
        return _list_html(*pages[page])

    async def fetch_detail(url):
        news_id = extract_news_id(url)
        detail_ids.append(news_id)
        return _detail_html(news_id)

    client = KabutanMarketNewsClient(fetch_list, fetch_detail)
    records = [row async for row in client.crawl_month(2026, 9, max_pages=9)]

    assert len(records) == 4
    assert requested_pages == [1, 2, 3]
    assert detail_ids == list(pages[1] + pages[2])
    assert client.scope_states["202609"]["stop_reason"] == "repeated_page_signature"


@pytest.mark.asyncio
async def test_repeated_month_ids_stop_before_detail_when_order_changes():
    pages = {
        1: ("n202609100101", "n202609100102"),
        2: ("n202609100102", "n202609100101"),
    }
    detail_ids = []

    async def fetch_list(year, month, page):
        return _list_html(*pages[page])

    async def fetch_detail(url):
        news_id = extract_news_id(url)
        detail_ids.append(news_id)
        return _detail_html(news_id)

    client = KabutanMarketNewsClient(fetch_list, fetch_detail)
    records = [row async for row in client.crawl_month(2026, 9, max_pages=9)]

    assert len(records) == 2
    assert detail_ids == list(pages[1])
    assert client.scope_states["202609"]["stop_reason"] == "repeated_page_ids"


@pytest.mark.asyncio
async def test_max_pages_is_partial_safety_cap():
    requested_pages = []

    async def fetch_list(year, month, page):
        requested_pages.append(page)
        return _list_html(f"n20260910{page:04d}")

    async def fetch_detail(url):
        return _detail_html(extract_news_id(url))

    client = KabutanMarketNewsClient(fetch_list, fetch_detail)
    records = [row async for row in client.crawl_month(2026, 9, max_pages=2)]

    assert len(records) == 2
    assert requested_pages == [1, 2]
    assert client.scope_states["202609"] == {
        "year": 2026,
        "month": 9,
        "last_completed_page": 2,
        "last_news_id": "n202609100002",
        "month_complete": False,
        "free_access_complete": False,
        "stop_reason": "safety_capped",
        "archive_access_validated": False,
        "access_state": "available",
    }


@pytest.mark.asyncio
async def test_locked_archive_page_is_a_normal_free_access_boundary():
    detail_calls = []

    async def fetch_list(year, month, page):
        return """
        <table class="s_news_list"><tr>
          <td class="news_time">26/07/31 23:59</td>
          <td>材料</td><td><span class="fin_modal vtlink">locked</span></td>
        </tr></table>
        """

    async def fetch_detail(url):
        detail_calls.append(url)
        raise AssertionError("locked rows must stop before detail")

    client = KabutanMarketNewsClient(fetch_list, fetch_detail)
    records = [row async for row in client.crawl_month(2026, 7)]

    assert records == []
    assert detail_calls == []
    assert client.scope_states["202607"]["month_complete"] is False
    assert client.scope_states["202607"]["free_access_complete"] is True
    assert client.scope_states["202607"]["stop_reason"] == "free_access_boundary"
    assert client.scope_states["202607"]["archive_access_validated"] is False


@pytest.mark.asyncio
async def test_mixed_page_keeps_public_rows_then_stops_at_free_boundary():
    detail_calls = []

    async def fetch_list(year, month, page):
        return """
        <table class="s_news_list">
          <tr><td class="news_time">26/08/14 12:00</td><td>市況</td>
            <td><a href="/news/marketnews/?b=n202608140001">open</a></td></tr>
          <tr><td class="news_time">26/08/14 11:59</td><td>材料</td>
            <td><span class="fin_modal vtlink">locked</span></td></tr>
        </table>
        """

    async def fetch_detail(url):
        detail_calls.append(url)
        return _detail_html(extract_news_id(url))

    client = KabutanMarketNewsClient(fetch_list, fetch_detail)
    records = [row async for row in client.crawl_month(2026, 8)]

    assert [row["news_id"] for row in records] == ["n202608140001"]
    assert len(detail_calls) == 1
    assert client.scope_states["202608"]["month_complete"] is False
    assert client.scope_states["202608"]["free_access_complete"] is True
    assert client.scope_states["202608"]["last_completed_page"] == 1


@pytest.mark.asyncio
async def test_max_pages_is_a_resume_window_not_an_absolute_page_number():
    requested_pages = []

    async def fetch_list(year, month, page):
        requested_pages.append(page)
        return _list_html(f"n20260910{page:04d}")

    async def fetch_detail(url):
        return _detail_html(extract_news_id(url))

    client = KabutanMarketNewsClient(fetch_list, fetch_detail)
    records = [
        row async for row in client.crawl_month(
            2026,
            9,
            start_page=501,
            max_pages=2,
            prior_last_news_id="n202609100500",
        )
    ]

    assert len(records) == 2
    assert requested_pages == [501, 502]
    assert client.scope_states["202609"]["last_completed_page"] == 502
    assert client.scope_states["202609"]["stop_reason"] == "safety_capped"


@pytest.mark.asyncio
async def test_real_client_adapter_uses_kabutan_params_and_japanese_headers():
    requests = []

    async def requester(url, kwargs):
        requests.append((url, kwargs))
        if kwargs.get("params"):
            content = _list_html("n202609100101").encode()
        else:
            content = _detail_html("n202609100101").encode()
        return HttpResponse(status_code=200, content=content, url=url)

    client = KabutanMarketNewsClient.from_transport(
        HttpTransport(requester=requester)
    )
    records = [row async for row in client.crawl_month(2026, 9, max_pages=1)]

    assert len(records) == 1
    list_url, list_kwargs = requests[0]
    assert list_url == "https://kabutan.jp/news/marketnews/"
    assert list_kwargs["params"] == {
        "category": -1,
        "date": "20260900",
        "page": 1,
    }
    assert list_kwargs["headers"]["Accept-Language"].startswith("ja-JP")
    assert list_kwargs["headers"]["Referer"] == list_url


@pytest.mark.asyncio
async def test_405_human_verification_raises_explicit_waf_error_without_retry():
    calls = 0

    async def requester(url, kwargs):
        nonlocal calls
        calls += 1
        return HttpResponse(status_code=405, content=WAF_HTML.encode(), url=url)

    client = KabutanMarketNewsClient.from_transport(HttpTransport(
        requester=requester,
    ))

    with pytest.raises(KabutanWafBlockedError, match="AWS WAF"):
        [row async for row in client.crawl_month(2026, 9)]

    assert calls == 1
    assert client.scope_states["202609"] == {
        "year": 2026,
        "month": 9,
        "last_completed_page": 0,
        "last_news_id": None,
        "month_complete": False,
        "free_access_complete": False,
        "stop_reason": "waf_human_verification",
        "archive_access_validated": False,
        "access_state": "temporarily_blocked",
    }


@pytest.mark.asyncio
async def test_generic_405_without_waf_markers_remains_http_error():
    async def requester(url, kwargs):
        return HttpResponse(
            status_code=405,
            content=b"Method Not Allowed",
            url=url,
        )

    client = KabutanMarketNewsClient.from_transport(HttpTransport(
        requester=requester,
    ))

    with pytest.raises(KabutanHTTPError, match="Kabutan HTTP 405") as raised:
        [row async for row in client.crawl_month(2026, 9)]

    assert not isinstance(raised.value, KabutanWafBlockedError)
    assert client.scope_states["202609"]["stop_reason"] == "http_failure"


@pytest.mark.asyncio
async def test_access_probe_reports_available_real_dom():
    async def requester(url, kwargs):
        return HttpResponse(
            status_code=200,
            content=_list_html("n202609100101").encode(),
            url=url,
        )

    result = await probe_kabutan_access(
        HttpTransport(requester=requester),
        now=datetime(2026, 9, 11, tzinfo=timezone.utc),
    )

    assert result.http_status == 200
    assert result.kabutan_page_detected is True
    assert result.waf_human_verification is False
    assert result.access_state == "AVAILABLE"


@pytest.mark.asyncio
async def test_access_probe_reports_verified_waf_block():
    calls = 0

    async def requester(url, kwargs):
        nonlocal calls
        calls += 1
        return HttpResponse(status_code=405, content=WAF_HTML.encode(), url=url)

    result = await probe_kabutan_access(
        HttpTransport(requester=requester),
        now=datetime(2026, 9, 11, tzinfo=timezone.utc),
    )

    assert calls == 1
    assert result.http_status == 405
    assert result.kabutan_page_detected is False
    assert result.waf_human_verification is True
    assert result.access_state == "WAF_BLOCKED"


@pytest.mark.asyncio
async def test_http_failure_is_not_marked_complete():
    async def fetch_list(year, month, page):
        raise RuntimeError("network down")

    async def fetch_detail(url):
        raise AssertionError("detail must not run")

    client = KabutanMarketNewsClient(fetch_list, fetch_detail)
    with pytest.raises(RuntimeError, match="network down"):
        [row async for row in client.crawl_month(2026, 9)]

    assert client.scope_states["202609"]["month_complete"] is False
    assert client.scope_states["202609"]["stop_reason"] == "http_failure"


@pytest.mark.asyncio
async def test_plugin_resumes_after_last_durable_page_and_builds_candidate_checkpoint():
    requested_pages = []

    async def fetch_list(year, month, page):
        requested_pages.append(page)
        return "<table class='s_news_list'><tbody></tbody></table>"

    async def fetch_detail(url):
        raise AssertionError("empty resume page has no detail")

    client = KabutanMarketNewsClient(fetch_list, fetch_detail)
    plugin = KabutanPlugin(client=client)
    scope = CrawlScope("month", "202609", "2026-09", {"year": 2026, "month": 9})
    original = CrawlCheckpoint(state={
        "year": 2026,
        "month": 9,
        "last_completed_page": 2,
        "last_news_id": "n202609100099",
        "month_complete": False,
        "stop_reason": "safety_capped",
    })

    records = [
        row async for row in plugin.crawl(
            "news_article",
            scope,
            original,
                CrawlContext(extra={
                    "kabutan_mode": "full",
                    "kabutan_max_pages_per_month": 10,
                    "kabutan_now": datetime(2026, 10, 1),
                }),
        )
    ]
    candidate = plugin.checkpoint_after_scope(
        "news_article", scope, original, CrawlContext()
    )

    assert records == []
    assert requested_pages == [3]
    assert candidate.state["last_completed_page"] == 2
    assert candidate.state["last_news_id"] == "n202609100099"
    assert candidate.state["month_complete"] is True
    assert candidate.state["stop_reason"] == "empty_page"
    assert candidate.state["archive_access_validated"] is True


@pytest.mark.asyncio
async def test_free_full_revalidates_legacy_complete_checkpoint_without_access_marker():
    requested_pages = []

    async def fetch_list(year, month, page):
        requested_pages.append(page)
        return """
        <table class="s_news_list"><tr>
          <td class="news_time">26/07/01 12:00</td>
          <td><span class="fin_modal vtlink">locked</span></td>
        </tr></table>
        """

    async def fetch_detail(url):
        raise AssertionError("locked rows have no detail")

    plugin = KabutanPlugin(client=KabutanMarketNewsClient(fetch_list, fetch_detail))
    scope = CrawlScope("month", "202607", "2026-07", {"year": 2026, "month": 7})
    legacy_complete = CrawlCheckpoint(state={
        "last_completed_page": 0,
        "month_complete": True,
        "stop_reason": "empty_page",
    })

    records = [row async for row in plugin.crawl(
        "news_article",
        scope,
        legacy_complete,
        CrawlContext(extra={"kabutan_mode": "free_full"}),
    )]

    assert records == []
    assert requested_pages == [1]
    assert plugin.client.scope_states["202607"]["free_access_complete"] is True


@pytest.mark.asyncio
async def test_full_mode_skips_access_validated_complete_checkpoint():
    async def fetch_list(year, month, page):
        raise AssertionError("validated complete checkpoint must skip HTTP")

    async def fetch_detail(url):
        raise AssertionError("validated complete checkpoint must skip detail")

    plugin = KabutanPlugin(client=KabutanMarketNewsClient(fetch_list, fetch_detail))
    scope = CrawlScope("month", "202608", "2026-08", {"year": 2026, "month": 8})
    complete = CrawlCheckpoint(state={
        "last_completed_page": 378,
        "month_complete": True,
        "stop_reason": "empty_page",
        "archive_access_validated": True,
    })

    records = [row async for row in plugin.crawl(
        "news_article",
        scope,
        complete,
        CrawlContext(extra={"kabutan_mode": "free_full"}),
    )]

    assert records == []


@pytest.mark.asyncio
async def test_incremental_mode_rescans_completed_overlap_month_from_page_one():
    requested_pages = []

    async def fetch_list(year, month, page):
        requested_pages.append(page)
        return "<table class='s_news_list'><tbody></tbody></table>"

    async def fetch_detail(url):
        raise AssertionError("empty page has no detail")

    plugin = KabutanPlugin(client=KabutanMarketNewsClient(fetch_list, fetch_detail))
    scope = CrawlScope("month", "202609", "2026-09", {"year": 2026, "month": 9})
    complete = CrawlCheckpoint(state={
        "last_completed_page": 255,
        "last_news_id": "n202609100904",
        "month_complete": True,
        "stop_reason": "empty_page",
    })

    records = [
        row async for row in plugin.crawl(
            "news_article",
            scope,
            complete,
            CrawlContext(extra={"kabutan_mode": "incremental"}),
        )
    ]

    assert records == []
    assert requested_pages == [1]
