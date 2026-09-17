from pathlib import Path

import pytest
import yaml

from crawl_framework.core.plugin import CrawlCheckpoint, CrawlContext, CrawlScope
from crawl_framework.sites.hkexnews import HKEXReportListItem, HKEXStockInfo
from crawl_framework.sites.hkexnews import HKEXNewsPlugin


def test_hkexnews_plugin_has_financial_report_contract():
    plugin = HKEXNewsPlugin()

    assert plugin.site_id == "hkexnews"
    assert plugin.country == "HK"
    assert plugin.timezone == "Asia/Hong_Kong"
    assert plugin.validate_datasets() == (
        "financial_report",
        "financial_report_instrument",
        "attachment",
    )


def test_hkexnews_site_manifest_is_disabled_http_site():
    path = (
        Path(__file__).parents[2]
        / "src/crawl_framework/sites/hkexnews/site.yaml"
    )
    manifest = yaml.safe_load(path.read_text(encoding="utf-8"))

    assert manifest["site_id"] == "hkexnews"
    assert manifest["enabled"] is False
    assert manifest["transport"] == {"type": "http"}
    assert manifest["datasets"] == [
        "financial_report",
        "financial_report_instrument",
        "attachment",
    ]


@pytest.mark.asyncio
async def test_hkexnews_rejects_unsupported_dataset():
    plugin = HKEXNewsPlugin()

    with pytest.raises(ValueError, match="unsupported HKEXnews dataset"):
        await anext(plugin.discover("news_article", CrawlContext()))


@pytest.mark.asyncio
async def test_hkexnews_discovery_normalizes_and_deduplicates_instruments():
    plugin = HKEXNewsPlugin()

    scopes = [
        scope async for scope in plugin.discover(
            "financial_report",
            CrawlContext(extra={"instrument_codes": ("5", "XHKG:00005", "700")}),
        )
    ]

    assert [(scope.scope_id, scope.source_key) for scope in scopes] == [
        ("XHKG:00005", "00005"),
        ("XHKG:00700", "00700"),
    ]


@pytest.mark.asyncio
async def test_hkexnews_discovery_requires_instrument_universe():
    with pytest.raises(ValueError, match="requires instrument_codes"):
        await anext(
            HKEXNewsPlugin().discover("financial_report", CrawlContext())
        )


def _report_raw():
    return {
        "release_time_raw": "11/09/2026 16:33",
        "stock_code": "00005",
        "stock_name": "HSBC Holdings",
        "title": "Annual Report 2025",
        "pdf_url": "/listedco/listconews/sehk/2026/0911/a.pdf",
        "report_type": "annual",
        "report_type_code": "40100",
        "hkex_stock_id": 12345,
        "input_stock_name": "HSBC",
    }


def _scope():
    return CrawlScope(
        scope_type="instrument",
        scope_id="XHKG:00005",
        source_key="00005",
    )


def test_hkexnews_normalizes_canonical_financial_report():
    record = HKEXNewsPlugin().normalize_and_validate(
        "financial_report",
        _report_raw(),
        _scope(),
    )

    assert record is not None
    assert record.site_id == "hkexnews"
    assert record.country == "HK"
    assert record.dataset == "financial_report"
    assert len(record.source_id) == 64
    assert record.instrument_id == "XHKG:00005"
    assert record.identity_scope_type == "global"
    assert record.event_time.isoformat() == "2026-09-11T08:33:00+00:00"
    assert record.content is None
    assert record.payload == {
        "report_type": "annual",
        "report_type_code": "40100",
        "release_time_raw": "11/09/2026 16:33",
        "hkex_stock_id": 12345,
        "hkex_stock_name": "HSBC Holdings",
        "input_stock_name": "HSBC",
        "pdf_url": record.source_url,
        "source_section": "financial_reports_esg",
        "document_language": None,
        "fiscal_period_end": None,
        "fiscal_year": None,
    }
    assert record.relations[0].instrument_id == "XHKG:00005"


def test_hkexnews_report_identity_ignores_operational_scope():
    plugin = HKEXNewsPlugin()
    first = plugin.normalize("financial_report", _report_raw(), _scope())
    alternate = CrawlScope(
        scope_type="instrument",
        scope_id="XHKG:00005",
        source_key="alternate-search",
    )
    second = plugin.normalize("financial_report", _report_raw(), alternate)

    assert first.source_id == second.source_id
    assert first.record_uid == second.record_uid


def test_hkexnews_same_pdf_across_taxonomy_keeps_logical_identity():
    plugin = HKEXNewsPlugin()
    annual_raw = _report_raw()
    interim_raw = {**annual_raw, "report_type": "interim", "report_type_code": "40200"}

    annual = plugin.normalize("financial_report", annual_raw, _scope())
    interim = plugin.normalize("financial_report", interim_raw, _scope())

    assert annual.source_id == interim.source_id
    assert annual.record_uid == interim.record_uid
    assert annual.version_hash != interim.version_hash


def test_hkexnews_date_only_release_remains_null_and_raw_is_preserved():
    raw = {**_report_raw(), "release_time_raw": "11/09/2026"}

    record = HKEXNewsPlugin().normalize("financial_report", raw, _scope())

    assert record.event_time is None
    assert record.payload["release_time_raw"] == "11/09/2026"
    assert record.payload["fiscal_period_end"] is None
    assert record.payload["fiscal_year"] is None


def test_hkexnews_normalizes_report_instrument_relation():
    record = HKEXNewsPlugin().normalize_and_validate(
        "financial_report_instrument",
        _report_raw(),
        _scope(),
    )

    assert record is not None
    assert record.mutation_policy == "immutable"
    assert record.instrument_id == "XHKG:00005"
    assert record.payload["report_source_id"] in record.source_id


def test_hkexnews_rejects_report_for_different_scope_instrument():
    raw = _report_raw()
    raw["stock_code"] = "00700"

    with pytest.raises(ValueError, match="instrument mismatch"):
        HKEXNewsPlugin().normalize("financial_report", raw, _scope())


def test_hkexnews_accepts_scope_in_explicit_dual_counter_stock_codes():
    raw = _report_raw()
    raw["stock_code"] = "00005 80005"

    record = HKEXNewsPlugin().normalize("financial_report", raw, _scope())

    assert record.instrument_id == "XHKG:00005"


def test_hkexnews_accepts_exact_resolved_dual_counter_alias():
    raw = _report_raw()
    raw["stock_code"] = "00016"
    raw["hkex_resolved_stock_code"] = "80016"
    scope = CrawlScope(
        scope_type="instrument",
        scope_id="XHKG:80016",
        source_key="80016",
    )

    record = HKEXNewsPlugin().normalize("financial_report", raw, scope)

    assert record.instrument_id == "XHKG:80016"


def test_hkexnews_rejects_unproven_dual_counter_alias():
    raw = _report_raw()
    raw["stock_code"] = "00016"
    scope = CrawlScope(
        scope_type="instrument",
        scope_id="XHKG:80016",
        source_key="80016",
    )

    with pytest.raises(ValueError, match="instrument mismatch"):
        HKEXNewsPlugin().normalize("financial_report", raw, scope)


def test_hkexnews_builds_generic_attachment_request():
    plugin = HKEXNewsPlugin()
    report = plugin.normalize("financial_report", _report_raw(), _scope())

    request = plugin.build_attachment_request(report)

    assert request.parent_record_uid == report.record_uid
    assert request.source_url == report.source_url
    assert request.filename == "a.pdf"
    assert request.mime_type == "application/pdf"
    assert request.metadata["report_source_id"] == report.source_id


@pytest.mark.asyncio
async def test_hkexnews_processes_pdf_through_generic_attachment_pipeline():
    calls = []

    class Attachment:
        attachment_id = "sha"
        parent_record_uid = "parent"
        source_url = "https://www1.hkexnews.hk/a.pdf"
        filename = "a.pdf"
        mime_type = "application/pdf"
        sha256 = "sha"
        file_size = 2048
        local_path = Path("warehouse/a.pdf")

    class Upload:
        remote_path = "attachments/a.pdf"
        status = "verified"

    class Pipeline:
        async def process(self, request, **kwargs):
            calls.append((request, kwargs))
            return type("Result", (), {"attachment": Attachment(), "upload_result": Upload()})()

    plugin = HKEXNewsPlugin(attachment_pipeline=Pipeline())
    report = plugin.normalize("financial_report", _report_raw(), _scope())
    raw = await plugin.process_report_attachment(report)
    attachment = plugin.normalize_and_validate("attachment", raw, _scope())

    assert raw["upload_status"] == "verified"
    assert calls[0][1]["require_pdf"] is True
    assert calls[0][1]["minimum_size_bytes"] == 1024
    assert attachment is not None
    assert attachment.payload["parent_record_uid"] == "parent"


class FakeFilingsClient:
    def __init__(self, *, error=None, stock_found=True):
        self.error = error
        self.stock_found = stock_found
        self.resolve_calls = []
        self.search_calls = []

    async def resolve_stock(self, code):
        self.resolve_calls.append(code)
        if not self.stock_found:
            return None
        return HKEXStockInfo(stock_id=12345, code=code, name="HSBC Holdings")

    async def search_reports(self, stock_id, report_types, **kwargs):
        self.search_calls.append((stock_id, report_types, kwargs))
        if self.error:
            raise self.error
        return [
            HKEXReportListItem(
                release_time_raw="11/09/2026 16:33",
                stock_code="00005",
                stock_name="HSBC Holdings",
                title="Annual Report 2025",
                pdf_url="https://www1.hkexnews.hk/docs/a.pdf",
                report_type="annual",
                report_type_code="40100",
            )
        ]


def _crawl_context(**extra):
    return CrawlContext(
        extra={
            "hkex_report_types": ("annual", "interim", "quarterly"),
            "hkex_report_date_from": "20260101",
            "hkex_report_date_to": "20260911",
            **extra,
        }
    )


@pytest.mark.asyncio
async def test_hkexnews_crawl_creates_checkpoint_candidate_only_after_completion():
    client = FakeFilingsClient()
    plugin = HKEXNewsPlugin(client=client)
    checkpoint = CrawlCheckpoint()

    rows = [
        row async for row in plugin.crawl(
            "financial_report", _scope(), checkpoint, _crawl_context()
        )
    ]
    candidate = plugin.checkpoint_after_scope(
        "financial_report", _scope(), checkpoint, _crawl_context()
    )

    assert len(rows) == 1
    assert candidate.state["scope_complete"] is True
    assert candidate.state["reports_found"] == 1
    assert candidate.state["attachment_policy"] == "metadata_only"
    assert len(candidate.state["last_report_source_id"]) == 64


@pytest.mark.asyncio
async def test_hkexnews_failed_search_does_not_advance_checkpoint():
    plugin = HKEXNewsPlugin(client=FakeFilingsClient(error=RuntimeError("boom")))
    checkpoint = CrawlCheckpoint(state={"old": "state"})

    with pytest.raises(RuntimeError, match="boom"):
        _ = [
            row async for row in plugin.crawl(
                "financial_report", _scope(), checkpoint, _crawl_context()
            )
        ]

    candidate = plugin.checkpoint_after_scope(
        "financial_report", _scope(), checkpoint, _crawl_context()
    )
    assert candidate is checkpoint
    assert candidate.state == {"old": "state"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dataset",
    ("financial_report", "financial_report_instrument", "attachment"),
)
async def test_hkexnews_missing_exact_stock_is_a_durable_empty_scope(dataset):
    client = FakeFilingsClient(stock_found=False)
    plugin = HKEXNewsPlugin(client=client)
    checkpoint = CrawlCheckpoint()

    rows = [
        row async for row in plugin.crawl(
            dataset, _scope(), checkpoint, _crawl_context()
        )
    ]
    candidate = plugin.checkpoint_after_scope(
        dataset, _scope(), checkpoint, _crawl_context()
    )

    assert rows == []
    assert client.resolve_calls == ["00005"]
    assert client.search_calls == []
    assert candidate.state["scope_complete"] is True
    assert candidate.state["stop_reason"] == "stock_not_found"
    assert candidate.state["stock_resolution"] == "not_found"
    assert candidate.state["reports_found"] == 0
    assert candidate.state["last_report_source_id"] is None


@pytest.mark.asyncio
async def test_hkexnews_reuses_successful_search_across_logical_datasets():
    client = FakeFilingsClient()
    plugin = HKEXNewsPlugin(client=client)

    metadata = [
        row async for row in plugin.crawl(
            "financial_report", _scope(), CrawlCheckpoint(), _crawl_context()
        )
    ]
    relations = [
        row async for row in plugin.crawl(
            "financial_report_instrument",
            _scope(),
            CrawlCheckpoint(),
            _crawl_context(),
        )
    ]

    assert len(metadata) == 1
    assert len(relations) == 1
    assert client.resolve_calls == ["00005"]
    assert len(client.search_calls) == 1


@pytest.mark.asyncio
async def test_hkexnews_does_not_cache_failed_search():
    client = FakeFilingsClient(error=RuntimeError("boom"))
    plugin = HKEXNewsPlugin(client=client)

    for _ in range(2):
        with pytest.raises(RuntimeError, match="boom"):
            _ = [
                row async for row in plugin.crawl(
                    "financial_report", _scope(), CrawlCheckpoint(), _crawl_context()
                )
            ]

    assert len(client.resolve_calls) == 2
    assert len(client.search_calls) == 2


@pytest.mark.asyncio
async def test_hkexnews_full_resume_skips_matching_complete_scope():
    client = FakeFilingsClient()
    plugin = HKEXNewsPlugin(client=client)
    options = plugin._crawl_options("financial_report", _scope(), _crawl_context())
    checkpoint = CrawlCheckpoint(
        state={
            "scope_complete": True,
            "request_signature": options["request_signature"],
        }
    )

    rows = [
        row async for row in plugin.crawl(
            "financial_report", _scope(), checkpoint, _crawl_context()
        )
    ]

    assert rows == []
    assert client.resolve_calls == []


def test_hkexnews_resume_policy_accepts_only_matching_full_checkpoint():
    plugin = HKEXNewsPlugin(client=FakeFilingsClient())
    scope = _scope()
    context = _crawl_context()
    options = plugin._crawl_options("financial_report", scope, context)

    assert plugin.is_checkpoint_durable_complete(
        "financial_report",
        scope,
        CrawlCheckpoint(state={
            "scope_complete": True,
            "request_signature": options["request_signature"],
        }),
        context,
    )
    assert not plugin.is_checkpoint_durable_complete(
        "financial_report",
        scope,
        CrawlCheckpoint(state={
            "scope_complete": True,
            "request_signature": "different",
        }),
        context,
    )


@pytest.mark.asyncio
async def test_hkexnews_changed_range_does_not_skip_completed_scope():
    client = FakeFilingsClient()
    plugin = HKEXNewsPlugin(client=client)
    checkpoint = CrawlCheckpoint(
        state={"scope_complete": True, "request_signature": "old"}
    )

    rows = [
        row async for row in plugin.crawl(
            "financial_report", _scope(), checkpoint, _crawl_context()
        )
    ]

    assert len(rows) == 1
    assert client.resolve_calls == ["00005"]
