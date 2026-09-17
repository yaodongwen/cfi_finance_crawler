from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import AsyncIterator
from pathlib import PurePosixPath
from zoneinfo import ZoneInfo

from crawl_framework.core.adapter import AttachmentRequest
from crawl_framework.core.models import CanonicalRecord, RecordRelation
from crawl_framework.core.plugin import (
    CrawlCheckpoint,
    CrawlContext,
    CrawlScope,
    SitePlugin,
)
from crawl_framework.sites.hkexnews.filings import (
    build_hkex_report_source_id,
    canonical_hkex_instrument_id,
    canonicalize_hkex_pdf_url,
    normalize_hkex_stock_code,
    parse_hkex_stock_codes,
    parse_hkex_release_time,
)


HKEXNEWS_SITE_ID = "hkexnews"
HKEXNEWS_COUNTRY = "HK"
HKEXNEWS_TIMEZONE = "Asia/Hong_Kong"
HKEXNEWS_DATASETS = (
    "financial_report",
    "financial_report_instrument",
    "attachment",
)


class HKEXNewsPlugin(SitePlugin):
    """Disabled-by-default HKEXnews plugin skeleton for Phase L2."""

    requires_http = True

    def __init__(self, *, client=None, attachment_pipeline=None) -> None:
        self.client = client
        self.attachment_pipeline = attachment_pipeline
        self._scope_states: dict[tuple[str, str], dict] = {}
        self._report_search_cache: dict[tuple, tuple[object | None, tuple]] = {}

    @property
    def site_id(self) -> str:
        return HKEXNEWS_SITE_ID

    @property
    def country(self) -> str:
        return HKEXNEWS_COUNTRY

    @property
    def timezone(self) -> str:
        return HKEXNEWS_TIMEZONE

    def datasets(self) -> tuple[str, ...]:
        return HKEXNEWS_DATASETS

    async def discover(
        self,
        dataset: str,
        ctx: CrawlContext,
    ) -> AsyncIterator[CrawlScope]:
        self._validate_dataset(dataset)
        instruments = tuple((ctx.extra or {}).get("instrument_codes") or ())
        if not instruments:
            raise ValueError("HKEXnews discovery requires instrument_codes")
        seen: set[str] = set()
        for raw_value in instruments:
            value = str(raw_value).strip()
            if value.startswith("XHKG:"):
                value = value.split(":", 1)[1]
            code = normalize_hkex_stock_code(value)
            instrument_id = canonical_hkex_instrument_id(code)
            if instrument_id in seen:
                continue
            seen.add(instrument_id)
            yield CrawlScope(
                scope_type="instrument",
                scope_id=instrument_id,
                source_key=code,
            )

    async def crawl(
        self,
        dataset: str,
        scope: CrawlScope,
        checkpoint: CrawlCheckpoint,
        ctx: CrawlContext,
    ) -> AsyncIterator[dict]:
        self._validate_dataset(dataset)
        scope_key = (dataset, scope.source_key)
        self._scope_states.pop(scope_key, None)
        options = self._crawl_options(dataset, scope, ctx)
        prior_state = checkpoint.state if checkpoint is not None else {}
        if (
            options["mode"] == "full"
            and prior_state.get("scope_complete") is True
            and prior_state.get("request_signature") == options["request_signature"]
        ):
            return

        client = self._resolve_client(ctx)
        search_cache_key = (
            options["stock_code"],
            tuple(options["report_types"]),
            options["date_from"],
            options["date_to"],
        )
        cached = self._report_search_cache.get(search_cache_key)
        if cached is None:
            stock = await client.resolve_stock(options["stock_code"])
            reports = ()
            if stock is not None:
                reports = tuple(
                    await client.search_reports(
                        stock.stock_id,
                        options["report_types"],
                        date_from=options["date_from"],
                        date_to=options["date_to"],
                    )
                )
            self._report_search_cache[search_cache_key] = (stock, reports)
        else:
            stock, reports = cached

        if stock is None:
            self._scope_states[scope_key] = {
                "instrument_id": options["instrument_id"],
                "stock_code": options["stock_code"],
                "report_types": list(options["report_types"]),
                "date_from": options["date_from"],
                "date_to": options["date_to"],
                "request_signature": options["request_signature"],
                "last_report_source_id": None,
                "reports_found": 0,
                "scope_complete": True,
                "stop_reason": "stock_not_found",
                "stock_resolution": "not_found",
                "attachment_policy": (
                    "downloaded" if dataset == "attachment" else "metadata_only"
                ),
            }
            return

        last_source_id = None
        for item in reports:
            raw = {
                **item.to_raw(),
                "hkex_stock_id": stock.stock_id,
                "hkex_resolved_stock_code": stock.code,
                "hkex_stock_name": stock.name,
                "input_stock_name": scope.metadata.get("input_stock_name"),
            }
            report = self.normalize("financial_report", raw, scope)
            last_source_id = report.source_id
            if dataset == "attachment":
                yield await self.process_report_attachment(report)
            else:
                yield raw

        self._scope_states[scope_key] = {
            "instrument_id": options["instrument_id"],
            "stock_code": options["stock_code"],
            "report_types": list(options["report_types"]),
            "date_from": options["date_from"],
            "date_to": options["date_to"],
            "request_signature": options["request_signature"],
            "last_report_source_id": last_source_id,
            "reports_found": len(reports),
            "scope_complete": True,
            "stop_reason": "search_complete",
            "attachment_policy": (
                "downloaded" if dataset == "attachment" else "metadata_only"
            ),
        }

    def is_checkpoint_durable_complete(
        self,
        dataset: str,
        scope: CrawlScope,
        checkpoint: CrawlCheckpoint,
        ctx: CrawlContext,
    ) -> bool:
        self._validate_dataset(dataset)
        options = self._crawl_options(dataset, scope, ctx)
        return bool(
            options["mode"] == "full"
            and checkpoint.state.get("scope_complete") is True
            and checkpoint.state.get("request_signature")
            == options["request_signature"]
        )

    def checkpoint_after_scope(
        self,
        dataset: str,
        scope: CrawlScope,
        checkpoint: CrawlCheckpoint,
        ctx: CrawlContext,
    ) -> CrawlCheckpoint:
        self._validate_dataset(dataset)
        state = self._scope_states.get((dataset, scope.source_key))
        if state is None:
            return checkpoint
        return CrawlCheckpoint(state={**checkpoint.state, **state})

    def normalize(
        self,
        dataset: str,
        raw: dict,
        scope: CrawlScope,
    ) -> CanonicalRecord:
        self._validate_dataset(dataset)
        if dataset == "attachment":
            return self._normalize_attachment(raw, scope)
        instrument_id = self._scope_instrument_id(scope)
        pdf_url = canonicalize_hkex_pdf_url(str(raw.get("pdf_url", "")))
        report_source_id = build_hkex_report_source_id(pdf_url)
        raw_code = raw.get("stock_code")
        observed_codes: tuple[str, ...] = ()
        if raw_code not in {None, ""}:
            observed_codes = parse_hkex_stock_codes(raw_code)
            observed = {
                canonical_hkex_instrument_id(code)
                for code in observed_codes
            }
            resolved_code = raw.get("hkex_resolved_stock_code")
            resolved_instrument = (
                canonical_hkex_instrument_id(resolved_code)
                if resolved_code not in {None, ""}
                else None
            )
            if instrument_id not in observed and resolved_instrument != instrument_id:
                raise ValueError(
                    "HKEXnews report instrument mismatch: "
                    f"{sorted(observed)} does not contain {instrument_id}"
                )

        if dataset == "financial_report_instrument":
            return CanonicalRecord(
                site_id=self.site_id,
                country=self.country,
                dataset=dataset,
                source_id=f"{report_source_id}:{instrument_id}",
                scope_type="global",
                scope_id=None,
                instrument_id=instrument_id,
                event_time=parse_hkex_release_time(raw.get("release_time_raw")),
                title=raw.get("title"),
                source_url=pdf_url,
                payload={
                    "report_source_id": report_source_id,
                    "relation_type": "primary",
                    "relation_evidence": "hkexnews_title_search_result",
                },
                relations=[
                    RecordRelation(
                        instrument_id=instrument_id,
                        relation_type="primary",
                        confidence=1.0,
                    )
                ],
                mutation_policy="immutable",
            )

        return CanonicalRecord(
            site_id=self.site_id,
            country=self.country,
            dataset=dataset,
            source_id=report_source_id,
            scope_type="instrument",
            scope_id=instrument_id,
            identity_scope_type="global",
            identity_scope_id=None,
            instrument_id=instrument_id,
            event_time=parse_hkex_release_time(raw.get("release_time_raw")),
            title=raw.get("title"),
            content=None,
            source_url=pdf_url,
            payload={
                "report_type": raw.get("report_type"),
                "report_type_code": raw.get("report_type_code"),
                "release_time_raw": raw.get("release_time_raw"),
                "hkex_stock_id": raw.get("hkex_stock_id"),
                "hkex_stock_name": raw.get("hkex_stock_name") or raw.get("stock_name"),
                "input_stock_name": raw.get("input_stock_name"),
                "pdf_url": pdf_url,
                "source_section": "financial_reports_esg",
                "document_language": None,
                "fiscal_period_end": None,
                "fiscal_year": None,
            },
            relations=[
                RecordRelation(
                    instrument_id=instrument_id,
                    relation_type="primary",
                    confidence=1.0,
                )
            ],
            mutation_policy="versioned",
        )

    def build_attachment_request(
        self,
        report: CanonicalRecord,
    ) -> AttachmentRequest:
        if report.dataset != "financial_report" or report.site_id != self.site_id:
            raise ValueError("attachment parent must be an HKEXnews financial_report")
        pdf_url = canonicalize_hkex_pdf_url(
            str(report.payload.get("pdf_url") or report.source_url or "")
        )
        filename = PurePosixPath(pdf_url.split("?", 1)[0]).name
        return AttachmentRequest(
            parent_record_uid=report.record_uid,
            source_url=pdf_url,
            filename=filename,
            mime_type="application/pdf",
            metadata={
                "site_id": self.site_id,
                "country": self.country,
                "dataset": "financial_report",
                "report_source_id": report.source_id,
                "instrument_id": report.instrument_id,
            },
        )

    async def process_report_attachment(
        self,
        report: CanonicalRecord,
    ) -> dict:
        if self.attachment_pipeline is None:
            raise RuntimeError("attachment pipeline is not configured")
        request = self.build_attachment_request(report)
        result = await self.attachment_pipeline.process(
            request,
            site_id=self.site_id,
            country=self.country,
            dataset="financial_report",
            event_time=report.event_time,
            require_pdf=True,
            minimum_size_bytes=1024,
        )
        attachment = result.attachment
        return {
            "attachment_id": attachment.attachment_id,
            "parent_record_uid": attachment.parent_record_uid,
            "report_source_id": report.source_id,
            "instrument_id": report.instrument_id,
            "source_url": attachment.source_url,
            "filename": attachment.filename,
            "mime_type": attachment.mime_type,
            "sha256": attachment.sha256,
            "file_size": attachment.file_size,
            "local_path": str(attachment.local_path),
            "remote_path": result.upload_result.remote_path,
            "upload_status": result.upload_result.status,
        }

    def _normalize_attachment(
        self,
        raw: dict,
        scope: CrawlScope,
    ) -> CanonicalRecord:
        attachment_id = str(raw.get("attachment_id", "")).strip()
        parent_record_uid = str(raw.get("parent_record_uid", "")).strip()
        sha256 = str(raw.get("sha256", "")).strip()
        if not attachment_id or not parent_record_uid or not sha256:
            raise ValueError("HKEXnews attachment requires id, parent UID, and SHA256")
        instrument_id = self._scope_instrument_id(scope)
        return CanonicalRecord(
            site_id=self.site_id,
            country=self.country,
            dataset="attachment",
            source_id=attachment_id,
            scope_type="instrument",
            scope_id=instrument_id,
            instrument_id=instrument_id,
            title=raw.get("filename"),
            source_url=raw.get("source_url"),
            payload={
                "attachment_id": attachment_id,
                "parent_record_uid": parent_record_uid,
                "report_source_id": raw.get("report_source_id"),
                "filename": raw.get("filename"),
                "mime_type": raw.get("mime_type"),
                "sha256": sha256,
                "file_size": raw.get("file_size"),
                "local_path": raw.get("local_path"),
                "remote_path": raw.get("remote_path"),
                "upload_status": raw.get("upload_status"),
            },
            relations=[
                RecordRelation(
                    instrument_id=instrument_id,
                    relation_type="primary",
                    confidence=1.0,
                )
            ],
            mutation_policy="immutable",
        )

    @staticmethod
    def _scope_instrument_id(scope: CrawlScope) -> str:
        value = str(scope.scope_id or "").strip()
        if value.startswith("XHKG:"):
            return canonical_hkex_instrument_id(value.split(":", 1)[1])
        return canonical_hkex_instrument_id(scope.source_key)

    def _validate_dataset(self, dataset: str) -> None:
        if dataset not in self.validate_datasets():
            raise ValueError(f"unsupported HKEXnews dataset: {dataset!r}")

    def _resolve_client(self, ctx: CrawlContext):
        client = self.client
        if client is None and ctx.extra:
            client = ctx.extra.get("hkexnews_client")
        if client is None and ctx.http is not None:
            from crawl_framework.sites.hkexnews.filings import HKEXFilingsClient

            client = HKEXFilingsClient.from_transport(ctx.http)
        if client is None:
            raise RuntimeError("HKEXnews crawl requires an HKEXFilingsClient")
        self.client = client
        return client

    def _crawl_options(
        self,
        dataset: str,
        scope: CrawlScope,
        ctx: CrawlContext,
    ) -> dict:
        extra = ctx.extra or {}
        instrument_id = self._scope_instrument_id(scope)
        stock_code = normalize_hkex_stock_code(instrument_id.split(":", 1)[1])
        raw_types = extra.get(
            "hkex_report_types",
            ("annual", "interim", "quarterly"),
        )
        if isinstance(raw_types, str):
            raw_types = tuple(value.strip() for value in raw_types.split(","))
        from crawl_framework.sites.hkexnews.filings import (
            normalize_hkex_report_type,
            normalize_hkex_search_date,
        )

        report_types = tuple(
            dict.fromkeys(normalize_hkex_report_type(value)[0] for value in raw_types)
        )
        if not report_types:
            raise ValueError("HKEXnews report types cannot be empty")
        today = datetime.now(ZoneInfo(self.timezone)).date()
        date_from = normalize_hkex_search_date(
            extra.get("hkex_report_date_from", "19990401")
        )
        date_to = normalize_hkex_search_date(
            extra.get("hkex_report_date_to", today)
        )
        if date_from > date_to:
            raise ValueError("HKEX report date_from must not be after date_to")
        mode = str(extra.get("hkex_report_mode", "full")).strip().lower()
        if mode not in {"full", "incremental"}:
            raise ValueError(f"unsupported HKEXnews report mode: {mode!r}")
        signature_payload = {
            "dataset": dataset,
            "instrument_id": instrument_id,
            "report_types": report_types,
            "date_from": date_from,
            "date_to": date_to,
        }
        request_signature = hashlib.sha256(
            json.dumps(signature_payload, sort_keys=True).encode("utf-8")
        ).hexdigest()
        return {
            **signature_payload,
            "stock_code": stock_code,
            "mode": mode,
            "request_signature": request_signature,
        }
