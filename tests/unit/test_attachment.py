import asyncio
import time

from datetime import datetime, timezone

import pytest

from crawl_framework.core.adapter import (
    AttachmentRequest,
)
from crawl_framework.core.concurrency import GlobalStageBudget
from crawl_framework.storage.attachment import (
    AttachmentPipeline,
    AttachmentContent,
    AttachmentDownloadError,
    HTTPAttachmentDownloader,
    LocalAttachmentStore,
    attachment_sha256,
    validate_attachment_content,
)
from crawl_framework.storage.uploader import (
    LocalUploader,
)


def test_validate_attachment_content_rejects_empty_body():

    with pytest.raises(
        AttachmentDownloadError,
        match="empty",
    ):

        validate_attachment_content(
            AttachmentContent(
                body=b"",
            )
        )


def test_validate_attachment_content_checks_pdf_when_required():

    validate_attachment_content(
        AttachmentContent(
            body=b"%PDF-1.7\nbody",
            mime_type="application/pdf",
        ),
        require_pdf=True,
    )

    with pytest.raises(
        AttachmentDownloadError,
        match="PDF",
    ):

        validate_attachment_content(
            AttachmentContent(
                body=b"not a pdf",
                mime_type="text/plain",
            ),
            require_pdf=True,
        )

    with pytest.raises(AttachmentDownloadError, match="PDF"):
        validate_attachment_content(
            AttachmentContent(
                body=b"<html>blocked</html>",
                mime_type="application/pdf",
            ),
            require_pdf=True,
        )


def test_validate_attachment_content_enforces_configurable_minimum_size():
    with pytest.raises(AttachmentDownloadError, match="minimum"):
        validate_attachment_content(
            AttachmentContent(body=b"%PDF-short"),
            require_pdf=True,
            minimum_size_bytes=1024,
        )


def test_local_attachment_store_writes_stable_metadata(
    tmp_path,
):

    store = LocalAttachmentStore(
        tmp_path
    )

    body = b"%PDF-1.7\nbody"

    record = store.store(
        AttachmentRequest(
            parent_record_uid="parent",
            source_url="https://example.com/report.pdf",
            filename="report.pdf",
            mime_type="application/pdf",
        ),
        AttachmentContent(
            body=body,
        ),
        site_id="naver_finance",
        country="KR",
        dataset="research_report",
        event_time=datetime(
            2026,
            8,
            27,
            tzinfo=timezone.utc,
        ),
        require_pdf=True,
    )

    assert (
        record.sha256
        == attachment_sha256(
            body
        )
    )

    assert (
        record.file_size
        == len(
            body
        )
    )

    assert (
        record.remote_relative_path.startswith(
            "attachments/site=naver_finance/"
            "country=KR/dataset=research_report/"
            "year=2026/month=08/day=27/"
        )
    )

    assert (
        record.local_path.read_bytes()
        == body
    )
    assert list(record.local_path.parent.glob("*.part")) == []


class FakeDownloader:

    async def download(
        self,
        request,
    ):

        return AttachmentContent(
            body=b"%PDF-1.7\nbody",
            mime_type="application/pdf",
            filename=request.filename,
        )


def test_attachment_pipeline_downloads_stores_and_uploads(
    tmp_path,
):

    pipeline = AttachmentPipeline(
        downloader=FakeDownloader(),
        store=LocalAttachmentStore(
            tmp_path
            / "warehouse"
        ),
        uploader=LocalUploader(
            tmp_path
            / "remote",
            verify_size=True,
            verify_sha256=True,
        ),
    )

    async def run():

        return await pipeline.process(
            AttachmentRequest(
                parent_record_uid="parent",
                source_url="https://example.com/report.pdf",
                filename="report.pdf",
                mime_type="application/pdf",
            ),
            site_id="naver_finance",
            country="KR",
            dataset="research_report",
            event_time=datetime(
                2026,
                8,
                27,
                tzinfo=timezone.utc,
            ),
            require_pdf=True,
        )

    import asyncio

    result = asyncio.run(
        run()
    )

    assert result.attachment.local_path.exists()

    assert result.upload_result.status == "verified"

    assert (
        tmp_path
        / "remote"
        / result.attachment.relative_path
    ).exists()


@pytest.mark.asyncio
async def test_attachment_pipelines_share_global_upload_budget(tmp_path):
    class SlowUploader(LocalUploader):
        def upload(self, item):
            time.sleep(0.02)
            return super().upload(item)

    budget = GlobalStageBudget(
        writer_workers=1,
        upload_workers=1,
        catalog_workers=1,
    )

    def pipeline(name):
        return AttachmentPipeline(
            downloader=FakeDownloader(),
            store=LocalAttachmentStore(tmp_path / name / "warehouse"),
            uploader=SlowUploader(
                tmp_path / name / "remote",
                verify_size=True,
                verify_sha256=True,
            ),
            stage_budget=budget,
        )

    async def process(item, name):
        return await item.process(
            AttachmentRequest(
                parent_record_uid=name,
                source_url=f"https://example.com/{name}.pdf",
                filename=f"{name}.pdf",
                mime_type="application/pdf",
            ),
            site_id=name,
            country="KR",
            dataset="attachment",
            require_pdf=True,
        )

    await asyncio.gather(
        process(pipeline("left"), "left"),
        process(pipeline("right"), "right"),
    )

    snapshot = budget.snapshot()
    assert snapshot.writer.max_active == 1
    assert snapshot.upload.max_active == 1
    assert snapshot.upload.waits > 0


def test_http_attachment_downloader_uses_response_headers():

    class Response:
        status_code = 200
        content = b"%PDF-1.7\nbody"
        headers = {
            "Content-Type": "application/pdf"
        }

        def raise_for_status(
            self,
        ):

            pass

    class Session:
        def __init__(
            self,
        ):

            self.calls = []

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

            return Response()

    session = Session()

    downloader = HTTPAttachmentDownloader(
        session=session,
        timeout_seconds=3,
    )

    async def run():

        return await downloader.download(
            AttachmentRequest(
                parent_record_uid="parent",
                source_url="https://example.com/report.pdf",
            )
        )

    import asyncio

    content = asyncio.run(
        run()
    )

    assert content.mime_type == "application/pdf"

    assert session.calls[0]["timeout"] == 3
