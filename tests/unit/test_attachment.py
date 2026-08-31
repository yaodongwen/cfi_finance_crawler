from datetime import datetime, timezone

import pytest

from crawl_framework.core.adapter import (
    AttachmentRequest,
)
from crawl_framework.storage.attachment import (
    AttachmentContent,
    AttachmentDownloadError,
    LocalAttachmentStore,
    attachment_sha256,
    validate_attachment_content,
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
