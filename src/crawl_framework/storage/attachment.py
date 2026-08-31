from __future__ import annotations

import hashlib
import mimetypes

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from crawl_framework.core.adapter import (
    AttachmentRequest,
)


class AttachmentDownloadError(
    RuntimeError
):
    pass


@dataclass(
    frozen=True,
    slots=True,
)
class AttachmentContent:
    body: bytes

    mime_type: str | None = None

    filename: str | None = None


@dataclass(
    frozen=True,
    slots=True,
)
class AttachmentRecord:
    attachment_id: str

    parent_record_uid: str

    site_id: str

    dataset: str

    source_url: str

    filename: str

    mime_type: str

    sha256: str

    file_size: int

    local_path: Path

    remote_relative_path: str

    fetched_at: datetime


class AttachmentDownloader(
    Protocol
):

    async def download(
        self,
        request: AttachmentRequest,
    ) -> AttachmentContent:
        ...


def attachment_sha256(
    body: bytes,
) -> str:

    return hashlib.sha256(
        body
    ).hexdigest()


def validate_attachment_content(
    content: AttachmentContent,
    *,
    require_pdf: bool = False,
) -> None:

    if not content.body:

        raise AttachmentDownloadError(
            "attachment body is empty"
        )

    mime_type = (
        content.mime_type
        or ""
    ).lower()

    if (
        require_pdf
        and not (
            content.body.startswith(
                b"%PDF"
            )
            or "pdf" in mime_type
        )
    ):

        raise AttachmentDownloadError(
            "attachment is not a PDF"
        )


def attachment_filename(
    request: AttachmentRequest,
    content: AttachmentContent,
    sha: str,
) -> str:

    value = (
        content.filename
        or request.filename
        or f"{sha}.bin"
    )

    return Path(
        value
    ).name


def attachment_mime_type(
    request: AttachmentRequest,
    content: AttachmentContent,
    filename: str,
) -> str:

    return (
        content.mime_type
        or request.mime_type
        or mimetypes.guess_type(
            filename
        )[0]
        or "application/octet-stream"
    )


class LocalAttachmentStore:
    """
    Generic local attachment materializer.
    """

    def __init__(
        self,
        root: str | Path,
    ) -> None:

        self.root = Path(
            root
        )


    def store(
        self,
        request: AttachmentRequest,
        content: AttachmentContent,
        *,
        site_id: str,
        country: str,
        dataset: str,
        event_time: datetime | None = None,
        require_pdf: bool = False,
    ) -> AttachmentRecord:

        validate_attachment_content(
            content,
            require_pdf=require_pdf,
        )

        sha = attachment_sha256(
            content.body
        )

        filename = attachment_filename(
            request,
            content,
            sha,
        )

        mime_type = attachment_mime_type(
            request,
            content,
            filename,
        )

        timestamp = (
            event_time
            or datetime.now(
                timezone.utc
            )
        )

        partition_date = timestamp.date()

        remote_relative_path = (
            "attachments/"
            f"site={site_id}/"
            f"country={country}/"
            f"dataset={dataset}/"
            f"year={partition_date.year:04d}/"
            f"month={partition_date.month:02d}/"
            f"day={partition_date.day:02d}/"
            f"{sha}-{filename}"
        )

        local_path = (
            self.root
            / remote_relative_path
        )

        local_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        local_path.write_bytes(
            content.body
        )

        return AttachmentRecord(
            attachment_id=sha,
            parent_record_uid=request.parent_record_uid,
            site_id=site_id,
            dataset=dataset,
            source_url=request.source_url,
            filename=filename,
            mime_type=mime_type,
            sha256=sha,
            file_size=len(
                content.body
            ),
            local_path=local_path,
            remote_relative_path=remote_relative_path,
            fetched_at=datetime.now(
                timezone.utc
            ),
        )
