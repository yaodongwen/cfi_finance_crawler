from __future__ import annotations

import hashlib
import mimetypes
import time

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

import requests

from crawl_framework.core.adapter import (
    AttachmentRequest,
)
from crawl_framework.storage.uploader import (
    BaseUploader,
    UploadResult,
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


    @property
    def relative_path(
        self,
    ) -> Path:

        return Path(
            self.remote_relative_path
        )


    @property
    def file_path(
        self,
    ) -> Path:

        return self.local_path


class AttachmentDownloader(
    Protocol
):

    async def download(
        self,
        request: AttachmentRequest,
    ) -> AttachmentContent:
        ...


class HTTPAttachmentDownloader:
    """
    Generic HTTP attachment downloader.
    """

    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        timeout_seconds: float = 120,
        retries: int = 3,
        retry_sleep_seconds: float = 0.0,
    ) -> None:

        if timeout_seconds <= 0:

            raise ValueError(
                "timeout_seconds must be positive"
            )

        if retries <= 0:

            raise ValueError(
                "retries must be positive"
            )

        self.session = (
            session
            or requests.Session()
        )

        self.timeout_seconds = timeout_seconds

        self.retries = retries

        self.retry_sleep_seconds = retry_sleep_seconds


    async def download(
        self,
        request: AttachmentRequest,
    ) -> AttachmentContent:

        last_error: Exception | None = None

        for attempt in range(
            self.retries
        ):

            try:

                response = self.session.get(
                    request.source_url,
                    timeout=self.timeout_seconds,
                )

                status_code = int(
                    getattr(
                        response,
                        "status_code",
                        0,
                    )
                )

                if status_code >= 400:

                    raise AttachmentDownloadError(
                        "attachment HTTP "
                        f"{status_code}: "
                        f"{request.source_url}"
                    )

                if hasattr(
                    response,
                    "raise_for_status",
                ):

                    response.raise_for_status()

                headers = getattr(
                    response,
                    "headers",
                    {},
                )

                return AttachmentContent(
                    body=response.content,
                    mime_type=(
                        headers.get(
                            "Content-Type"
                        )
                        if hasattr(
                            headers,
                            "get",
                        )
                        else None
                    ),
                    filename=request.filename,
                )

            except Exception as exc:

                last_error = exc

                if (
                    attempt
                    + 1
                    >= self.retries
                ):

                    break

                if self.retry_sleep_seconds > 0:

                    time.sleep(
                        self.retry_sleep_seconds
                        * (attempt + 1)
                    )

        raise AttachmentDownloadError(
            "attachment download failed: "
            f"{last_error}"
        )


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


@dataclass(
    frozen=True,
    slots=True,
)
class AttachmentProcessResult:
    attachment: AttachmentRecord

    upload_result: UploadResult


class AttachmentPipeline:
    """
    Generic attachment durable pipeline.
    """

    def __init__(
        self,
        *,
        downloader: AttachmentDownloader,
        store: LocalAttachmentStore,
        uploader: BaseUploader,
    ) -> None:

        self.downloader = downloader

        self.store = store

        self.uploader = uploader


    async def process(
        self,
        request: AttachmentRequest,
        *,
        site_id: str,
        country: str,
        dataset: str,
        event_time: datetime | None = None,
        require_pdf: bool = False,
    ) -> AttachmentProcessResult:

        content = await self.downloader.download(
            request
        )

        attachment = self.store.store(
            request,
            content,
            site_id=site_id,
            country=country,
            dataset=dataset,
            event_time=event_time,
            require_pdf=require_pdf,
        )

        upload_result = self.uploader.upload(
            attachment
        )

        return AttachmentProcessResult(
            attachment=attachment,
            upload_result=upload_result,
        )
