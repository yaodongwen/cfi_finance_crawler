from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Iterable

from crawl_framework.core.models import (
    CanonicalRecord,
    InstrumentRef,
)
from crawl_framework.core.plugin import (
    CrawlCheckpoint,
    CrawlContext,
    CrawlScope,
)


@dataclass(
    frozen=True,
    slots=True,
)
class AdapterCapability:
    """
    Generic capability declaration for site adapters.
    """

    dataset: str

    discovers_instruments: bool = False

    discovers_tasks: bool = True

    fetches_attachments: bool = False


@dataclass(
    frozen=True,
    slots=True,
)
class CrawlTask:
    """
    Site-owned fetch task.

    Core treats task_id/source_key/cursor as opaque values.
    """

    task_id: str

    source_key: str

    scope: CrawlScope

    cursor: str | None = None

    metadata: dict[
        str,
        Any,
    ] = field(
        default_factory=dict
    )

    def __post_init__(
        self,
    ) -> None:

        task_id = str(
            self.task_id
        ).strip()

        source_key = str(
            self.source_key
        ).strip()

        if not task_id:

            raise ValueError(
                "task_id cannot be empty"
            )

        if not source_key:

            raise ValueError(
                "source_key cannot be empty"
            )

        object.__setattr__(
            self,
            "task_id",
            task_id,
        )

        object.__setattr__(
            self,
            "source_key",
            source_key,
        )


@dataclass(
    frozen=True,
    slots=True,
)
class RawFetchResult:
    """
    Opaque site raw payload plus generic provenance.
    """

    task: CrawlTask

    payload: Any

    source_url: str | None = None

    metadata: dict[
        str,
        Any,
    ] = field(
        default_factory=dict
    )


@dataclass(
    frozen=True,
    slots=True,
)
class AttachmentRequest:
    """
    Generic attachment download request.
    """

    parent_record_uid: str

    source_url: str

    filename: str | None = None

    mime_type: str | None = None

    metadata: dict[
        str,
        Any,
    ] = field(
        default_factory=dict
    )

    def __post_init__(
        self,
    ) -> None:

        parent_record_uid = str(
            self.parent_record_uid
        ).strip()

        source_url = str(
            self.source_url
        ).strip()

        if not parent_record_uid:

            raise ValueError(
                "parent_record_uid cannot be empty"
            )

        if not source_url:

            raise ValueError(
                "source_url cannot be empty"
            )

        object.__setattr__(
            self,
            "parent_record_uid",
            parent_record_uid,
        )

        object.__setattr__(
            self,
            "source_url",
            source_url,
        )


class SiteAdapter(
    ABC
):
    """
    Generic site adapter boundary.

    Site-specific code owns discovery, fetch, normalization,
    and attachment discovery. Core owns concurrency, buffering,
    storage, upload, catalog, index, SeenStore, checkpoint,
    recovery, and query.
    """

    @property
    @abstractmethod
    def site_id(
        self,
    ) -> str:
        raise NotImplementedError


    @property
    @abstractmethod
    def country(
        self,
    ) -> str:
        raise NotImplementedError


    @property
    @abstractmethod
    def timezone(
        self,
    ) -> str:
        raise NotImplementedError


    @abstractmethod
    def supported_datasets(
        self,
    ) -> Iterable[
        str
    ]:
        raise NotImplementedError


    def capabilities(
        self,
    ) -> tuple[
        AdapterCapability,
        ...
    ]:

        return tuple(
            AdapterCapability(
                dataset=dataset,
            )
            for dataset in self.supported_datasets()
        )


    async def discover_instruments(
        self,
        context: CrawlContext,
    ) -> AsyncIterator[
        InstrumentRef
    ]:

        if False:

            yield


    @abstractmethod
    async def discover_tasks(
        self,
        dataset: str,
        scope: CrawlScope,
        checkpoint: CrawlCheckpoint | None,
        context: CrawlContext,
    ) -> AsyncIterator[
        CrawlTask
    ]:
        raise NotImplementedError


    @abstractmethod
    async def fetch(
        self,
        dataset: str,
        task: CrawlTask,
        context: CrawlContext,
    ) -> RawFetchResult:
        raise NotImplementedError


    @abstractmethod
    def normalize(
        self,
        dataset: str,
        raw: RawFetchResult,
        scope: CrawlScope,
    ) -> CanonicalRecord | None:
        raise NotImplementedError


    async def discover_attachments(
        self,
        dataset: str,
        raw: RawFetchResult,
        context: CrawlContext,
    ) -> AsyncIterator[
        AttachmentRequest
    ]:

        if False:

            yield
