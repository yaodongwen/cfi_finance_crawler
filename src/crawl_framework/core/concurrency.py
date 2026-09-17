from __future__ import annotations

import asyncio

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator


@dataclass(
    frozen=True,
    slots=True,
)
class StageConcurrencyConfig:
    crawl_workers: int = 1

    http_concurrency: int = 1

    attachment_workers: int = 1

    writer_workers: int = 1

    upload_workers: int = 1

    catalog_workers: int = 1

    def __post_init__(
        self,
    ) -> None:

        for name in (
            "crawl_workers",
            "http_concurrency",
            "attachment_workers",
            "writer_workers",
            "upload_workers",
            "catalog_workers",
        ):

            if getattr(
                self,
                name,
            ) < 1:

                raise ValueError(
                    f"{name} must be >= 1"
                )


@dataclass(
    frozen=True,
    slots=True,
)
class DatasetResourceBudget:
    crawl_workers: int | None = None

    http_concurrency: int | None = None

    detail_workers: int | None = None

    attachment_workers: int | None = None

    def __post_init__(
        self,
    ) -> None:

        for name in (
            "crawl_workers",
            "http_concurrency",
            "detail_workers",
            "attachment_workers",
        ):

            value = getattr(
                self,
                name,
            )

            if (
                value is not None
                and value < 1
            ):

                raise ValueError(
                    f"{name} must be >= 1"
                )


@dataclass(
    frozen=True,
    slots=True,
)
class QueueSizeConfig:
    records: int = 1000

    durable_files: int = 128

    uploads: int = 128

    catalog: int = 128

    attachments: int = 128

    def __post_init__(
        self,
    ) -> None:

        for name in (
            "records",
            "durable_files",
            "uploads",
            "catalog",
            "attachments",
        ):

            if getattr(
                self,
                name,
            ) < 1:

                raise ValueError(
                    f"{name} must be >= 1"
                )


@dataclass(frozen=True, slots=True)
class StageBudgetSnapshot:
    limit: int
    active: int
    max_active: int
    waits: int


@dataclass(frozen=True, slots=True)
class GlobalStageBudgetSnapshot:
    writer: StageBudgetSnapshot
    upload: StageBudgetSnapshot
    catalog: StageBudgetSnapshot

    def to_dict(self) -> dict[str, dict[str, int]]:
        return {
            name: {
                "limit": item.limit,
                "active": item.active,
                "max_active": item.max_active,
                "waits": item.waits,
            }
            for name, item in (
                ("writer", self.writer),
                ("upload", self.upload),
                ("catalog", self.catalog),
            )
        }


class _StageLimiter:
    def __init__(self, limit: int) -> None:
        if int(limit) < 1:
            raise ValueError("global stage limit must be >= 1")
        self.limit = int(limit)
        self._semaphore = asyncio.Semaphore(self.limit)
        self.active = 0
        self.max_active = 0
        self.waits = 0

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        if self._semaphore.locked():
            self.waits += 1
        await self._semaphore.acquire()
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            yield
        finally:
            self.active -= 1
            self._semaphore.release()

    def snapshot(self) -> StageBudgetSnapshot:
        return StageBudgetSnapshot(
            limit=self.limit,
            active=self.active,
            max_active=self.max_active,
            waits=self.waits,
        )


class GlobalStageBudget:
    """Shared writer/upload/Catalog permits for all platform runtimes."""

    def __init__(
        self,
        *,
        writer_workers: int = 2,
        upload_workers: int = 2,
        catalog_workers: int = 2,
    ) -> None:
        self._writer = _StageLimiter(writer_workers)
        self._upload = _StageLimiter(upload_workers)
        self._catalog = _StageLimiter(catalog_workers)

    def writer_slot(self):
        return self._writer.slot()

    def upload_slot(self):
        return self._upload.slot()

    def catalog_slot(self):
        return self._catalog.slot()

    @asynccontextmanager
    async def recovery_slot(self) -> AsyncIterator[None]:
        async with self.upload_slot():
            async with self.catalog_slot():
                yield

    @asynccontextmanager
    async def maintenance_slot(self) -> AsyncIterator[None]:
        async with self.writer_slot():
            async with self.upload_slot():
                async with self.catalog_slot():
                    yield

    def snapshot(self) -> GlobalStageBudgetSnapshot:
        return GlobalStageBudgetSnapshot(
            writer=self._writer.snapshot(),
            upload=self._upload.snapshot(),
            catalog=self._catalog.snapshot(),
        )
