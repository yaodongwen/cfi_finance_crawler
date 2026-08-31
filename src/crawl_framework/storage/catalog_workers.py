from __future__ import annotations

import asyncio

from dataclasses import dataclass
from typing import Callable, Iterable

from crawl_framework.storage.parquet_writer import (
    ParquetFileInfo,
)
from crawl_framework.storage.uploader import (
    UploadResult,
)


@dataclass(
    frozen=True,
    slots=True,
)
class CatalogIndexJob:
    parquet_info: ParquetFileInfo

    upload_result: UploadResult


CatalogRegister = Callable[
    [
        CatalogIndexJob,
    ],
    None,
]

RecordIndexRegister = Callable[
    [
        CatalogIndexJob,
    ],
    None,
]


@dataclass(
    frozen=True,
    slots=True,
)
class ParallelCatalogConfig:
    worker_count: int = 1

    queue_size: int = 128

    def __post_init__(
        self,
    ) -> None:

        if self.worker_count < 1:

            raise ValueError(
                "worker_count must be >= 1"
            )

        if self.queue_size < 1:

            raise ValueError(
                "queue_size must be >= 1"
            )


@dataclass(
    frozen=True,
    slots=True,
)
class ParallelCatalogResult:
    registered: int

    max_queue_size: int


class ParallelCatalogIndexWorkers:
    """
    Generic bounded workers for catalog/index registration.
    """

    def __init__(
        self,
        *,
        register_catalog: CatalogRegister,
        register_record_index: RecordIndexRegister | None = None,
        config: ParallelCatalogConfig | None = None,
    ) -> None:

        self.register_catalog = register_catalog
        self.register_record_index = register_record_index
        self.config = (
            config
            or ParallelCatalogConfig()
        )


    async def run(
        self,
        jobs: Iterable[
            CatalogIndexJob
        ],
    ) -> ParallelCatalogResult:

        queue: asyncio.Queue = asyncio.Queue(
            maxsize=self.config.queue_size
        )

        sentinel = object()

        registered = 0
        max_queue_size = 0

        lock = asyncio.Lock()

        async def producer() -> None:

            nonlocal max_queue_size

            for job in jobs:

                await queue.put(
                    job
                )

                max_queue_size = max(
                    max_queue_size,
                    queue.qsize(),
                )

            for _ in range(
                self.config.worker_count
            ):

                await queue.put(
                    sentinel
                )

        async def worker() -> None:

            nonlocal registered

            while True:

                item = await queue.get()

                try:

                    if item is sentinel:

                        return

                    if self.register_record_index is not None:

                        await asyncio.to_thread(
                            self.register_record_index,
                            item,
                        )

                    await asyncio.to_thread(
                        self.register_catalog,
                        item,
                    )

                    async with lock:

                        registered += 1

                finally:

                    queue.task_done()

        tasks = [
            asyncio.create_task(
                producer()
            )
        ]

        tasks.extend(
            asyncio.create_task(
                worker()
            )
            for _ in range(
                self.config.worker_count
            )
        )

        await asyncio.gather(
            *tasks
        )

        return ParallelCatalogResult(
            registered=registered,
            max_queue_size=max_queue_size,
        )
