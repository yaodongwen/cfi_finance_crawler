from __future__ import annotations

import asyncio

from dataclasses import dataclass
from typing import Iterable

from crawl_framework.storage.parquet_writer import (
    ParquetFileInfo,
)
from crawl_framework.storage.uploader import (
    BaseUploader,
    UploadResult,
)


@dataclass(
    frozen=True,
    slots=True,
)
class ParallelUploadConfig:
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
class ParallelUploadResult:
    uploads: tuple[
        UploadResult,
        ...
    ]

    max_queue_size: int


class ParallelUploadWorkers:
    """
    Generic bounded parallel upload workers.
    """

    def __init__(
        self,
        uploader: BaseUploader,
        *,
        config: ParallelUploadConfig | None = None,
    ) -> None:

        self.uploader = uploader

        self.config = (
            config
            or ParallelUploadConfig()
        )


    async def run(
        self,
        files: Iterable[
            ParquetFileInfo
        ],
    ) -> ParallelUploadResult:

        queue: asyncio.Queue = asyncio.Queue(
            maxsize=self.config.queue_size
        )

        sentinel = object()

        results: list[
            UploadResult
        ] = []

        max_queue_size = 0

        lock = asyncio.Lock()

        async def producer() -> None:

            nonlocal max_queue_size

            for info in files:

                await queue.put(
                    info
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

            while True:

                item = await queue.get()

                try:

                    if item is sentinel:

                        return

                    result = await asyncio.to_thread(
                        self.uploader.upload,
                        item,
                    )

                    async with lock:

                        results.append(
                            result
                        )

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

        return ParallelUploadResult(
            uploads=tuple(
                results
            ),
            max_queue_size=max_queue_size,
        )
