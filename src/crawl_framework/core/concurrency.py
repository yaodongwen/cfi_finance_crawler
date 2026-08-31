from __future__ import annotations

from dataclasses import dataclass


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
