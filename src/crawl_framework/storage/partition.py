from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from hashlib import sha1
from pathlib import Path

from crawl_framework.core.dataset import (
    DatasetSpec,
    get_dataset_spec,
)
from crawl_framework.core.models import (
    CanonicalRecord,
)


def safe_partition_value(
    value: str | None,
) -> str:
    """
    把字符串转换成安全的 Hive-style partition value。
    """

    text = str(
        value or ""
    ).strip()

    if not text:
        return "_"

    result: list[str] = []

    for char in text:

        if (
            char.isalnum()
            or char in {
                "-",
                "_",
                ".",
                ":",
            }
        ):
            result.append(
                char
            )

        else:
            result.append(
                "_"
            )

    cleaned = "".join(
        result
    )

    while ".." in cleaned:
        cleaned = cleaned.replace(
            "..",
            "_",
        )

    return cleaned or "_"


def stable_bucket(
    value: str,
    *,
    bucket_count: int = 256,
) -> int:
    """
    根据字符串稳定分桶。

    同一个 instrument / scope 永远进入同一个 bucket。

    默认 256 个 bucket：

        0 ~ 255
    """

    if bucket_count < 1:
        raise ValueError(
            "bucket_count must be >= 1"
        )

    text = str(
        value
    ).strip()

    if not text:
        text = "_"

    digest = sha1(
        text.encode(
            "utf-8"
        )
    ).digest()

    number = int.from_bytes(
        digest[:8],
        byteorder="big",
        signed=False,
    )

    return (
        number
        % bucket_count
    )


def bucket_name(
    bucket: int,
    *,
    bucket_count: int = 256,
) -> str:
    """
    默认 256 bucket 时生成：

        00
        01
        ...
        ff

    如果未来 bucket_count > 256，
    自动扩大十六进制位数。
    """

    if bucket < 0:
        raise ValueError(
            "bucket cannot be negative"
        )

    if bucket_count < 1:
        raise ValueError(
            "bucket_count must be >= 1"
        )

    width = max(
        2,
        len(
            format(
                bucket_count - 1,
                "x",
            )
        ),
    )

    return format(
        bucket,
        f"0{width}x",
    )


@dataclass(
    frozen=True,
    slots=True,
)
class PartitionKey:
    """
    一条记录最终对应的逻辑分区。

    例如：

        site=naver_finance
        country=KR
        dataset=forum_post
        year=2026
        month=08
        day=24
        bucket=3f
    """

    site_id: str

    country: str

    dataset: str

    partition_date: date

    bucket: int

    bucket_count: int = 256


    @property
    def bucket_label(
        self,
    ) -> str:

        return bucket_name(
            self.bucket,
            bucket_count=self.bucket_count,
        )


    def relative_path(
        self,
    ) -> Path:
        """
        返回 Hive-style 相对目录。
        """

        return Path(
            f"site={safe_partition_value(self.site_id)}"
        ) / (
            f"country={safe_partition_value(self.country)}"
        ) / (
            f"dataset={safe_partition_value(self.dataset)}"
        ) / (
            f"year={self.partition_date.year:04d}"
        ) / (
            f"month={self.partition_date.month:02d}"
        ) / (
            f"day={self.partition_date.day:02d}"
        ) / (
            f"bucket={self.bucket_label}"
        )


class Partitioner:
    """
    CanonicalRecord
        ↓
    PartitionKey

    Core Storage 后续统一使用这个对象，
    Site Plugin 不允许自己决定 Parquet 路径。
    """

    def __init__(
        self,
        *,
        bucket_count: int = 256,
    ) -> None:

        if bucket_count < 1:
            raise ValueError(
                "bucket_count must be >= 1"
            )

        self.bucket_count = int(
            bucket_count
        )


    def resolve_partition_datetime(
        self,
        record: CanonicalRecord,
        spec: DatasetSpec,
    ) -> datetime:
        """
        根据 DatasetSpec 决定使用哪个时间字段分区。

        支持：

            event_time
            updated_at
            crawled_at

        如果指定字段缺失：

        event_time / updated_at
            ↓
        crawled_at

        这样可以避免坏数据直接写进未知年份目录。

        但如果 DatasetSpec 明确禁止缺失 event_time，
        则直接报错。
        """

        field_name = (
            spec.partition_time_field
        )

        value = getattr(
            record,
            field_name,
        )

        if value is None:

            if (
                field_name
                == "event_time"
                and not spec.allow_missing_event_time
            ):
                raise ValueError(
                    f"{record.dataset}: "
                    "event_time is required "
                    "for partitioning"
                )

            value = record.crawled_at

        if value is None:
            raise ValueError(
                "unable to determine "
                "partition datetime"
            )

        if value.tzinfo is None:
            value = value.replace(
                tzinfo=timezone.utc
            )

        return value.astimezone(
            timezone.utc
        )


    def resolve_bucket_key(
        self,
        record: CanonicalRecord,
    ) -> str:
        """
        决定用什么字段分桶。

        优先级：

            instrument_id
            ↓
            scope_id
            ↓
            source_id

        原因：

        股票数据优先让同一证券稳定进入同一 bucket。

        全球新闻没有 instrument 时，
        再使用 scope/source ID。
        """

        return (
            record.instrument_id
            or record.scope_id
            or record.source_id
        )


    def partition_for(
        self,
        record: CanonicalRecord,
    ) -> PartitionKey:
        """
        生成 PartitionKey。
        """

        spec = get_dataset_spec(
            record.dataset
        )

        dt = (
            self.resolve_partition_datetime(
                record,
                spec,
            )
        )

        bucket_key = (
            self.resolve_bucket_key(
                record
            )
        )

        bucket = stable_bucket(
            bucket_key,
            bucket_count=(
                spec.partition_bucket_count
                if spec.partition_bucket_count is not None
                else self.bucket_count
            ),
        )

        bucket_count = (
            spec.partition_bucket_count
            if spec.partition_bucket_count is not None
            else self.bucket_count
        )

        partition_date = dt.date()
        if spec.partition_time_granularity == "month":
            partition_date = partition_date.replace(day=1)
        elif spec.partition_time_granularity == "year":
            partition_date = partition_date.replace(month=1, day=1)

        return PartitionKey(
            site_id=record.site_id,
            country=record.country,
            dataset=record.dataset,
            partition_date=partition_date,
            bucket=bucket,
            bucket_count=bucket_count,
        )


    def path_for(
        self,
        record: CanonicalRecord,
    ) -> Path:
        """
        直接返回记录对应的相对分区路径。
        """

        return (
            self.partition_for(
                record
            )
            .relative_path()
        )
