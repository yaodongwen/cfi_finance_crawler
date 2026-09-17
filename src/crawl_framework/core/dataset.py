from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from crawl_framework.core.models import MutationPolicy


PartitionTimeField = Literal[
    "event_time",
    "updated_at",
    "crawled_at",
]

PartitionTimeGranularity = Literal["day", "month", "year"]


@dataclass(
    frozen=True,
    slots=True,
)
class DatasetSpec:
    """
    描述一个标准 Dataset 在 Crawl Framework 中的行为。

    Site Plugin 只负责把网站数据转换成 CanonicalRecord。

    DatasetSpec 决定：

    - 数据是否必须关联证券
    - 默认 mutation policy
    - 分区使用哪个时间字段
    - 是否允许没有正文
    - 是否允许没有 event_time
    - 默认 scope_type
    - 是否属于 relation dataset
    """

    name: str

    mutation_policy: MutationPolicy

    partition_time_field: PartitionTimeField

    default_scope_type: str

    requires_instrument: bool = False

    allows_multiple_instruments: bool = False

    allow_missing_event_time: bool = True

    allow_missing_content: bool = True

    relation_dataset: bool = False

    partition_time_granularity: PartitionTimeGranularity = "day"

    partition_bucket_count: int | None = None

    legacy_partition_bucket_counts: tuple[int, ...] = ()

    def __post_init__(self) -> None:
        if self.partition_time_granularity not in {"day", "month", "year"}:
            raise ValueError("invalid partition_time_granularity")
        if self.partition_bucket_count is not None and self.partition_bucket_count < 1:
            raise ValueError("partition_bucket_count must be positive")
        if any(count < 1 for count in self.legacy_partition_bucket_counts):
            raise ValueError("legacy_partition_bucket_counts must be positive")


# ============================================================
# Dataset definitions
# ============================================================


NEWS_ARTICLE = DatasetSpec(
    name="news_article",
    mutation_policy="versioned",
    partition_time_field="event_time",
    default_scope_type="global",
    requires_instrument=False,
    allows_multiple_instruments=True,
    allow_missing_event_time=True,
    allow_missing_content=True,
)


NEWS_INSTRUMENT = DatasetSpec(
    name="news_instrument",
    mutation_policy="immutable",
    partition_time_field="crawled_at",
    default_scope_type="instrument",
    requires_instrument=True,
    allows_multiple_instruments=False,
    allow_missing_event_time=True,
    allow_missing_content=True,
    relation_dataset=True,
)


FORUM_POST = DatasetSpec(
    name="forum_post",
    mutation_policy="versioned",
    partition_time_field="event_time",
    default_scope_type="instrument",
    requires_instrument=False,
    allows_multiple_instruments=False,
    allow_missing_event_time=True,
    allow_missing_content=True,
)


COMMENT = DatasetSpec(
    name="comment",
    mutation_policy="versioned",
    partition_time_field="event_time",
    default_scope_type="record",
    requires_instrument=False,
    allows_multiple_instruments=False,
    allow_missing_event_time=True,
    allow_missing_content=True,
)


RESEARCH_REPORT = DatasetSpec(
    name="research_report",
    mutation_policy="versioned",
    partition_time_field="event_time",
    default_scope_type="global",
    requires_instrument=False,
    allows_multiple_instruments=True,
    allow_missing_event_time=True,
    allow_missing_content=True,
)


RESEARCH_INSTRUMENT = DatasetSpec(
    name="research_instrument",
    mutation_policy="immutable",
    partition_time_field="crawled_at",
    default_scope_type="instrument",
    requires_instrument=True,
    allows_multiple_instruments=False,
    allow_missing_event_time=True,
    allow_missing_content=True,
    relation_dataset=True,
)


FINANCIAL_REPORT = DatasetSpec(
    name="financial_report",
    mutation_policy="versioned",
    partition_time_field="event_time",
    default_scope_type="instrument",
    requires_instrument=True,
    allows_multiple_instruments=True,
    allow_missing_event_time=True,
    allow_missing_content=True,
    partition_time_granularity="year",
    partition_bucket_count=1,
    legacy_partition_bucket_counts=(256,),
)


FINANCIAL_REPORT_INSTRUMENT = DatasetSpec(
    name="financial_report_instrument",
    mutation_policy="immutable",
    partition_time_field="crawled_at",
    default_scope_type="instrument",
    requires_instrument=True,
    allows_multiple_instruments=False,
    allow_missing_event_time=True,
    allow_missing_content=True,
    relation_dataset=True,
    partition_time_granularity="year",
    partition_bucket_count=1,
    legacy_partition_bucket_counts=(256,),
)


AUTHOR_PROFILE = DatasetSpec(
    name="author_profile",
    mutation_policy="versioned",
    partition_time_field="updated_at",
    default_scope_type="author",
    requires_instrument=False,
    allows_multiple_instruments=True,
    allow_missing_event_time=True,
    allow_missing_content=True,
)


AUTHOR_POST = DatasetSpec(
    name="author_post",
    mutation_policy="versioned",
    partition_time_field="event_time",
    default_scope_type="author",
    requires_instrument=False,
    allows_multiple_instruments=True,
    allow_missing_event_time=True,
    allow_missing_content=True,
)


HOLDING_SNAPSHOT = DatasetSpec(
    name="holding_snapshot",
    mutation_policy="immutable",
    partition_time_field="event_time",
    default_scope_type="author",
    requires_instrument=False,
    allows_multiple_instruments=True,
    allow_missing_event_time=False,
    allow_missing_content=True,
)


HOLDING_POSITION = DatasetSpec(
    name="holding_position",
    mutation_policy="immutable",
    partition_time_field="event_time",
    default_scope_type="instrument",
    requires_instrument=True,
    allows_multiple_instruments=False,
    allow_missing_event_time=False,
    allow_missing_content=True,
)


INSTRUMENT = DatasetSpec(
    name="instrument",
    mutation_policy="versioned",
    partition_time_field="crawled_at",
    default_scope_type="instrument",
    requires_instrument=True,
    allows_multiple_instruments=False,
    allow_missing_event_time=True,
    allow_missing_content=True,
)


INSTRUMENT_RELATION = DatasetSpec(
    name="instrument_relation",
    mutation_policy="versioned",
    partition_time_field="event_time",
    default_scope_type="instrument",
    requires_instrument=True,
    allows_multiple_instruments=True,
    allow_missing_event_time=True,
    allow_missing_content=True,
    relation_dataset=True,
)


ATTACHMENT = DatasetSpec(
    name="attachment",
    mutation_policy="immutable",
    partition_time_field="crawled_at",
    default_scope_type="record",
    requires_instrument=False,
    allows_multiple_instruments=True,
    allow_missing_event_time=True,
    allow_missing_content=True,
)


# ============================================================
# Registry
# ============================================================


DATASETS: dict[
    str,
    DatasetSpec,
] = {
    spec.name: spec
    for spec in (
        NEWS_ARTICLE,
        NEWS_INSTRUMENT,
        FORUM_POST,
        COMMENT,
        RESEARCH_REPORT,
        RESEARCH_INSTRUMENT,
        FINANCIAL_REPORT,
        FINANCIAL_REPORT_INSTRUMENT,
        AUTHOR_PROFILE,
        AUTHOR_POST,
        HOLDING_SNAPSHOT,
        HOLDING_POSITION,
        INSTRUMENT,
        INSTRUMENT_RELATION,
        ATTACHMENT,
    )
}


def get_dataset_spec(
    name: str,
) -> DatasetSpec:
    """
    根据名称取得 DatasetSpec。

    不允许未知 dataset 静默进入 Storage。
    """
    key = str(
        name
    ).strip()

    try:
        return DATASETS[
            key
        ]

    except KeyError as exc:
        raise KeyError(
            f"unknown dataset: {key!r}"
        ) from exc


def is_registered_dataset(
    name: str,
) -> bool:
    """
    判断 dataset 是否已注册。
    """
    return (
        str(name).strip()
        in DATASETS
    )


def list_datasets() -> list[str]:
    """
    返回所有标准 dataset 名称。
    """
    return sorted(
        DATASETS
    )
