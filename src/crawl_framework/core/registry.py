from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pyarrow as pa

from crawl_framework.core.dataset import (
    DATASETS,
    DatasetSpec,
    get_dataset_spec,
)


# ============================================================
# Common physical fields
#
# 所有 CanonicalRecord 都先使用这一套稳定的基础 Schema。
#
# 注意：
#
# payload_json / relations_json 暂时保存为 JSON string。
#
# 后续不同 dataset 可以在自己的 schema 中增加 typed columns。
# ============================================================


COMMON_FIELDS: tuple[
    pa.Field,
    ...
] = (
    pa.field(
        "schema_version",
        pa.int32(),
        nullable=False,
    ),

    pa.field(
        "record_uid",
        pa.string(),
        nullable=False,
    ),

    pa.field(
        "version_hash",
        pa.string(),
        nullable=False,
    ),

    pa.field(
        "mutation_policy",
        pa.string(),
        nullable=False,
    ),

    pa.field(
        "site_id",
        pa.string(),
        nullable=False,
    ),

    pa.field(
        "country",
        pa.string(),
        nullable=False,
    ),

    pa.field(
        "dataset",
        pa.string(),
        nullable=False,
    ),

    pa.field(
        "source_id",
        pa.string(),
        nullable=False,
    ),

    pa.field(
        "scope_type",
        pa.string(),
        nullable=False,
    ),

    pa.field(
        "scope_id",
        pa.string(),
        nullable=True,
    ),

    pa.field(
        "instrument_id",
        pa.string(),
        nullable=True,
    ),

    pa.field(
        "event_time",
        pa.timestamp(
            "us",
            tz="UTC",
        ),
        nullable=True,
    ),

    pa.field(
        "updated_at",
        pa.timestamp(
            "us",
            tz="UTC",
        ),
        nullable=True,
    ),

    pa.field(
        "crawled_at",
        pa.timestamp(
            "us",
            tz="UTC",
        ),
        nullable=False,
    ),

    pa.field(
        "title",
        pa.string(),
        nullable=True,
    ),

    pa.field(
        "content",
        pa.string(),
        nullable=True,
    ),

    pa.field(
        "author_id",
        pa.string(),
        nullable=True,
    ),

    pa.field(
        "author_name",
        pa.string(),
        nullable=True,
    ),

    pa.field(
        "source_url",
        pa.string(),
        nullable=True,
    ),

    pa.field(
        "payload_json",
        pa.large_string(),
        nullable=False,
    ),

    pa.field(
        "relations_json",
        pa.large_string(),
        nullable=False,
    ),
)


COMMON_SCHEMA = pa.schema(
    COMMON_FIELDS
)


@dataclass(
    frozen=True,
    slots=True,
)
class DatasetSchema:
    """
    Dataset 的物理存储 Schema。

    spec:
        逻辑层 DatasetSpec

    arrow_schema:
        Parquet/PyArrow 使用的固定物理 Schema

    schema_version:
        当前物理 Schema 版本
    """

    name: str

    spec: DatasetSpec

    arrow_schema: pa.Schema

    schema_version: int = 1


    def __post_init__(
        self,
    ) -> None:

        if not self.name:
            raise ValueError(
                "DatasetSchema.name "
                "cannot be empty"
            )

        if self.schema_version < 1:
            raise ValueError(
                "schema_version must be >= 1"
            )

        if (
            self.name
            != self.spec.name
        ):
            raise ValueError(
                "DatasetSchema name mismatch: "
                f"{self.name!r} != "
                f"{self.spec.name!r}"
            )


class DatasetSchemaRegistry:
    """
    Dataset Schema Registry。

    Storage 层不允许自己猜 Schema。

    所有 dataset 的 PyArrow Schema
    必须从这里取得。
    """

    def __init__(
        self,
    ) -> None:

        self._schemas: dict[
            str,
            DatasetSchema,
        ] = {}


    def register(
        self,
        schema: DatasetSchema,
        *,
        replace: bool = False,
    ) -> None:

        name = schema.name

        if (
            name in self._schemas
            and not replace
        ):
            raise KeyError(
                "dataset schema already "
                f"registered: {name!r}"
            )

        self._schemas[
            name
        ] = schema


    def get(
        self,
        name: str,
    ) -> DatasetSchema:

        key = str(
            name
        ).strip()

        try:
            return self._schemas[
                key
            ]

        except KeyError as exc:
            raise KeyError(
                "dataset schema not "
                f"registered: {key!r}"
            ) from exc


    def contains(
        self,
        name: str,
    ) -> bool:

        return (
            str(name).strip()
            in self._schemas
        )


    def names(
        self,
    ) -> list[str]:

        return sorted(
            self._schemas
        )


    def values(
        self,
    ) -> tuple[
        DatasetSchema,
        ...
    ]:

        return tuple(
            self._schemas[
                name
            ]
            for name
            in self.names()
        )


def make_default_dataset_schema(
    name: str,
) -> DatasetSchema:
    """
    第一阶段所有 dataset 先使用 COMMON_SCHEMA。

    后续可以逐个扩展：

        forum_post
        news_article
        research_report
        holding_position

    但不会影响 Core API。
    """

    spec = get_dataset_spec(
        name
    )

    return DatasetSchema(
        name=name,
        spec=spec,
        arrow_schema=COMMON_SCHEMA,
        schema_version=1,
    )


def build_default_registry() -> DatasetSchemaRegistry:
    """
    为 dataset.py 中所有已注册 dataset
    创建默认物理 Schema。
    """

    registry = (
        DatasetSchemaRegistry()
    )

    for name in DATASETS:

        registry.register(
            make_default_dataset_schema(
                name
            )
        )

    return registry


DEFAULT_SCHEMA_REGISTRY = (
    build_default_registry()
)


def get_dataset_schema(
    name: str,
) -> DatasetSchema:
    """
    全局快捷入口。
    """

    return (
        DEFAULT_SCHEMA_REGISTRY
        .get(
            name
        )
    )


def list_dataset_schemas() -> list[str]:
    """
    返回所有已注册物理 Schema。
    """

    return (
        DEFAULT_SCHEMA_REGISTRY
        .names()
    )