from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Iterable

from crawl_framework.core.dataset import (
    get_dataset_spec,
)
from crawl_framework.core.models import (
    CanonicalRecord,
)


@dataclass(
    frozen=True,
    slots=True,
)
class CrawlScope:
    """
    一个抓取任务的范围。

    例如：

        Naver 三星电子评论：

        CrawlScope(
            scope_type="instrument",
            scope_id="XKRX:005930",
            source_key="005930",
        )

        Toss 三星电子：

        CrawlScope(
            scope_type="instrument",
            scope_id="XKRX:005930",
            source_key="A005930",
        )

        网站总新闻：

        CrawlScope(
            scope_type="global",
            scope_id=None,
            source_key="global_news",
        )

    source_key:
        网站内部使用的原始标识。

        Core 不解释这个字段。
    """

    scope_type: str

    source_key: str

    scope_id: str | None = None

    metadata: dict[str, Any] = field(
        default_factory=dict
    )

    def __post_init__(
        self,
    ) -> None:

        scope_type = str(
            self.scope_type
        ).strip()

        source_key = str(
            self.source_key
        ).strip()

        scope_id = (
            str(
                self.scope_id
            ).strip()
            if self.scope_id is not None
            else None
        )

        if not scope_type:
            raise ValueError(
                "scope_type cannot be empty"
            )

        if not source_key:
            raise ValueError(
                "source_key cannot be empty"
            )

        object.__setattr__(
            self,
            "scope_type",
            scope_type,
        )

        object.__setattr__(
            self,
            "source_key",
            source_key,
        )

        object.__setattr__(
            self,
            "scope_id",
            scope_id or None,
        )


@dataclass(
    slots=True,
)
class CrawlCheckpoint:
    """
    Core 统一保存的 checkpoint 容器。

    checkpoint 内部具体含义由 Site Plugin 自己决定。

    例如：

        Naver:
            {
                "page": 100
            }

        Toss:
            {
                "oldest_seen_id": "...",
                "baseline_complete": False
            }

        HotCopper:
            {
                "page": 712
            }

    Core 只保存和恢复，不解释 state 内容。
    """

    state: dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(
    slots=True,
)
class CrawlContext:
    """
    Runtime 在执行 SitePlugin 时传入的上下文。

    当前第一版只保留通用对象容器。

    后面 transports/runtime 完成后，会加入：

        http
        browser
        logger
        checkpoint_store
        seen_store
        shutdown_event

    暂时不让 plugin 自己初始化这些资源。
    """

    runtime: Any = None

    http: Any = None

    browser: Any = None

    logger: Any = None

    extra: dict[str, Any] = field(
        default_factory=dict
    )


class SitePlugin(
    ABC
):
    """
    所有网站插件的基础接口。

    一个 SitePlugin 只负责：

        1. 声明网站信息
        2. 声明支持的数据类型
        3. discover 抓取范围
        4. crawl 获取原始数据
        5. normalize 转换 CanonicalRecord

    SitePlugin 不负责：

        PostgreSQL
        Parquet
        rsync
        全局去重
        Storage cleanup
        Storage recovery
    """


    # ========================================================
    # Site metadata
    # ========================================================

    @property
    @abstractmethod
    def site_id(
        self,
    ) -> str:
        """
        全平台唯一网站 ID。

        示例：

            naver_finance
            tossinvest
            hotcopper
            stockhouse
        """
        raise NotImplementedError


    @property
    @abstractmethod
    def country(
        self,
    ) -> str:
        """
        ISO 风格国家代码：

            KR
            AU
            CA
            US
            TW
        """
        raise NotImplementedError


    @property
    @abstractmethod
    def timezone(
        self,
    ) -> str:
        """
        网站主要时区。

        示例：

            Asia/Seoul
            Australia/Sydney
            America/Toronto
        """
        raise NotImplementedError


    # ========================================================
    # Dataset declaration
    # ========================================================

    @abstractmethod
    def datasets(
        self,
    ) -> Iterable[str]:
        """
        返回插件支持的标准 Dataset。

        示例：

            [
                "news_article",
                "news_instrument",
                "forum_post",
            ]
        """
        raise NotImplementedError


    def validate_datasets(
        self,
    ) -> tuple[str, ...]:
        """
        验证插件声明的数据集。

        不允许插件自己创造未注册 dataset。
        """

        values: list[str] = []

        seen: set[str] = set()

        for raw_name in self.datasets():

            name = str(
                raw_name
            ).strip()

            if not name:
                raise ValueError(
                    f"{self.site_id}: "
                    "dataset name cannot be empty"
                )

            get_dataset_spec(
                name
            )

            if name in seen:
                continue

            seen.add(
                name
            )

            values.append(
                name
            )

        if not values:
            raise ValueError(
                f"{self.site_id}: "
                "plugin must support "
                "at least one dataset"
            )

        return tuple(
            values
        )


    def supports_dataset(
        self,
        dataset: str,
    ) -> bool:
        """
        当前插件是否支持某 Dataset。
        """

        dataset = str(
            dataset
        ).strip()

        return (
            dataset
            in self.validate_datasets()
        )


    # ========================================================
    # Discovery
    # ========================================================

    @abstractmethod
    async def discover(
        self,
        dataset: str,
        ctx: CrawlContext,
    ) -> AsyncIterator[CrawlScope]:
        """
        发现该 Dataset 需要抓取的 scope。

        示例：

        Naver forum_post:

            yield CrawlScope(
                scope_type="instrument",
                scope_id="XKRX:005930",
                source_key="005930",
            )

        网站总新闻：

            yield CrawlScope(
                scope_type="global",
                source_key="global",
            )

        注意：

        discover 只负责发现 scope，
        不应该在这里做完整详情抓取。
        """

        if False:
            yield CrawlScope(
                scope_type="global",
                source_key="placeholder",
            )


    # ========================================================
    # Crawl
    # ========================================================

    @abstractmethod
    async def crawl(
        self,
        dataset: str,
        scope: CrawlScope,
        checkpoint: CrawlCheckpoint,
        ctx: CrawlContext,
    ) -> AsyncIterator[dict[str, Any]]:
        """
        抓取网站原始数据。

        返回网站自己的 raw dict。

        例如 Naver：

            {
                "nid": "...",
                "title": "...",
                "content": "...",
            }

        例如 Toss：

            {
                "post_id": "...",
                "author": "...",
                "content": "...",
            }

        这里不要返回 CanonicalRecord。

        CanonicalRecord 统一由 normalize() 创建。
        """

        if False:
            yield {}


    # ========================================================
    # Normalize
    # ========================================================

    @abstractmethod
    def normalize(
        self,
        dataset: str,
        raw: dict[str, Any],
        scope: CrawlScope,
    ) -> CanonicalRecord | None:
        """
        网站 raw dict
            ↓
        CanonicalRecord

        如果 raw 数据无效，可以返回 None。

        例如：

            缺少 source_id
            页面解析失败
            无法识别记录类型
        """
        raise NotImplementedError


    # ========================================================
    # Record validation
    # ========================================================

    def validate_record(
        self,
        record: CanonicalRecord,
    ) -> None:
        """
        Plugin 输出 CanonicalRecord 后的第一层检查。

        Storage Core 后续还会有更严格检查。
        """

        if record.site_id != self.site_id:
            raise ValueError(
                "record.site_id mismatch: "
                f"plugin={self.site_id!r}, "
                f"record={record.site_id!r}"
            )

        if (
            record.country.upper()
            != self.country.upper()
        ):
            raise ValueError(
                "record.country mismatch: "
                f"plugin={self.country!r}, "
                f"record={record.country!r}"
            )

        if not self.supports_dataset(
            record.dataset
        ):
            raise ValueError(
                f"{self.site_id}: "
                f"unsupported dataset "
                f"{record.dataset!r}"
            )


    # ========================================================
    # Convenience normalize wrapper
    # ========================================================

    def normalize_and_validate(
        self,
        dataset: str,
        raw: dict[str, Any],
        scope: CrawlScope,
    ) -> CanonicalRecord | None:
        """
        Runtime 后续主要调用这个方法。

        统一流程：

            normalize
                ↓
            validate_record
        """

        if not self.supports_dataset(
            dataset
        ):
            raise ValueError(
                f"{self.site_id}: "
                f"dataset {dataset!r} "
                "is not supported"
            )

        record = self.normalize(
            dataset,
            raw,
            scope,
        )

        if record is None:
            return None

        self.validate_record(
            record
        )

        return record