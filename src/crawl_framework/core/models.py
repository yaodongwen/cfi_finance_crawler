from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Literal


MutationPolicy = Literal[
    "immutable",
    "latest",
    "versioned",
]


def utc_now() -> datetime:
    """
    返回带 UTC timezone 的当前时间。
    """
    return datetime.now(timezone.utc)


def clean_text(
    value: Any,
) -> str:
    """
    把任意值转换为干净字符串。

    Core 层只做非常轻量的清洗。
    网站自己的复杂字段清洗应放在 site plugin。
    """
    if value is None:
        return ""

    return str(value).strip()


def normalize_optional_text(
    value: Any,
) -> str | None:
    """
    空字符串统一转换成 None。
    """
    text = clean_text(value)

    return text or None


def normalize_datetime(
    value: datetime | str | None,
) -> datetime | None:
    """
    将 datetime / ISO 字符串统一转换成 UTC datetime。

    支持：
        2026-08-24T10:30:00Z
        2026-08-24T10:30:00+09:00
        datetime(...)

    注意：
    网站特殊时间格式不要在这里处理。
    比如：
        "3小时前"
        "방금"
        "24 Aug 2026"

    这些应由 Site Plugin 转换后再交给 Core。
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        dt = value

    elif isinstance(value, str):
        text = value.strip()

        if not text:
            return None

        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        try:
            dt = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(
                f"无法解析 ISO datetime: {value!r}"
            ) from exc

    else:
        raise TypeError(
            "datetime value must be datetime, str or None"
        )

    if dt.tzinfo is None:
        dt = dt.replace(
            tzinfo=timezone.utc
        )

    return dt.astimezone(
        timezone.utc
    )


def datetime_to_iso_z(
    value: datetime | None,
) -> str | None:
    """
    datetime -> UTC ISO8601 Z 字符串。
    """
    if value is None:
        return None

    value = normalize_datetime(
        value
    )

    assert value is not None

    return (
        value.isoformat(
            timespec="seconds"
        )
        .replace(
            "+00:00",
            "Z",
        )
    )


def stable_sha256(
    *parts: Any,
) -> str:
    """
    对多个字段生成稳定 SHA256。

    使用不可见分隔符，降低普通字符串拼接产生歧义的可能。
    """
    separator = "\x1f"

    payload = separator.join(
        clean_text(part)
        for part in parts
    )

    return sha256(
        payload.encode("utf-8")
    ).hexdigest()


@dataclass(
    frozen=True,
    slots=True,
)
class InstrumentRef:
    """
    全平台统一证券标识。

    推荐 instrument_id：

        XKRX:005930
        XASX:BHP
        XTSE:SHOP
        XNAS:AAPL

    source_symbol 是网站自己看到的代码：

        Naver:
            005930

        Toss:
            A005930

        HotCopper:
            BHP
    """

    instrument_id: str

    source_symbol: str | None = None

    name: str | None = None

    market: str | None = None

    country: str | None = None

    currency: str | None = None

    source_url: str | None = None

    def __post_init__(
        self,
    ) -> None:
        instrument_id = clean_text(
            self.instrument_id
        )

        if not instrument_id:
            raise ValueError(
                "instrument_id cannot be empty"
            )

        object.__setattr__(
            self,
            "instrument_id",
            instrument_id,
        )

        for field_name in (
            "source_symbol",
            "name",
            "market",
            "country",
            "currency",
            "source_url",
        ):
            object.__setattr__(
                self,
                field_name,
                normalize_optional_text(
                    getattr(
                        self,
                        field_name,
                    )
                ),
            )


@dataclass(
    frozen=True,
    slots=True,
)
class RecordRelation:
    """
    一条记录与证券之间的关系。

    例如：

        news_123
            ->
        XKRX:005930

    relation_type 示例：

        mentions
        primary
        related
        holding
        author_focus
    """

    instrument_id: str

    relation_type: str = "related"

    confidence: float | None = None

    def __post_init__(
        self,
    ) -> None:
        instrument_id = clean_text(
            self.instrument_id
        )

        relation_type = clean_text(
            self.relation_type
        )

        if not instrument_id:
            raise ValueError(
                "RecordRelation.instrument_id "
                "cannot be empty"
            )

        if not relation_type:
            raise ValueError(
                "RecordRelation.relation_type "
                "cannot be empty"
            )

        if self.confidence is not None:
            if not (
                0.0
                <= self.confidence
                <= 1.0
            ):
                raise ValueError(
                    "confidence must be between "
                    "0 and 1"
                )

        object.__setattr__(
            self,
            "instrument_id",
            instrument_id,
        )

        object.__setattr__(
            self,
            "relation_type",
            relation_type,
        )


@dataclass(
    slots=True,
)
class CanonicalRecord:
    """
    Crawl Framework 的核心统一数据模型。

    所有网站数据最终都必须转换成 CanonicalRecord。

    Core 不关心网站原始字段叫什么：

        nid
        post_id
        article_id
        contentParams
        discussion_id

    Site Plugin 的职责是把它们转换成这里的统一字段。
    """

    # --------------------------------------------------------
    # 数据来源
    # --------------------------------------------------------

    site_id: str

    country: str

    dataset: str

    source_id: str


    # --------------------------------------------------------
    # Scope
    #
    # 表示这条数据属于什么范围。
    #
    # 示例：
    #
    # instrument:
    #     scope_type="instrument"
    #     scope_id="XKRX:005930"
    #
    # author:
    #     scope_type="author"
    #     scope_id="hotcopper:user:123"
    #
    # global:
    #     scope_type="global"
    #     scope_id=None
    # --------------------------------------------------------

    scope_type: str = "global"

    scope_id: str | None = None

    # Optional logical-identity scope. This lets an operational scope such as
    # a crawl month differ from the source record's stable identity boundary.
    identity_scope_type: str | None = None
    identity_scope_id: str | None = None


    # --------------------------------------------------------
    # 常见证券
    #
    # 对只有一个主要证券的数据方便直接查询。
    #
    # 多股票新闻则使用 relations。
    # --------------------------------------------------------

    instrument_id: str | None = None


    # --------------------------------------------------------
    # 时间
    # --------------------------------------------------------

    event_time: datetime | None = None

    updated_at: datetime | None = None

    crawled_at: datetime = field(
        default_factory=utc_now
    )


    # --------------------------------------------------------
    # 正文
    # --------------------------------------------------------

    title: str | None = None

    content: str | None = None


    # --------------------------------------------------------
    # 作者
    # --------------------------------------------------------

    author_id: str | None = None

    author_name: str | None = None


    # --------------------------------------------------------
    # 来源
    # --------------------------------------------------------

    source_url: str | None = None


    # --------------------------------------------------------
    # 原始/扩展字段
    #
    # 网站专有信息全部放这里。
    #
    # 例如：
    #
    # {
    #     "views": 100,
    #     "likes": 20,
    #     "reply_count": 5
    # }
    # --------------------------------------------------------

    payload: dict[str, Any] = field(
        default_factory=dict
    )


    # --------------------------------------------------------
    # 与证券关联
    # --------------------------------------------------------

    relations: list[
        RecordRelation
    ] = field(
        default_factory=list
    )


    # --------------------------------------------------------
    # Schema
    # --------------------------------------------------------

    schema_version: int = 1


    # --------------------------------------------------------
    # 更新策略
    # --------------------------------------------------------

    mutation_policy: MutationPolicy = (
        "immutable"
    )


    def __post_init__(
        self,
    ) -> None:

        self.site_id = clean_text(
            self.site_id
        )

        self.country = clean_text(
            self.country
        ).upper()

        self.dataset = clean_text(
            self.dataset
        )

        self.source_id = clean_text(
            self.source_id
        )

        self.scope_type = clean_text(
            self.scope_type
        ) or "global"

        self.scope_id = (
            normalize_optional_text(
                self.scope_id
            )
        )

        self.identity_scope_type = normalize_optional_text(
            self.identity_scope_type
        )

        self.identity_scope_id = normalize_optional_text(
            self.identity_scope_id
        )

        self.instrument_id = (
            normalize_optional_text(
                self.instrument_id
            )
        )

        self.title = (
            normalize_optional_text(
                self.title
            )
        )

        self.content = (
            normalize_optional_text(
                self.content
            )
        )

        self.author_id = (
            normalize_optional_text(
                self.author_id
            )
        )

        self.author_name = (
            normalize_optional_text(
                self.author_name
            )
        )

        self.source_url = (
            normalize_optional_text(
                self.source_url
            )
        )

        self.event_time = (
            normalize_datetime(
                self.event_time
            )
        )

        self.updated_at = (
            normalize_datetime(
                self.updated_at
            )
        )

        crawled_at = normalize_datetime(
            self.crawled_at
        )

        if crawled_at is None:
            crawled_at = utc_now()

        self.crawled_at = crawled_at

        if not self.site_id:
            raise ValueError(
                "site_id cannot be empty"
            )

        if not self.country:
            raise ValueError(
                "country cannot be empty"
            )

        if not self.dataset:
            raise ValueError(
                "dataset cannot be empty"
            )

        if not self.source_id:
            raise ValueError(
                "source_id cannot be empty"
            )

        if self.schema_version < 1:
            raise ValueError(
                "schema_version must be >= 1"
            )

        if self.mutation_policy not in {
            "immutable",
            "latest",
            "versioned",
        }:
            raise ValueError(
                "invalid mutation_policy: "
                f"{self.mutation_policy}"
            )


    @property
    def record_uid(
        self,
    ) -> str:
        """
        全平台唯一记录 ID。

        关键规则：

            site_id
            +
            dataset
            +
            scope_type
            +
            scope_id
            +
            source_id

        同样的 source_id 出现在不同网站、不同数据集、
        不同股票下都不会发生冲突。
        """

        identity_scope_type, identity_scope_id = self._identity_scope

        return stable_sha256(
            self.site_id,
            self.dataset,
            identity_scope_type,
            identity_scope_id or "",
            self.source_id,
        )


    @property
    def _identity_scope(self) -> tuple[str, str | None]:
        if self.identity_scope_type is None:
            return self.scope_type, self.scope_id

        return self.identity_scope_type, self.identity_scope_id


    @property
    def version_hash(
        self,
    ) -> str:
        """
        当前版本内容 Hash。

        用于判断：

            同一个 record_uid

        是否发生了正文、标题、作者、更新时间等变化。

        注意：

        crawled_at 不参与 hash。

        否则每次重新抓取都会产生不同 version。
        """

        relation_text = "|".join(
            sorted(
                (
                    f"{r.instrument_id}:"
                    f"{r.relation_type}:"
                    f"{r.confidence}"
                )
                for r in self.relations
            )
        )

        payload_text = repr(
            sorted(
                self.payload.items(),
                key=lambda item: item[0],
            )
        )

        identity_scope_type, identity_scope_id = self._identity_scope

        return stable_sha256(
            self.site_id,
            self.dataset,
            self.source_id,
            identity_scope_type,
            identity_scope_id or "",
            self.instrument_id or "",
            datetime_to_iso_z(
                self.event_time
            )
            or "",
            datetime_to_iso_z(
                self.updated_at
            )
            or "",
            self.title or "",
            self.content or "",
            self.author_id or "",
            self.author_name or "",
            self.source_url or "",
            relation_text,
            payload_text,
        )


    @property
    def event_date(
        self,
    ):
        """
        分区时优先使用 event_time。

        如果网站没有事件时间，
        后面的 Partitioner 再决定如何 fallback。
        """

        if self.event_time is None:
            return None

        return self.event_time.date()


    def to_dict(
        self,
    ) -> dict[str, Any]:
        """
        转换成适合 JSON / PyArrow / Parquet 使用的 dict。
        """

        return {
            "schema_version":
                self.schema_version,

            "record_uid":
                self.record_uid,

            "version_hash":
                self.version_hash,

            "mutation_policy":
                self.mutation_policy,

            "site_id":
                self.site_id,

            "country":
                self.country,

            "dataset":
                self.dataset,

            "source_id":
                self.source_id,

            "scope_type":
                self.scope_type,

            "scope_id":
                self.scope_id,

            "instrument_id":
                self.instrument_id,

            "event_time":
                datetime_to_iso_z(
                    self.event_time
                ),

            "updated_at":
                datetime_to_iso_z(
                    self.updated_at
                ),

            "crawled_at":
                datetime_to_iso_z(
                    self.crawled_at
                ),

            "title":
                self.title,

            "content":
                self.content,

            "author_id":
                self.author_id,

            "author_name":
                self.author_name,

            "source_url":
                self.source_url,

            "relations": [
                {
                    "instrument_id":
                        relation.instrument_id,

                    "relation_type":
                        relation.relation_type,

                    "confidence":
                        relation.confidence,
                }
                for relation
                in self.relations
            ],

            "payload":
                self.payload,
        }
