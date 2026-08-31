from __future__ import annotations

import subprocess
import tempfile

import pyarrow.dataset as ds
from dataclasses import dataclass
from datetime import (
    date,
    datetime,
    time,
    timedelta,
    timezone,
)
from zoneinfo import (
    ZoneInfo,
    ZoneInfoNotFoundError,
)
from pathlib import Path
from time import perf_counter
from typing import (
    Iterable,
    Sequence,
)

import pyarrow as pa
import pyarrow.parquet as pq

from crawl_framework.storage.postgres import (
    CatalogDataFile,
    PostgresCatalog,
)

from crawl_framework.storage.partition import (
    bucket_name,
    stable_bucket,
)


# ============================================================
# Constants
# ============================================================


UTC = timezone.utc


DEFAULT_COLUMNS: tuple[str, ...] = (
    "schema_version",
    "record_uid",
    "version_hash",
    "mutation_policy",
    "site_id",
    "country",
    "dataset",
    "source_id",
    "scope_type",
    "scope_id",
    "instrument_id",
    "event_time",
    "updated_at",
    "crawled_at",
    "title",
    "content",
    "author_id",
    "author_name",
    "source_url",
    "payload_json",
    "relations_json",
)


# ============================================================
# Query specification
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class QuerySpec:
    """
    Canonical Parquet 查询条件。

    第一版支持：

        site_id
        dataset
        country
        instrument_id
        instrument_ids
        start_date
        end_date
        record_uid
        columns
        timezone

    start_date / end_date 表示 timezone 所指定时区中的
    本地自然日，而不是 UTC 自然日。

    例如：

        timezone = "Asia/Seoul"
        start_date = 2026-08-26
        end_date = 2026-08-26

    实际 event_time UTC 过滤范围为：

        >= 2026-08-25 15:00:00 UTC
        <  2026-08-26 15:00:00 UTC

    日期区间仍然是闭区间：

        start_date <= local date <= end_date

    默认 timezone="UTC"，保持向后兼容。
    """

    site_id: str

    dataset: str

    country: str | None = None

    instrument_id: str | None = None

    instrument_ids: tuple[str, ...] | None = None

    start_date: date | str | None = None

    end_date: date | str | None = None

    record_uid: str | None = None

    columns: tuple[str, ...] | None = None

    timezone: str = "UTC"

# ============================================================
# Query statistics
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class QueryStats:
    """
    用于观察查询剪枝效果。
    """

    catalog_files: int

    parquet_files_read: int

    rows_read: int

    rows_returned: int


@dataclass(
    slots=True,
)
class StreamingQueryStats:
    """
    流式查询可观测性统计。

    candidate_physical_rows 来自 Catalog row_count，
    表示候选文件物理行数，不等同于 Arrow 实际返回行数。
    """

    catalog_sql_calls: int = 0

    catalog_files: int = 0

    parquet_files_materialized: int = 0

    parquet_files_read: int = 0

    candidate_physical_rows: int = 0

    rows_yielded: int = 0

    bytes_materialized: int = 0

    batches_yielded: int = 0

    query_duration_seconds: float = 0.0

    catalog_duration_seconds: float = 0.0

    materialization_duration_seconds: float = 0.0

    scan_duration_seconds: float = 0.0


# ============================================================
# Query result
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class QueryResult:
    """
    查询结果。
    """

    table: pa.Table

    stats: QueryStats


# ============================================================
# Date helpers
# ============================================================


def normalize_date(
    value: date | str | None,
) -> date | None:
    """
    date / YYYY-MM-DD -> date。
    """

    if value is None:

        return None

    if isinstance(
        value,
        datetime,
    ):

        return value.date()

    if isinstance(
        value,
        date,
    ):

        return value

    text = str(
        value
    ).strip()

    if not text:

        return None

    try:

        return date.fromisoformat(
            text
        )

    except ValueError as exc:

        raise ValueError(
            "date must use YYYY-MM-DD: "
            f"{value!r}"
        ) from exc


def normalize_timezone(
    value: str,
) -> ZoneInfo:
    """
    IANA timezone name -> ZoneInfo。

    例如：

        UTC
        Asia/Seoul
        Asia/Tokyo
        Australia/Sydney
        America/New_York
    """

    name = normalize_required_text(
        value,
        field_name="timezone",
    )

    try:

        return ZoneInfo(
            name
        )

    except ZoneInfoNotFoundError as exc:

        raise ValueError(
            "unknown timezone: "
            f"{name!r}"
        ) from exc


def local_start_of_date_as_utc(
    value: date,
    *,
    tz: ZoneInfo,
) -> datetime:
    """
    将某个本地自然日的 00:00 转换为 UTC。

    例如：

        2026-08-26 00:00 Asia/Seoul

    ->

        2026-08-25 15:00 UTC
    """

    local_dt = datetime.combine(
        value,
        time.min,
        tzinfo=tz,
    )

    return local_dt.astimezone(
        UTC
    )

# ============================================================
# Validation helpers
# ============================================================


def normalize_required_text(
    value: str,
    *,
    field_name: str,
) -> str:

    text = str(
        value
    ).strip()

    if not text:

        raise ValueError(
            f"{field_name} cannot be empty"
        )

    return text


def normalize_optional_text(
    value: str | None,
) -> str | None:

    if value is None:

        return None

    text = str(
        value
    ).strip()

    return (
        text
        or None
    )


def normalize_columns(
    columns: Sequence[str] | None,
) -> tuple[str, ...]:

    if columns is None:

        return DEFAULT_COLUMNS

    result: list[str] = []

    seen: set[str] = set()

    for raw in columns:

        name = str(
            raw
        ).strip()

        if not name:

            raise ValueError(
                "column name cannot be empty"
            )

        if (
            name
            not in DEFAULT_COLUMNS
        ):

            raise ValueError(
                "unknown canonical column: "
                f"{name}"
            )

        if name in seen:

            continue

        seen.add(
            name
        )

        result.append(
            name
        )

    if not result:

        raise ValueError(
            "columns cannot be empty"
        )

    return tuple(
        result
    )


# ============================================================
# Query normalization
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class NormalizedQuery:
    site_id: str

    dataset: str

    country: str | None

    instrument_id: str | None

    instrument_ids: tuple[str, ...]

    start_date: date | None

    end_date: date | None

    timezone_name: str

    start_time: datetime | None

    end_time_exclusive: datetime | None

    record_uid: str | None

    columns: tuple[str, ...]

    bucket: str | None

    buckets: tuple[str, ...]

def instrument_bucket(
    instrument_id: str | None,
    *,
    bucket_count: int = 256,
) -> str | None:
    """
    根据 canonical instrument_id
    计算与 Partitioner 完全一致的 bucket label。

    注意：

        只有明确存在 instrument_id 时才可剪枝。

    不能对：

        scope_id
        source_id
        record_uid

    擅自推导 bucket，因为查询端并不知道
    写入时究竟使用了哪个 fallback key。
    """

    if instrument_id is None:

        return None

    normalized = normalize_optional_text(
        instrument_id
    )

    if normalized is None:

        return None

    bucket = stable_bucket(
        normalized,
        bucket_count=bucket_count,
    )

    return bucket_name(
        bucket,
        bucket_count=bucket_count,
    )


def normalize_instrument_ids(
    *,
    instrument_id: str | None,
    instrument_ids: Sequence[str] | None,
) -> tuple[str, ...]:
    """
    合并单证券与多证券查询参数，并稳定去重。

    兼容：

        instrument_id="XKRX:005930"

    新增：

        instrument_ids=(
            "XKRX:005930",
            "XKRX:000660",
        )

    如果两者同时提供，则合并后去重。
    """

    result: list[str] = []

    seen: set[str] = set()

    values: list[str] = []

    if instrument_id is not None:

        values.append(
            instrument_id
        )

    if instrument_ids is not None:

        values.extend(
            instrument_ids
        )

    for raw in values:

        normalized = (
            normalize_optional_text(
                raw
            )
        )

        if normalized is None:

            continue

        if normalized in seen:

            continue

        seen.add(
            normalized
        )

        result.append(
            normalized
        )

    return tuple(
        result
    )


def instrument_buckets(
    instrument_ids: Sequence[str],
    *,
    bucket_count: int = 256,
) -> tuple[str, ...]:
    """
    为多证券查询计算去重后的稳定 bucket labels。

    多只证券可能碰撞到同一个 bucket；
    Catalog 只需要查询该 bucket 一次。
    """

    result: list[str] = []

    seen: set[str] = set()

    for instrument_id in instrument_ids:

        bucket = instrument_bucket(
            instrument_id,
            bucket_count=bucket_count,
        )

        if bucket is None:

            continue

        if bucket in seen:

            continue

        seen.add(
            bucket
        )

        result.append(
            bucket
        )

    return tuple(
        result
    )


def normalize_query(
    spec: QuerySpec,
) -> NormalizedQuery:

    site_id = normalize_required_text(
        spec.site_id,
        field_name="site_id",
    )

    dataset = normalize_required_text(
        spec.dataset,
        field_name="dataset",
    )

    country = normalize_optional_text(
        spec.country
    )

    if country is not None:

        country = country.upper()

    instrument_id = (
        normalize_optional_text(
            spec.instrument_id
        )
    )

    instrument_ids = (
        normalize_instrument_ids(
            instrument_id=instrument_id,
            instrument_ids=spec.instrument_ids,
        )
    )

    normalized_single_instrument_id = (
        instrument_ids[0]
        if len(
            instrument_ids
        ) == 1
        else None
    )

    record_uid = (
        normalize_optional_text(
            spec.record_uid
        )
    )

    start_date = normalize_date(
        spec.start_date
    )

    end_date = normalize_date(
        spec.end_date
    )

    if (
        start_date is not None
        and
        end_date is not None
        and
        start_date > end_date
    ):

        raise ValueError(
            "start_date cannot be "
            "after end_date"
        )

    timezone_name = (
        normalize_required_text(
            spec.timezone,
            field_name="timezone",
        )
    )

    tz = normalize_timezone(
        timezone_name
    )

    start_time = (
        local_start_of_date_as_utc(
            start_date,
            tz=tz,
        )
        if start_date is not None
        else None
    )

    end_time_exclusive = (
        local_start_of_date_as_utc(
            end_date
            +
            timedelta(
                days=1
            ),
            tz=tz,
        )
        if end_date is not None
        else None
    )

    columns = normalize_columns(
        spec.columns
    )

    buckets = instrument_buckets(
        instrument_ids
    )

    bucket = (
        buckets[0]
        if len(
            buckets
        ) == 1
        else None
    )

    return NormalizedQuery(
        site_id=site_id,
        dataset=dataset,
        country=country,
        instrument_id=(
            normalized_single_instrument_id
        ),
        instrument_ids=instrument_ids,
        start_date=start_date,
        end_date=end_date,
        timezone_name=timezone_name,
        start_time=start_time,
        end_time_exclusive=end_time_exclusive,
        record_uid=record_uid,
        columns=columns,
        bucket=bucket,
        buckets=buckets,
    )

# ============================================================
# Catalog pruning
# ============================================================


def iter_dates(
    start_date: date,
    end_date: date,
) -> Iterable[date]:

    current = start_date

    while current <= end_date:

        yield current

        current += timedelta(
            days=1
        )

def catalog_partition_date_range(
    query: NormalizedQuery,
) -> tuple[
    date,
    date,
] | None:
    """
    根据最终 UTC event_time 范围，
    推导需要扫描的 UTC partition_date。

    Parquet partition_date 属于物理存储层，
    必须按实际 UTC event_time 范围剪枝，
    不能直接使用用户输入的本地自然日。

    end_time_exclusive 是开区间，
    所以计算最后分区时减 1 微秒。
    """

    if (
        query.start_time is None
        or
        query.end_time_exclusive is None
    ):

        return None

    first_date = (
        query.start_time.date()
    )

    last_included_time = (
        query.end_time_exclusive
        -
        timedelta(
            microseconds=1
        )
    )

    last_date = (
        last_included_time.date()
    )

    return (
        first_date,
        last_date,
    )

def _deduplicate_catalog_files(
    files: Iterable[CatalogDataFile],
) -> list[CatalogDataFile]:
    """
    Catalog 多 bucket 查询可能返回重复对象时做防御性去重。
    """

    result: list[CatalogDataFile] = []

    seen: set[
        tuple[
            int,
            str,
        ]
    ] = set()

    for item in files:

        key = (
            int(
                item.id
            ),
            str(
                item.file_path
            ),
        )

        if key in seen:

            continue

        seen.add(
            key
        )

        result.append(
            item
        )

    return result


def list_candidate_files(
    *,
    catalog: PostgresCatalog,
    query: NormalizedQuery,
) -> list[CatalogDataFile]:
    """
    PostgreSQL Catalog 第一层剪枝。

    剪枝维度：

        1. UTC partition_date range
        2. instrument bucket / buckets

    查询策略：

    无 instrument：

        不加 bucket 条件。

    单 instrument：

        使用原有单 bucket Catalog API。

    多 instrument：

        所有 instrument 先转换成稳定 bucket，
        bucket 去重。

        然后使用 multi-bucket Catalog API：

            bucket = ANY(...)

        因此：

            3 buckets -> 1 SQL
            100 buckets -> 1 SQL

        不再逐 bucket 查询 PostgreSQL。

    Arrow 层仍会继续使用：

        instrument_id IN (...)

    做精确记录过滤。

    也就是说：

        Catalog bucket
            =
        粗粒度文件剪枝

        Arrow instrument_id IN (...)
            =
        精确行级过滤
    """

    partition_range = (
        catalog_partition_date_range(
            query
        )
    )

    # ========================================================
    # 有完整日期范围
    # ========================================================

    if partition_range is not None:

        (
            first_partition_date,
            last_partition_date,
        ) = partition_range

        start_partition_date = (
            first_partition_date
            .isoformat()
        )

        end_partition_date = (
            last_partition_date
            .isoformat()
        )

        # ----------------------------------------------------
        # 没有指定 instrument：
        #
        # 查询该日期范围全部 bucket。
        # ----------------------------------------------------

        if not query.buckets:

            return (
                catalog.list_active_data_files_range(
                    site_id=query.site_id,
                    dataset=query.dataset,
                    country=query.country,
                    start_partition_date=(
                        start_partition_date
                    ),
                    end_partition_date=(
                        end_partition_date
                    ),
                    bucket=None,
                )
            )

        # ----------------------------------------------------
        # 单 bucket：
        #
        # 保留原 API，
        # 保持单股票查询完全向后兼容。
        # ----------------------------------------------------

        if len(
            query.buckets
        ) == 1:

            return (
                catalog.list_active_data_files_range(
                    site_id=query.site_id,
                    dataset=query.dataset,
                    country=query.country,
                    start_partition_date=(
                        start_partition_date
                    ),
                    end_partition_date=(
                        end_partition_date
                    ),
                    bucket=(
                        query.buckets[0]
                    ),
                )
            )

        # ----------------------------------------------------
        # 多 bucket：
        #
        # 一次 SQL：
        #
        # bucket = ANY(...)
        # ----------------------------------------------------

        return (
            catalog
            .list_active_data_files_range_multi_bucket(
                site_id=query.site_id,
                dataset=query.dataset,
                country=query.country,
                start_partition_date=(
                    start_partition_date
                ),
                end_partition_date=(
                    end_partition_date
                ),
                buckets=(
                    query.buckets
                ),
            )
        )

    # ========================================================
    # 没有完整日期范围
    # ========================================================

    # --------------------------------------------------------
    # 无 instrument：
    #
    # 查询所有 active uploaded 文件。
    # --------------------------------------------------------

    if not query.buckets:

        return (
            catalog.list_active_data_files(
                site_id=query.site_id,
                dataset=query.dataset,
                country=query.country,
                bucket=None,
            )
        )

    # --------------------------------------------------------
    # 单 bucket
    # --------------------------------------------------------

    if len(
        query.buckets
    ) == 1:

        return (
            catalog.list_active_data_files(
                site_id=query.site_id,
                dataset=query.dataset,
                country=query.country,
                bucket=(
                    query.buckets[0]
                ),
            )
        )

    # --------------------------------------------------------
    # 多 bucket
    # --------------------------------------------------------

    return (
        catalog
        .list_active_data_files_multi_bucket(
            site_id=query.site_id,
            dataset=query.dataset,
            country=query.country,
            buckets=(
                query.buckets
            ),
        )
    )

    
# ============================================================
# Parquet predicate
# ============================================================

def build_dataset_filter(
    query: NormalizedQuery,
):
    """
    构造 pyarrow.dataset Scanner 使用的过滤表达式。

    与 build_parquet_filters() 保持相同查询语义：

        instrument_id
        record_uid
        event_time >= start_time
        event_time < end_time_exclusive

    返回：

        None
        或 pyarrow.compute.Expression
    """

    expression = None

    def add(
        condition,
    ) -> None:

        nonlocal expression

        if expression is None:

            expression = condition

        else:

            expression = (
                expression
                &
                condition
            )

    if query.instrument_ids:

        if len(
            query.instrument_ids
        ) == 1:

            add(
                ds.field(
                    "instrument_id"
                )
                ==
                query.instrument_ids[0]
            )

        else:

            add(
                ds.field(
                    "instrument_id"
                )
                .isin(
                    list(
                        query.instrument_ids
                    )
                )
            )

    if query.record_uid is not None:

        add(
            ds.field(
                "record_uid"
            )
            ==
            query.record_uid
        )

    if query.start_time is not None:

        add(
            ds.field(
                "event_time"
            )
            >=
            query.start_time
        )

    if (
        query.end_time_exclusive
        is not None
    ):

        add(
            ds.field(
                "event_time"
            )
            <
            query.end_time_exclusive
        )

    return expression

def build_parquet_filters(
    query: NormalizedQuery,
):
    """
    构造 PyArrow Parquet filters。

    同一 inner list 内是 AND。

    返回形式：

        [
            [
                ("instrument_id", "=", "..."),
                ("event_time", ">=", ...),
                ...
            ]
        ]

    pyarrow 会尽可能利用 row-group statistics
    做 predicate pushdown。
    """

    filters: list[
        tuple[
            str,
            str,
            object,
        ]
    ] = []

    if query.instrument_ids:

        if len(
            query.instrument_ids
        ) == 1:

            filters.append(
                (
                    "instrument_id",
                    "=",
                    query.instrument_ids[0],
                )
            )

        else:

            filters.append(
                (
                    "instrument_id",
                    "in",
                    list(
                        query.instrument_ids
                    ),
                )
            )

    if query.record_uid is not None:

        filters.append(
            (
                "record_uid",
                "=",
                query.record_uid,
            )
        )

    if query.start_time is not None:

        filters.append(
            (
                "event_time",
                ">=",
                query.start_time,
            )
        )

    if (
        query.end_time_exclusive
        is not None
    ):

        filters.append(
            (
                "event_time",
                "<",
                query.end_time_exclusive,
            )
        )

    if not filters:

        return None

    return [
        filters
    ]


# ============================================================
# Remote file resolver
# ============================================================


class CatalogFileResolver:
    """
    把 CatalogDataFile 转换成本机可读 Path。

    支持两种情况：

    1. remote_path 本机本来就可见
       例如 Query 服务直接运行在 NAS 服务器上；

    2. Mac 等客户端机器看不到 NAS 路径
       则通过 scp 临时拉取。

    临时文件生命周期只存在于一次 query() 中，
    不长期占用本地磁盘。
    """

    def __init__(
        self,
        *,
        remote_host: str | None = None,
        remote_user: str | None = None,
        ssh_port: int = 22,
    ) -> None:

        self.remote_host = (
            normalize_optional_text(
                remote_host
            )
        )

        self.remote_user = (
            normalize_optional_text(
                remote_user
            )
        )

        self.ssh_port = int(
            ssh_port
        )


    def _remote_spec(
        self,
        remote_path: str,
    ) -> str:

        if not self.remote_host:

            raise RuntimeError(
                "remote_host is required "
                "because remote_path is not "
                "locally accessible"
            )

        host = self.remote_host

        if self.remote_user:

            target = (
                f"{self.remote_user}@{host}"
            )

        else:

            target = host

        return (
            f"{target}:{remote_path}"
        )


    def materialize(
        self,
        *,
        item: CatalogDataFile,
        temp_dir: Path,
    ) -> Path:
        """
        返回本机可读的 Parquet 路径。
        """

        if not item.remote_path:

            raise RuntimeError(
                "active uploaded catalog file "
                "has empty remote_path: "
                f"id={item.id}, "
                f"file_path={item.file_path}"
            )

        remote_path = str(
            item.remote_path
        ).strip()

        direct = Path(
            remote_path
        )

        # ----------------------------------------------------
        # Query 进程若直接运行在服务器/NAS 上，
        # 不进行任何复制。
        # ----------------------------------------------------

        if direct.is_file():

            return direct

        # ----------------------------------------------------
        # 否则通过 SCP 临时获取。
        # ----------------------------------------------------

        local_path = (
            temp_dir
            /
            (
                f"catalog-{item.id}-"
                f"{Path(item.file_path).name}"
            )
        )

        subprocess.run(
            [
                "scp",
                "-q",
                "-P",
                str(
                    self.ssh_port
                ),
                self._remote_spec(
                    remote_path
                ),
                str(
                    local_path
                ),
            ],
            check=True,
        )

        if not local_path.is_file():

            raise RuntimeError(
                "scp completed but local "
                "Parquet file is missing: "
                f"id={item.id}"
            )

        actual_size = (
            local_path.stat().st_size
        )

        if (
            actual_size
            !=
            int(
                item.file_size
            )
        ):

            raise RuntimeError(
                "downloaded Parquet size "
                "does not match catalog: "
                f"id={item.id}, "
                f"expected={item.file_size}, "
                f"actual={actual_size}"
            )

        return local_path


# ============================================================
# Main query reader
# ============================================================


class CatalogParquetReader:
    """
    PostgreSQL Catalog + Parquet Query Layer。

    查询路径：

        QuerySpec
            ↓
        normalize
            ↓
        Catalog active-file pruning
            ↓
        remote file materialization
            ↓
        Parquet column projection
            ↓
        Parquet predicate pushdown
            ↓
        Arrow Table
    """

    def __init__(
        self,
        *,
        catalog: PostgresCatalog,
        resolver: CatalogFileResolver,
    ) -> None:

        self.catalog = catalog

        self.resolver = resolver


    def query(
        self,
        spec: QuerySpec,
    ) -> QueryResult:

        query = normalize_query(
            spec
        )

        candidates = (
            list_candidate_files(
                catalog=self.catalog,
                query=query,
            )
        )

        if not candidates:

            return QueryResult(
                table=pa.table(
                    {
                        name: pa.array(
                            []
                        )
                        for name in query.columns
                    }
                ),
                stats=QueryStats(
                    catalog_files=0,
                    parquet_files_read=0,
                    rows_read=0,
                    rows_returned=0,
                ),
            )

        filters = build_parquet_filters(
            query
        )

        tables: list[
            pa.Table
        ] = []

        rows_read = 0

        parquet_files_read = 0

        with tempfile.TemporaryDirectory(
            prefix="crawl_framework_query_"
        ) as temp_dir_text:

            temp_dir = Path(
                temp_dir_text
            )

            for item in candidates:

                local_path = (
                    self.resolver.materialize(
                        item=item,
                        temp_dir=temp_dir,
                    )
                )

                parquet_files_read += 1

                # Catalog row_count 是本文件物理行数。
                rows_read += int(
                    item.row_count
                )

                table = pq.read_table(
                    local_path,
                    columns=list(
                        query.columns
                    ),
                    filters=filters,
                )

                if table.num_rows < 1:

                    continue

                tables.append(
                    table
                )

        if not tables:

            # 用第一个文件 schema 构造空表会更严谨，
            # 但第一版保持实现简单。
            #
            # 无数据时返回 columns 对应的空 Arrow Table。
            return QueryResult(
                table=pa.table(
                    {
                        name: pa.array(
                            []
                        )
                        for name in query.columns
                    }
                ),
                stats=QueryStats(
                    catalog_files=len(
                        candidates
                    ),
                    parquet_files_read=(
                        parquet_files_read
                    ),
                    rows_read=rows_read,
                    rows_returned=0,
                ),
            )

        if len(
            tables
        ) == 1:

            result_table = (
                tables[0]
            )

        else:

            result_table = (
                pa.concat_tables(
                    tables,
                    promote_options="default",
                )
            )

        # ----------------------------------------------------
        # 为查询结果提供稳定顺序。
        #
        # Parquet 文件顺序不应该成为业务 API 契约。
        # ----------------------------------------------------

        if (
            "event_time"
            in
            result_table.column_names
            and
            result_table.num_rows > 1
        ):

            sort_keys = [
                (
                    "event_time",
                    "ascending",
                )
            ]

            if (
                "record_uid"
                in
                result_table.column_names
            ):

                sort_keys.append(
                    (
                        "record_uid",
                        "ascending",
                    )
                )

            result_table = (
                result_table.sort_by(
                    sort_keys
                )
            )

        return QueryResult(
            table=result_table,
            stats=QueryStats(
                catalog_files=len(
                    candidates
                ),
                parquet_files_read=(
                    parquet_files_read
                ),
                rows_read=rows_read,
                rows_returned=(
                    result_table.num_rows
                ),
            ),
        )

    def iter_batches(
        self,
        spec: QuerySpec,
        *,
        batch_size: int = 65_536,
        max_rows: int | None = None,
        stats: StreamingQueryStats | None = None,
    ):
        """
        流式读取符合 QuerySpec 的数据。

        query():
            返回完整 pa.Table，
            适合小规模交互查询。

        iter_batches():
            逐批返回 pa.RecordBatch，
            适合大规模数据处理。

        max_rows:
            可选的底层查询行数上限。

            None:
                不限制结果数量。

            N:
                最多 yield N 行，
                达到 N 行后立即停止，
                不再读取后续 Parquet 文件。

        注意：

            max_rows 与 CLI 的“只打印多少行”
            是不同概念。

            它是真正的查询执行上限，
            可以减少 NAS / SCP / Parquet I/O。

        保留：

            Catalog lifecycle pruning
            partition range pruning
            instrument bucket pruning
            Arrow Dataset predicate pushdown

        Streaming API 不保证跨文件全局排序。
        """

        if batch_size < 1:

            raise ValueError(
                "batch_size must be >= 1"
            )

        if (
            max_rows is not None
            and
            max_rows < 1
        ):

            raise ValueError(
                "max_rows must be >= 1"
            )

        query = normalize_query(
            spec
        )

        query_started_at = (
            perf_counter()
        )

        catalog_started_at = (
            perf_counter()
        )

        candidates = (
            list_candidate_files(
                catalog=self.catalog,
                query=query,
            )
        )

        if stats is not None:

            stats.catalog_sql_calls += 1

            stats.catalog_duration_seconds += (
                perf_counter()
                -
                catalog_started_at
            )

            stats.catalog_files = len(
                candidates
            )

            stats.candidate_physical_rows = sum(
                int(
                    item.row_count
                )
                for item in candidates
            )

        if not candidates:

            if stats is not None:

                stats.query_duration_seconds += (
                    perf_counter()
                    -
                    query_started_at
                )

            return

        dataset_filter = (
            build_dataset_filter(
                query
            )
        )

        rows_yielded = 0

        with tempfile.TemporaryDirectory(
            prefix="crawl_framework_query_stream_"
        ) as temp_dir_text:

            temp_dir = Path(
                temp_dir_text
            )

            for item in candidates:

                if (
                    max_rows is not None
                    and
                    rows_yielded >= max_rows
                ):

                    if stats is not None:

                        stats.query_duration_seconds += (
                            perf_counter()
                            -
                            query_started_at
                        )

                    return

                materialization_started_at = (
                    perf_counter()
                )

                local_path = (
                    self.resolver.materialize(
                        item=item,
                        temp_dir=temp_dir,
                    )
                )

                if stats is not None:

                    stats.parquet_files_materialized += 1

                    stats.materialization_duration_seconds += (
                        perf_counter()
                        -
                        materialization_started_at
                    )

                    try:

                        stats.bytes_materialized += (
                            local_path
                            .stat()
                            .st_size
                        )

                    except OSError:

                        pass

                dataset = ds.dataset(
                    str(
                        local_path
                    ),
                    format="parquet",
                )

                if stats is not None:

                    stats.parquet_files_read += 1

                scanner = dataset.scanner(
                    columns=list(
                        query.columns
                    ),
                    filter=dataset_filter,
                    batch_size=batch_size,
                    use_threads=True,
                )

                scan_started_at = (
                    perf_counter()
                )

                for batch in (
                    scanner.to_batches()
                ):

                    if batch.num_rows < 1:

                        continue

                    if max_rows is None:

                        rows_yielded += (
                            batch.num_rows
                        )

                        if stats is not None:

                            stats.rows_yielded += (
                                batch.num_rows
                            )

                            stats.batches_yielded += 1

                        yield batch

                        continue

                    remaining = (
                        max_rows
                        -
                        rows_yielded
                    )

                    if remaining < 1:

                        if stats is not None:

                            stats.scan_duration_seconds += (
                                perf_counter()
                                -
                                scan_started_at
                            )

                            stats.query_duration_seconds += (
                                perf_counter()
                                -
                                query_started_at
                            )

                        return

                    if (
                        batch.num_rows
                        <=
                        remaining
                    ):

                        rows_yielded += (
                            batch.num_rows
                        )

                        if stats is not None:

                            stats.rows_yielded += (
                                batch.num_rows
                            )

                            stats.batches_yielded += 1

                        yield batch

                    else:

                        limited_batch = (
                            batch.slice(
                                0,
                                remaining,
                            )
                        )

                        rows_yielded += (
                            limited_batch.num_rows
                        )

                        if stats is not None:

                            stats.rows_yielded += (
                                limited_batch.num_rows
                            )

                            stats.batches_yielded += 1

                        yield limited_batch

                    if (
                        rows_yielded
                        >=
                        max_rows
                    ):

                        if stats is not None:

                            stats.scan_duration_seconds += (
                                perf_counter()
                                -
                                scan_started_at
                            )

                            stats.query_duration_seconds += (
                                perf_counter()
                                -
                                query_started_at
                            )

                        return

                if stats is not None:

                    stats.scan_duration_seconds += (
                        perf_counter()
                        -
                        scan_started_at
                    )

        if stats is not None:

            stats.query_duration_seconds += (
                perf_counter()
                -
                query_started_at
            )
