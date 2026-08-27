from __future__ import annotations

import argparse
import json
import sys

from datetime import (
    date,
    datetime,
)
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


# ============================================================
# Project import
# ============================================================


PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[1]
)

SRC_DIR = (
    PROJECT_ROOT
    / "src"
)

if str(
    SRC_DIR
) not in sys.path:

    sys.path.insert(
        0,
        str(
            SRC_DIR
        ),
    )


from crawl_framework.config import (
    load_default_config,
)

from crawl_framework.storage.postgres import (
    PostgresCatalog,
)

from crawl_framework.storage.postgres_connection import (
    make_config_postgres_connection_factory,
)

from crawl_framework.storage.query import (
    CatalogFileResolver,
    CatalogParquetReader,
    QuerySpec,
)


# ============================================================
# CLI
# ============================================================


def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description=(
            "Query active Parquet data through "
            "PostgreSQL Catalog."
        )
    )

    parser.add_argument(
        "--site",
        required=True,
        help=(
            "Site ID, e.g. "
            "naver_finance"
        ),
    )

    parser.add_argument(
        "--dataset",
        required=True,
        help=(
            "Dataset, e.g. "
            "forum_post"
        ),
    )

    parser.add_argument(
        "--country",
        default=None,
        help=(
            "Country code, e.g. KR"
        ),
    )

    parser.add_argument(
        "--instrument",
        default=None,
        help=(
            "Single canonical instrument ID, "
            "e.g. XKRX:042700"
        ),
    )

    parser.add_argument(
        "--instruments",
        nargs="+",
        default=None,
        help=(
            "Multiple canonical instrument IDs. "
            "Example: --instruments "
            "XKRX:005930 XKRX:000660 XKRX:042700"
        ),
    )

    parser.add_argument(
        "--start-date",
        default=None,
        help=(
            "Inclusive start date "
            "YYYY-MM-DD"
        ),
    )

    parser.add_argument(
        "--end-date",
        default=None,
        help=(
            "Inclusive end date "
            "YYYY-MM-DD"
        ),
    )

    parser.add_argument(
        "--timezone",
        default="UTC",
        help=(
            "IANA timezone used to interpret "
            "start/end dates. "
            "Examples: UTC, Asia/Seoul, "
            "Asia/Tokyo, America/New_York. "
            "Default: UTC"
        ),
    )

    parser.add_argument(
        "--record-uid",
        default=None,
        help=(
            "Optional exact record_uid"
        ),
    )

    parser.add_argument(
        "--columns",
        nargs="+",
        default=None,
        help=(
            "Optional projected columns. "
            "Example: "
            "--columns record_uid "
            "event_time title content"
        ),
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help=(
            "Maximum rows printed. "
            "Query itself is not limited. "
            "Default: 20"
        ),
    )

    parser.add_argument(
        "--json",
        action="store_true",
        help=(
            "Output rows as JSON. "
            "With --stream, output is JSON Lines."
        ),
    )

    parser.add_argument(
        "--stream",
        action="store_true",
        help=(
            "Stream query results as "
            "Arrow RecordBatches instead "
            "of loading the full result "
            "table into memory."
        ),
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=65_536,
        help=(
            "Maximum Arrow streaming "
            "batch size. "
            "Used only with --stream. "
            "Default: 65536"
        ),
    )

    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help=(
            "Optional hard streaming query row limit. "
            "Unlike --limit, this stops execution "
            "after N returned rows and can avoid "
            "reading later Parquet files."
        ),
    )

    parser.add_argument(
        "--output",
        default=None,
        help=(
            "Optional streaming output file. "
            "Supported extensions: "
            ".jsonl and .parquet. "
            "Requires --stream."
        ),
    )

    return (
        parser.parse_args()
    )


# ============================================================
# Serialization
# ============================================================


def json_default(
    value,
):

    if isinstance(
        value,
        (
            datetime,
            date,
        ),
    ):

        return (
            value.isoformat()
        )

    return str(
        value
    )


# ============================================================
# Human output helpers
# ============================================================


def print_query_header(
    *,
    title: str,
    args: argparse.Namespace,
    include_stream_options: bool = False,
) -> None:

    print()
    print(
        "========================================"
    )
    print(
        title
    )
    print(
        "========================================"
    )

    print(
        "site =",
        args.site,
    )

    print(
        "dataset =",
        args.dataset,
    )

    print(
        "country =",
        args.country,
    )

    print(
        "instrument =",
        args.instrument,
    )

    print(
        "start_date =",
        args.start_date,
    )

    print(
        "end_date =",
        args.end_date,
    )

    print(
        "timezone =",
        args.timezone,
    )

    print(
        "record_uid =",
        args.record_uid,
    )

    if include_stream_options:

        print(
            "batch_size =",
            args.batch_size,
        )

        print(
            "max_rows =",
            args.max_rows,
        )


def print_stats(
    result,
) -> None:

    stats = result.stats

    print()
    print(
        "========================================"
    )
    print(
        "QUERY STATS"
    )
    print(
        "========================================"
    )

    print(
        "catalog_files =",
        stats.catalog_files,
    )

    print(
        "parquet_files_read =",
        stats.parquet_files_read,
    )

    print(
        "physical_rows_in_files =",
        stats.rows_read,
    )

    print(
        "rows_returned =",
        stats.rows_returned,
    )


def print_rows_human(
    rows: list[dict],
    *,
    limit: int,
) -> None:

    shown = rows[
        :limit
    ]

    print()
    print(
        "========================================"
    )
    print(
        "ROWS"
    )
    print(
        "========================================"
    )

    if not shown:

        print(
            "(no rows)"
        )

        return

    for index, row in enumerate(
        shown,
        start=1,
    ):

        print()
        print(
            f"----- row {index} -----"
        )

        for key, value in (
            row.items()
        ):

            print(
                f"{key} = {value!r}"
            )

    if (
        len(
            rows
        )
        >
        limit
    ):

        print()
        print(
            "... "
            f"{len(rows) - limit} "
            "more rows not printed"
        )


# ============================================================
# Streaming output helpers
# ============================================================


def print_stream_rows_human(
    *,
    reader,
    spec,
    batch_size: int,
    limit: int,
    max_rows: int | None,
) -> tuple[
    int,
    int,
]:
    """
    流式打印查询结果。

    limit 只限制显示数量。
    max_rows 才是真正的底层查询硬上限。
    """

    total_batches = 0
    total_rows = 0
    printed_rows = 0

    print()
    print(
        "========================================"
    )
    print(
        "STREAM ROWS"
    )
    print(
        "========================================"
    )

    for batch in reader.iter_batches(
        spec,
        batch_size=batch_size,
        max_rows=max_rows,
    ):

        total_batches += 1
        total_rows += (
            batch.num_rows
        )

        if (
            limit == 0
            or
            printed_rows >= limit
        ):

            continue

        remaining = (
            limit
            -
            printed_rows
        )

        rows = (
            batch
            .slice(
                0,
                min(
                    remaining,
                    batch.num_rows,
                ),
            )
            .to_pylist()
        )

        for row in rows:

            printed_rows += 1

            print()
            print(
                f"----- row "
                f"{printed_rows} -----"
            )

            for key, value in (
                row.items()
            ):

                print(
                    f"{key} = {value!r}"
                )

    if total_rows == 0:

        print(
            "(no rows)"
        )

    elif (
        total_rows
        >
        printed_rows
    ):

        print()
        print(
            "... "
            f"{total_rows - printed_rows} "
            "more rows not printed"
        )

    return (
        total_batches,
        total_rows,
    )


def print_stream_rows_json(
    *,
    reader,
    spec,
    batch_size: int,
    limit: int,
    max_rows: int | None,
) -> tuple[
    int,
    int,
]:
    """
    Streaming JSON Lines 输出。

    每行一个 JSON object，不构造完整 JSON array。
    """

    total_batches = 0
    total_rows = 0
    printed_rows = 0

    for batch in reader.iter_batches(
        spec,
        batch_size=batch_size,
        max_rows=max_rows,
    ):

        total_batches += 1
        total_rows += (
            batch.num_rows
        )

        if (
            limit == 0
            or
            printed_rows >= limit
        ):

            continue

        remaining = (
            limit
            -
            printed_rows
        )

        rows = (
            batch
            .slice(
                0,
                min(
                    remaining,
                    batch.num_rows,
                ),
            )
            .to_pylist()
        )

        for row in rows:

            print(
                json.dumps(
                    row,
                    ensure_ascii=False,
                    default=json_default,
                )
            )

            printed_rows += 1

    return (
        total_batches,
        total_rows,
    )


def export_stream_jsonl(
    *,
    reader,
    spec,
    output_path: Path,
    batch_size: int,
    max_rows: int | None,
) -> tuple[
    int,
    int,
]:
    """
    流式导出 JSON Lines。
    """

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    total_batches = 0
    total_rows = 0

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as handle:

        for batch in reader.iter_batches(
            spec,
            batch_size=batch_size,
            max_rows=max_rows,
        ):

            total_batches += 1
            total_rows += (
                batch.num_rows
            )

            for row in batch.to_pylist():

                handle.write(
                    json.dumps(
                        row,
                        ensure_ascii=False,
                        default=json_default,
                    )
                )

                handle.write(
                    "\n"
                )

    return (
        total_batches,
        total_rows,
    )


def export_stream_parquet(
    *,
    reader,
    spec,
    output_path: Path,
    batch_size: int,
    max_rows: int | None,
) -> tuple[
    int,
    int,
]:
    """
    流式导出为单个 Parquet 文件。

    使用 ParquetWriter 分批写入，不构造完整 pa.Table。
    """

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    total_batches = 0
    total_rows = 0

    writer: pq.ParquetWriter | None = None

    try:

        for batch in reader.iter_batches(
            spec,
            batch_size=batch_size,
            max_rows=max_rows,
        ):

            if batch.num_rows < 1:

                continue

            total_batches += 1
            total_rows += (
                batch.num_rows
            )

            table = pa.Table.from_batches(
                [
                    batch
                ]
            )

            if writer is None:

                writer = pq.ParquetWriter(
                    output_path,
                    table.schema,
                    compression="snappy",
                )

            writer.write_table(
                table
            )

    finally:

        if writer is not None:

            writer.close()

    return (
        total_batches,
        total_rows,
    )


def export_stream(
    *,
    reader,
    spec,
    output_path: Path,
    batch_size: int,
    max_rows: int | None,
) -> tuple[
    int,
    int,
]:

    suffix = (
        output_path.suffix
        .lower()
    )

    if suffix == ".jsonl":

        return export_stream_jsonl(
            reader=reader,
            spec=spec,
            output_path=output_path,
            batch_size=batch_size,
            max_rows=max_rows,
        )

    if suffix == ".parquet":

        return export_stream_parquet(
            reader=reader,
            spec=spec,
            output_path=output_path,
            batch_size=batch_size,
            max_rows=max_rows,
        )

    raise ValueError(
        "--output must end with "
        ".jsonl or .parquet"
    )


# ============================================================
# Main
# ============================================================


def main() -> int:

    args = parse_args()

    if (
        args.limit
        <
        0
    ):

        raise ValueError(
            "--limit must be >= 0"
        )

    if (
        args.batch_size
        <
        1
    ):

        raise ValueError(
            "--batch-size must be >= 1"
        )

    if (
        args.max_rows is not None
        and
        args.max_rows < 1
    ):

        raise ValueError(
            "--max-rows must be >= 1"
        )

    if (
        args.output
        and
        not args.stream
    ):

        raise ValueError(
            "--output currently requires "
            "--stream"
        )

    config = (
        load_default_config()
    )

    connection_factory = (
        make_config_postgres_connection_factory(
            config
        )
    )

    connection = (
        connection_factory()
    )

    try:

        catalog = PostgresCatalog(
            connection
        )

        resolver = CatalogFileResolver(
            remote_host=str(
                config.server.host
            ),
            remote_user=str(
                config.server.user
            ),
            ssh_port=int(
                config.sync.ssh_port
            ),
        )

        reader = CatalogParquetReader(
            catalog=catalog,
            resolver=resolver,
        )

        columns = (
            tuple(
                args.columns
            )
            if args.columns
            else None
        )

        spec = QuerySpec(
            site_id=args.site,
            dataset=args.dataset,
            country=args.country,
            instrument_id=(
                args.instrument
            ),
            instrument_ids=(
                tuple(
                    args.instruments
                )
                if args.instruments
                else None
            ),
            start_date=(
                args.start_date
            ),
            end_date=(
                args.end_date
            ),
            record_uid=(
                args.record_uid
            ),
            columns=columns,
            timezone=(
                args.timezone
            ),
        )

        # ----------------------------------------------------
        # Streaming path
        # ----------------------------------------------------

        if args.stream:

            if args.output:

                output_path = Path(
                    args.output
                ).expanduser()

                (
                    total_batches,
                    total_rows,
                ) = export_stream(
                    reader=reader,
                    spec=spec,
                    output_path=output_path,
                    batch_size=(
                        args.batch_size
                    ),
                    max_rows=(
                        args.max_rows
                    ),
                )

                print(
                    json.dumps(
                        {
                            "output":
                                str(
                                    output_path
                                ),

                            "total_batches":
                                total_batches,

                            "rows_returned":
                                total_rows,

                            "batch_size":
                                args.batch_size,

                            "max_rows":
                                args.max_rows,
                        },
                        ensure_ascii=False,
                    )
                )

                return 0

            if args.json:

                (
                    total_batches,
                    total_rows,
                ) = print_stream_rows_json(
                    reader=reader,
                    spec=spec,
                    batch_size=(
                        args.batch_size
                    ),
                    limit=args.limit,
                    max_rows=(
                        args.max_rows
                    ),
                )

                print(
                    json.dumps(
                        {
                            "_stream_stats": {
                                "total_batches":
                                    total_batches,

                                "rows_returned":
                                    total_rows,

                                "batch_size":
                                    args.batch_size,

                                "max_rows":
                                    args.max_rows,
                            }
                        },
                        ensure_ascii=False,
                    )
                )

                return 0

            print_query_header(
                title=(
                    "CATALOG PARQUET STREAM QUERY"
                ),
                args=args,
                include_stream_options=True,
            )

            (
                total_batches,
                total_rows,
            ) = print_stream_rows_human(
                reader=reader,
                spec=spec,
                batch_size=(
                    args.batch_size
                ),
                limit=args.limit,
                max_rows=(
                    args.max_rows
                ),
            )

            print()
            print(
                "========================================"
            )
            print(
                "STREAM STATS"
            )
            print(
                "========================================"
            )

            print(
                "total_batches =",
                total_batches,
            )

            print(
                "rows_returned =",
                total_rows,
            )

            print(
                "max_rows =",
                args.max_rows,
            )

            return 0

        # ----------------------------------------------------
        # Non-streaming path
        # ----------------------------------------------------

        result = reader.query(
            spec
        )

        rows = (
            result.table
            .to_pylist()
        )

        if args.json:

            output = {
                "query": {
                    "site_id":
                        args.site,

                    "dataset":
                        args.dataset,

                    "country":
                        args.country,

                    "instrument_id":
                        args.instrument,

                    "instrument_ids":
                        args.instruments,

                    "start_date":
                        args.start_date,

                    "end_date":
                        args.end_date,

                    "timezone":
                        args.timezone,

                    "record_uid":
                        args.record_uid,

                    "columns":
                        args.columns,
                },

                "stats": {
                    "catalog_files":
                        result.stats.catalog_files,

                    "parquet_files_read":
                        result.stats.parquet_files_read,

                    "physical_rows_in_files":
                        result.stats.rows_read,

                    "rows_returned":
                        result.stats.rows_returned,
                },

                "rows": (
                    rows[
                        :args.limit
                    ]
                    if args.limit > 0
                    else []
                ),
            }

            print(
                json.dumps(
                    output,
                    ensure_ascii=False,
                    indent=2,
                    default=json_default,
                )
            )

            return 0

        print_query_header(
            title=(
                "CATALOG PARQUET QUERY"
            ),
            args=args,
            include_stream_options=False,
        )

        print_stats(
            result
        )

        print_rows_human(
            rows,
            limit=args.limit,
        )

        return 0

    finally:

        connection.close()


if __name__ == "__main__":

    raise SystemExit(
        main()
    )
