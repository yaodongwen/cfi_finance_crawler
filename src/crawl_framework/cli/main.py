from __future__ import annotations

import argparse
import asyncio
import json
import sys

from dataclasses import (
    asdict,
    dataclass,
)
from pathlib import Path
from typing import (
    Any,
    Callable,
    Iterable,
    Protocol,
    Sequence,
)

from crawl_framework.core.bootstrap import (
    BootstrapResult,
    CrawlBootstrap,
)


# ============================================================
# Exit codes
# ============================================================


EXIT_SUCCESS = 0

EXIT_STARTUP_BLOCKED = 2

EXIT_RUNTIME_ERROR = 3

EXIT_CONFIGURATION_ERROR = 4


# ============================================================
# CLI errors
# ============================================================


class CLIError(
    RuntimeError
):
    """
    CLI 层基础异常。
    """


class BootstrapFactoryNotConfiguredError(
    CLIError
):
    """
    CLI 尚未配置真正的 Bootstrap factory。

    当前阶段 CLI 与组件组装刻意分离。

    下一步 app_factory.py 会负责构建：

        SeenStore
        CheckpointStore
        ParquetWriter
        Uploader
        PostgresCatalog
        RecoveryManager
        RecoveryOrchestrator
        StoragePipeline
        SitePlugin
        CrawlRuntime
        CrawlBootstrap
    """


# ============================================================
# Parsed options
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class CLIOptions:
    """
    CLI 参数标准化结果。

    site:
        网站插件 ID。

    datasets:
        用户指定的数据集。

        None:
            使用插件支持的全部 dataset。

    flush_at_end:
        Runtime 结束时是否执行最终 flush。

    json_output:
        是否输出 JSON。

    recovery_only:
        是否只执行 startup recovery。

    instruments:
        用户显式指定的站点 instrument code。

        例如：

            005930
            000660
            042700

        None:
            没有显式限定 instrument。

    max_pages:
        本次运行最多抓取多少页。

        当前首先由 Naver forum_post 使用。

        None:
            不通过 CLI 覆盖插件默认值。
    """

    site: str

    datasets: tuple[
        str,
        ...,
    ] | None

    flush_at_end: bool

    json_output: bool

    recovery_only: bool

    instruments: tuple[
        str,
        ...,
    ] | None = None

    instruments_file: Path | None = None

    max_pages: int | None = None

    crawl_workers: int | None = None

    attachment_workers: int | None = None

    writer_workers: int | None = None

    upload_workers: int | None = None

    catalog_workers: int | None = None

    http_concurrency: int | None = None

    target_file_size_mb: int | None = None

    instrument_limit: int | None = None

# ============================================================
# Factory protocol
# ============================================================


class BootstrapFactory(
    Protocol
):
    """
    Composition root 的抽象接口。

    CLI 不需要知道 CrawlBootstrap
    内部具体如何构造。
    """

    def __call__(
        self,
        options: CLIOptions,
    ) -> CrawlBootstrap:
        ...


# ============================================================
# CLI execution result
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class CLIResult:
    """
    CLI 一次执行结果。
    """

    exit_code: int

    message: str

    bootstrap_result: (
        BootstrapResult
        | None
    ) = None


# ============================================================
# Parser
# ============================================================


def build_parser() -> argparse.ArgumentParser:
    """
    创建 crawler CLI parser。
    """

    parser = argparse.ArgumentParser(
        prog="crawl-framework",
        description=(
            "Universal financial web crawler "
            "framework"
        ),
    )

    # ========================================================
    # Site
    # ========================================================

    parser.add_argument(
        "--site",
        required=True,
        help=(
            "Site plugin id, for example "
            "naver_finance or tossinvest."
        ),
    )

    # ========================================================
    # Dataset
    # ========================================================

    parser.add_argument(
        "--dataset",
        dest="datasets",
        action="append",
        default=None,
        help=(
            "Dataset to crawl. "
            "Can be supplied multiple times. "
            "If omitted, all datasets supported "
            "by the plugin are used."
        ),
    )

    # ========================================================
    # Instrument
    # ========================================================

    parser.add_argument(
        "--instrument",
        dest="instruments",
        action="append",
        default=None,
        help=(
            "Site-specific instrument code to crawl. "
            "Can be supplied multiple times. "
            "Example for Naver Finance: "
            "--instrument 005930 "
            "--instrument 000660."
        ),
    )

    parser.add_argument(
        "--instruments-file",
        dest="instruments_file",
        default=None,
        help=(
            "UTF-8 file containing one site-specific "
            "instrument code per line. Blank lines are "
            "ignored. Values are merged after repeated "
            "--instrument values."
        ),
    )

    parser.add_argument(
        "--instrument-limit",
        dest="instrument_limit",
        type=int,
        default=None,
        help=(
            "Use only the first N effective instruments "
            "after merging --instrument and --instruments-file."
        ),
    )

    # ========================================================
    # Max pages
    # ========================================================

    parser.add_argument(
        "--max-pages",
        dest="max_pages",
        type=int,
        default=None,
        help=(
            "Maximum number of pages to crawl "
            "for each scope during this run. "
            "Must be a positive integer."
        ),
    )

    for option_name in (
        "crawl-workers",
        "attachment-workers",
        "writer-workers",
        "upload-workers",
        "catalog-workers",
        "http-concurrency",
        "target-file-size-mb",
    ):

        parser.add_argument(
            f"--{option_name}",
            dest=option_name.replace(
                "-",
                "_",
            ),
            type=int,
            default=None,
            help=(
                "Positive integer runtime override."
            ),
        )

    # ========================================================
    # Flush
    # ========================================================

    parser.add_argument(
        "--no-final-flush",
        action="store_true",
        help=(
            "Do not flush all remaining "
            "buffered records at runtime exit."
        ),
    )

    # ========================================================
    # Output
    # ========================================================

    parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help=(
            "Print machine-readable JSON output."
        ),
    )

    # ========================================================
    # Recovery
    # ========================================================

    parser.add_argument(
        "--recovery-only",
        action="store_true",
        help=(
            "Run startup recovery only and "
            "do not start the crawler."
        ),
    )

    return parser

# ============================================================
# Parse
# ============================================================


def parse_args(
    argv: Sequence[
        str
    ] | None = None,
) -> CLIOptions:
    """
    解析并标准化 CLI 参数。

    当前统一处理：

        site
        dataset
        instrument
        max_pages
        final flush
        json
        recovery only

    dataset / instrument：

        - 去除首尾空格
        - 去掉空字符串
        - 去重
        - 保留输入顺序
    """

    if argv is None:

        argv = sys.argv[
            1:
        ]

    if (
        argv
        and len(
            argv
        )
        >= 1
        and argv[0] == "crawl"
    ):

        argv = argv[
            1:
        ]

    parser = build_parser()

    args = parser.parse_args(
        argv
    )

    # ========================================================
    # Site
    # ========================================================

    site = str(
        args.site
    ).strip()

    if not site:

        parser.error(
            "--site cannot be empty"
        )

    # ========================================================
    # Helper:
    # normalize repeated string arguments
    # ========================================================

    def normalize_repeated(
        values,
    ) -> tuple[
        str,
        ...
    ] | None:

        if not values:

            return None

        result: list[
            str
        ] = []

        seen: set[
            str
        ] = set()

        for value in values:

            normalized = str(
                value
            ).strip()

            if not normalized:

                continue

            if normalized in seen:

                continue

            seen.add(
                normalized
            )

            result.append(
                normalized
            )

        if not result:

            return None

        return tuple(
            result
        )

    def read_instruments_file(
        path_value,
    ) -> tuple[
        str,
        ...
    ] | None:

        if path_value is None:

            return None

        path = Path(
            str(
                path_value
            ).strip()
        ).expanduser()

        if not str(
            path
        ):

            parser.error(
                "--instruments-file cannot be empty"
            )

        try:

            text = path.read_text(
                encoding="utf-8"
            )

        except OSError as exc:

            parser.error(
                f"--instruments-file cannot be read: {exc}"
            )

        values = [
            line
            for line in text.splitlines()
            if not line.strip().startswith(
                "#"
            )
        ]

        normalized = normalize_repeated(
            values
        )

        if normalized is None:

            parser.error(
                "--instruments-file did not contain "
                "any instruments"
            )

        return normalized

    # ========================================================
    # Datasets
    # ========================================================

    datasets = (
        normalize_repeated(
            args.datasets
        )
    )

    # ========================================================
    # Instruments
    # ========================================================

    instruments = (
        normalize_repeated(
            args.instruments
        )
    )

    file_instruments = read_instruments_file(
        args.instruments_file
    )

    instruments = normalize_repeated(
        (
            tuple(
                instruments
                or ()
            )
            +
            tuple(
                file_instruments
                or ()
            )
        )
    )

    instrument_limit = args.instrument_limit

    if instrument_limit is not None:

        instrument_limit = int(
            instrument_limit
        )

        if instrument_limit <= 0:

            parser.error(
                "--instrument-limit must be positive"
            )

        if instruments is not None:

            instruments = instruments[
                :instrument_limit
            ]

    # ========================================================
    # Max pages
    # ========================================================

    max_pages = (
        args.max_pages
    )

    if max_pages is not None:

        try:

            max_pages = int(
                max_pages
            )

        except (
            TypeError,
            ValueError,
        ):

            parser.error(
                "--max-pages must be "
                "an integer"
            )

        if max_pages <= 0:

            parser.error(
                "--max-pages must be "
                "positive"
            )

    def positive_optional_int(
        name,
    ) -> int | None:

        value = getattr(
            args,
            name,
        )

        if value is None:

            return None

        value = int(
            value
        )

        if value <= 0:

            parser.error(
                f"--{name.replace('_', '-')} "
                "must be positive"
            )

        return value

    # ========================================================
    # Return
    # ========================================================

    return CLIOptions(
        site=site,
        datasets=datasets,
        flush_at_end=(
            not args.no_final_flush
        ),
        json_output=bool(
            args.json_output
        ),
        recovery_only=bool(
            args.recovery_only
        ),
        instruments=instruments,
        instruments_file=(
            Path(
                args.instruments_file
            ).expanduser()
            if args.instruments_file
            else None
        ),
        max_pages=max_pages,
        crawl_workers=positive_optional_int(
            "crawl_workers"
        ),
        attachment_workers=positive_optional_int(
            "attachment_workers"
        ),
        writer_workers=positive_optional_int(
            "writer_workers"
        ),
        upload_workers=positive_optional_int(
            "upload_workers"
        ),
        catalog_workers=positive_optional_int(
            "catalog_workers"
        ),
        http_concurrency=positive_optional_int(
            "http_concurrency"
        ),
        target_file_size_mb=positive_optional_int(
            "target_file_size_mb"
        ),
        instrument_limit=instrument_limit,
    )
    

# ============================================================
# Output helpers
# ============================================================


def bootstrap_result_to_dict(
    result: BootstrapResult,
) -> dict[
    str,
    Any,
]:
    """
    BootstrapResult 转换为可 JSON 序列化结构。

    Runtime 返回值可能是 dataclass、
    list、dict 或其他结构，
    因此通过 _json_safe() 统一处理。
    """

    return {
        "success": (
            result.success
        ),
        "crawler_started": (
            result.crawler_started
        ),
        "message": (
            result.message
        ),
        "recovery": {
            "scanned": (
                result.recovery.scanned
            ),
            "attempted": (
                result.recovery.attempted
            ),
            "recovered": (
                result.recovery.recovered
            ),
            "skipped": (
                result.recovery.skipped
            ),
            "failed": (
                result.recovery.failed
            ),
            "terminal_failed": (
                result.recovery
                .terminal_failed
            ),
            "remaining_pending": (
                result.recovery
                .remaining_pending
            ),
            "can_continue": (
                result.recovery
                .can_continue
            ),
        },
        "runtime": _json_safe(
            result.runtime
        ),
    }


def _json_safe(
    value: Any,
) -> Any:
    """
    尽量把对象变成 JSON-safe structure。
    """

    if value is None:

        return None

    if isinstance(
        value,
        (
            str,
            int,
            float,
            bool,
        ),
    ):

        return value

    if isinstance(
        value,
        dict,
    ):

        return {
            str(key): _json_safe(
                item
            )
            for key, item
            in value.items()
        }

    if isinstance(
        value,
        (
            list,
            tuple,
            set,
            frozenset,
        ),
    ):

        return [
            _json_safe(
                item
            )
            for item
            in value
        ]

    if hasattr(
        value,
        "__dataclass_fields__",
    ):

        return _json_safe(
            asdict(
                value
            )
        )

    return str(
        value
    )


def format_text_result(
    result: BootstrapResult,
) -> str:
    """
    人类可读 CLI 输出。
    """

    recovery = (
        result.recovery
    )

    lines = [
        (
            "startup recovery: "
            f"scanned={recovery.scanned}, "
            f"attempted={recovery.attempted}, "
            f"recovered={recovery.recovered}, "
            f"skipped={recovery.skipped}, "
            f"failed={recovery.failed}, "
            f"terminal_failed="
            f"{recovery.terminal_failed}, "
            f"remaining="
            f"{recovery.remaining_pending}"
        ),
        (
            "crawler_started="
            f"{result.crawler_started}"
        ),
        (
            "success="
            f"{result.success}"
        ),
    ]

    if result.message:

        lines.append(
            f"message={result.message}"
        )

    return "\n".join(
        lines
    )


def print_bootstrap_result(
    result: BootstrapResult,
    *,
    json_output: bool,
    stream=None,
) -> None:
    """
    输出 BootstrapResult。
    """

    if stream is None:

        stream = sys.stdout

    if json_output:

        payload = (
            bootstrap_result_to_dict(
                result
            )
        )

        print(
            json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=stream,
        )

        return

    print(
        format_text_result(
            result
        ),
        file=stream,
    )


# ============================================================
# Recovery-only
# ============================================================


def run_recovery_only(
    bootstrap: CrawlBootstrap,
) -> BootstrapResult:
    """
    只运行 startup recovery。

    不调用 CrawlRuntime。

    为了保持 CLI 返回结构统一，
    仍然返回 BootstrapResult。
    """

    recovery_result = (
        bootstrap
        .recovery_orchestrator
        .run()
    )

    return BootstrapResult(
        recovery=recovery_result,
        runtime=None,
        crawler_started=False,
        success=(
            recovery_result
            .can_continue
        ),
        message=(
            "startup recovery completed"
            if recovery_result.can_continue
            else
            "startup recovery blocked"
        ),
    )


# ============================================================
# Async CLI
# ============================================================


async def async_main(
    argv: Sequence[
        str
    ] | None = None,
    *,
    bootstrap_factory: (
        BootstrapFactory
        | None
    ) = None,
    stdout=None,
    stderr=None,
) -> CLIResult:
    """
    CLI 的异步执行入口。

    bootstrap_factory 使用依赖注入，
    方便：

        unit test
        development
        production
        不同 site plugin

    下一步 app_factory.py 会成为默认 factory。
    """

    if stdout is None:

        stdout = sys.stdout

    if stderr is None:

        stderr = sys.stderr

    options = parse_args(
        argv
    )

    if bootstrap_factory is None:

        raise (
            BootstrapFactoryNotConfiguredError(
                "bootstrap factory is not "
                "configured yet"
            )
        )

    try:

        bootstrap = (
            bootstrap_factory(
                options
            )
        )

    except Exception as exc:

        message = (
            "bootstrap configuration failed: "
            f"{exc}"
        )

        print(
            message,
            file=stderr,
        )

        return CLIResult(
            exit_code=(
                EXIT_CONFIGURATION_ERROR
            ),
            message=message,
        )

    try:

        # ====================================================
        # Recovery only
        # ====================================================

        if options.recovery_only:

            result = (
                run_recovery_only(
                    bootstrap
                )
            )

        # ====================================================
        # Recovery + crawl
        # ====================================================

        else:

            result = (
                await bootstrap.run(
                    datasets=(
                        options.datasets
                    ),
                    flush_at_end=(
                        options.flush_at_end
                    ),
                )
            )

    except Exception as exc:

        message = (
            "crawler runtime failed: "
            f"{exc}"
        )

        print(
            message,
            file=stderr,
        )

        return CLIResult(
            exit_code=(
                EXIT_RUNTIME_ERROR
            ),
            message=message,
        )

    print_bootstrap_result(
        result,
        json_output=(
            options.json_output
        ),
        stream=stdout,
    )

    # ========================================================
    # Exit code
    # ========================================================

    if not result.success:

        return CLIResult(
            exit_code=(
                EXIT_STARTUP_BLOCKED
            ),
            message=(
                result.message
                or "startup blocked"
            ),
            bootstrap_result=result,
        )

    return CLIResult(
        exit_code=(
            EXIT_SUCCESS
        ),
        message=(
            result.message
            or "success"
        ),
        bootstrap_result=result,
    )


# ============================================================
# Sync CLI
# ============================================================


def main(
    argv: Sequence[
        str
    ] | None = None,
    *,
    bootstrap_factory: (
        BootstrapFactory
        | None
    ) = None,
) -> int:
    """
    同步 CLI 入口。

    如果调用者没有显式传入 bootstrap_factory，
    自动使用 application composition root：

        crawl_framework.app_factory
        .build_default_bootstrap
    """

    if bootstrap_factory is None:

        from crawl_framework.app_factory import (
            build_default_bootstrap,
        )

        bootstrap_factory = (
            build_default_bootstrap
        )

    result = asyncio.run(
        async_main(
            argv,
            bootstrap_factory=(
                bootstrap_factory
            ),
        )
    )

    return result.exit_code

# ============================================================
# python -m crawl_framework.cli.main
# ============================================================


if __name__ == "__main__":

    raise SystemExit(
        main()
    )
