from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
import uuid

from dataclasses import (
    asdict,
    dataclass,
)
from datetime import (
    datetime,
    timezone,
)
from pathlib import Path
from typing import (
    Any,
    Callable,
    Iterable,
    Mapping,
    Protocol,
    Sequence,
)

from crawl_framework.core.bootstrap import (
    BootstrapResult,
    CrawlBootstrap,
)
from crawl_framework.core.shutdown import (
    ShutdownController,
    ShutdownState,
    shutdown_signal_handlers,
)
from crawl_framework.core.platform import (
    PlatformOrchestrator,
    PlatformRunResult,
    PlatformSitePlan,
    PlatformSiteResult,
    PlatformSiteStatus,
    SitePreflight,
    resolve_global_profile,
)
from crawl_framework.core.concurrency import GlobalStageBudget


NAVER_FULL_PROFILE_DATASETS = (
    "forum_post",
    "news_article",
    "news_instrument",
    "research_report",
    "research_instrument",
    "attachment",
)

NAVER_INCREMENTAL_PROFILE_DATASETS = (
    "forum_post",
    "news_article",
    "news_instrument",
    "research_report",
    "research_instrument",
    "attachment",
)

NAVER_ROLLOUT_UNIVERSE_PATH = Path(
    "config/universes/"
    "naver_finance_kr_rollout_universe.txt"
)

TOSS_PROFILE_DATASETS = (
    "forum_post",
    "news_article",
    "news_instrument",
)

TOSS_ROLLOUT_UNIVERSE_PATH = Path(
    "config/universes/"
    "tossinvest_kr_rollout_universe.txt"
)

KABUTAN_PROFILE_DATASETS = ("news_article",)

HKEX_REPORT_PROFILE_DATASETS = (
    "financial_report",
    "financial_report_instrument",
    "attachment",
)

HKEX_ROLLOUT_UNIVERSE_PATH = Path(
    "config/universes/hkex_hk_rollout_universe.txt"
)


# ============================================================
# Exit codes
# ============================================================


EXIT_SUCCESS = 0

EXIT_STARTUP_BLOCKED = 2

EXIT_RUNTIME_ERROR = 3

EXIT_CONFIGURATION_ERROR = 4

EXIT_INTERRUPTED = 130


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

    profile: str | None = None

    instruments: tuple[
        str,
        ...,
    ] | None = None

    instruments_file: Path | None = None

    max_pages: int | None = None

    forum_max_pages: int | None = None

    news_max_pages: int | None = None

    research_max_pages: int | None = None

    news_mode: str | None = None

    forum_mode: str | None = None

    research_mode: str | None = None

    download_research_pdf: bool | None = None

    research_detail_workers: int | None = None

    pdf_workers: int | None = None

    forum_crawl_workers: int | None = None

    news_crawl_workers: int | None = None

    research_crawl_workers: int | None = None

    forum_http_concurrency: int | None = None

    news_http_concurrency: int | None = None

    research_http_concurrency: int | None = None

    progress_interval_seconds: float | None = None

    progress_enabled: bool | None = None

    progress_style: str = "auto"

    run_manifest: bool = False

    run_manifest_path: Path | None = None

    crawl_workers: int | None = None

    attachment_workers: int | None = None

    writer_workers: int | None = None

    upload_workers: int | None = None

    catalog_workers: int | None = None

    http_concurrency: int | None = None

    target_file_size_mb: int | None = None

    coalesce_scope_flushes: bool = False

    trust_rsync_success: bool = False

    ssh_multiplex: bool = False

    compact_after_dataset: bool = False

    compaction_min_file_count: int | None = None

    instrument_limit: int | None = None

    instrument_offset: int = 0

    research_categories: tuple[
        str,
        ...
    ] | None = None

    attachment_limit: int | None = None

    kabutan_start_month: str | None = None

    kabutan_end_month: str | None = None

    kabutan_overlap_months: int | None = None

    kabutan_max_pages_per_month: int | None = None

    report_types: tuple[str, ...] | None = None

    report_date_from: str | None = None

    report_date_to: str | None = None

    report_lookback_days: int | None = None

    download_report_pdf: bool | None = None


@dataclass(frozen=True, slots=True)
class PlatformCLIOptions:
    profile: str
    flush_at_end: bool = True
    json_output: bool = False
    recovery_only: bool = False
    progress_enabled: bool | None = None
    progress_style: str = "auto"
    progress_interval_seconds: float | None = None
    instrument_limit: int | None = None
    coalesce_scope_flushes: bool = False
    trust_rsync_success: bool = False
    ssh_multiplex: bool = False
    compact_after_dataset: bool = False
    site_workers: int = 2
    global_writer_workers: int = 2
    global_upload_workers: int = 2
    global_catalog_workers: int = 2
    run_manifest_path: Path | None = None

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

    platform_result: PlatformRunResult | None = None

    manifest_path: Path | None = None


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

    parser.add_argument(
        "--profile",
        choices=(
            "naver_full",
            "naver_incremental",
            "toss_full",
            "toss_incremental",
            "toss_historical_backfill",
            "kabutan_incremental",
            "kabutan_free_full",
            "kabutan_full",
            "hkex_reports_incremental",
            "hkex_reports_full",
        ),
        default=None,
        help=(
            "Named production crawl profile."
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

    parser.add_argument(
        "--instrument-offset",
        dest="instrument_offset",
        type=int,
        default=0,
        help=(
            "Skip the first N effective instruments before applying "
            "--instrument-limit. Useful for deterministic snapshot resume."
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
        "forum-max-pages",
        "news-max-pages",
        "research-max-pages",
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
                "Dataset-specific positive page "
                "limit override."
            ),
        )

    parser.add_argument(
        "--forum-mode",
        dest="forum_mode",
        choices=(
            "incremental",
            "full",
        ),
        default=None,
        help=(
            "Forum crawl mode."
        ),
    )

    parser.add_argument(
        "--news-mode",
        dest="news_mode",
        choices=(
            "incremental",
            "full",
        ),
        default=None,
        help=(
            "News crawl mode."
        ),
    )

    parser.add_argument(
        "--research-mode",
        dest="research_mode",
        choices=(
            "incremental",
            "full",
        ),
        default=None,
        help=(
            "Naver research crawl mode."
        ),
    )

    parser.add_argument(
        "--download-research-pdf",
        dest="download_research_pdf",
        action="store_true",
        default=None,
        help=(
            "Enable Naver research PDF attachment "
            "downloads for this run."
        ),
    )

    parser.add_argument(
        "--no-download-research-pdf",
        dest="download_research_pdf",
        action="store_false",
        help=(
            "Disable Naver research PDF attachment "
            "downloads for this run."
        ),
    )

    parser.add_argument(
        "--report-type",
        dest="report_types",
        action="append",
        choices=("annual", "interim", "quarterly", "all"),
        default=None,
    )
    parser.add_argument("--report-date-from", default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--report-date-to", default=None, metavar="YYYY-MM-DD")
    parser.add_argument(
        "--report-lookback-days", type=int, default=None, metavar="N"
    )
    parser.add_argument(
        "--download-report-pdf",
        dest="download_report_pdf",
        action="store_true",
        default=None,
    )
    parser.add_argument(
        "--no-download-report-pdf",
        dest="download_report_pdf",
        action="store_false",
    )

    for option_name in (
        "crawl-workers",
        "attachment-workers",
        "research-detail-workers",
        "pdf-workers",
        "forum-crawl-workers",
        "news-crawl-workers",
        "research-crawl-workers",
        "forum-http-concurrency",
        "news-http-concurrency",
        "research-http-concurrency",
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

    parser.add_argument(
        "--research-category",
        dest="research_categories",
        action="append",
        default=None,
        help=(
            "Naver research category to crawl. "
            "Can be supplied multiple times. "
            "Use all to crawl every supported category."
        ),
    )

    parser.add_argument(
        "--attachment-limit",
        dest="attachment_limit",
        type=int,
        default=None,
        help=(
            "Maximum number of attachments to process "
            "during this run. Must be positive."
        ),
    )

    parser.add_argument("--kabutan-start-month", default=None, metavar="YYYY-MM")
    parser.add_argument("--kabutan-end-month", default=None, metavar="YYYY-MM")
    parser.add_argument("--kabutan-overlap-months", type=int, default=None, metavar="N")
    parser.add_argument(
        "--kabutan-max-pages-per-month",
        type=int,
        default=None,
        metavar="N",
    )

    progress_group = parser.add_mutually_exclusive_group()
    progress_group.add_argument(
        "--progress",
        dest="progress_enabled",
        action="store_true",
        help="Enable production progress output.",
    )
    progress_group.add_argument(
        "--no-progress",
        dest="progress_enabled",
        action="store_false",
        help="Disable production progress output.",
    )
    parser.set_defaults(progress_enabled=None)

    parser.add_argument(
        "--progress-style",
        choices=("auto", "rich", "text"),
        default="auto",
        help="Progress renderer. Auto uses Rich on a TTY and text otherwise.",
    )

    parser.add_argument(
        "--progress-interval-seconds",
        dest="progress_interval_seconds",
        type=float,
        default=None,
        help=(
            "Print live production progress every N "
            "seconds. Positive number."
        ),
    )

    parser.add_argument(
        "--run-manifest",
        action="store_true",
        help=(
            "Write a JSON run manifest. Profiles "
            "enable this by default."
        ),
    )

    parser.add_argument(
        "--run-manifest-path",
        default=None,
        help=(
            "Path for the JSON run manifest. "
            "Defaults to state/run_manifests/<run_id>.json."
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

    parser.add_argument(
        "--coalesce-scope-flushes",
        action="store_true",
        help=(
            "Do not force a Parquet flush whenever a "
            "scope finishes. Completed scopes wait for "
            "size/age/final batch flush, reducing small "
            "files in large production rollouts."
        ),
    )

    parser.add_argument(
        "--trust-rsync-success",
        action="store_true",
        help=(
            "For rsync production uploads, treat a successful "
            "rsync exit code as verified and skip the extra "
            "per-file remote stat check. Catalog still records "
            "local file size and sha256."
        ),
    )

    parser.add_argument(
        "--ssh-multiplex",
        action="store_true",
        help=(
            "Use OpenSSH ControlMaster multiplexing for rsync "
            "production uploads. This reduces connection setup "
            "overhead for many small files."
        ),
    )

    parser.add_argument(
        "--compact-after-dataset",
        action="store_true",
        help=(
            "After each dataset finishes, compact active uploaded "
            "small Parquet files per storage partition and mark "
            "source files superseded in Catalog."
        ),
    )

    parser.add_argument(
        "--compaction-min-file-count",
        dest="compaction_min_file_count",
        type=int,
        default=None,
        help=(
            "Minimum active files in a partition before automatic "
            "post-dataset compaction runs. Must be positive."
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


def build_platform_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="crawl-framework run-platform",
        description="Run the official four-site production profile.",
    )
    parser.add_argument(
        "--profile",
        required=True,
        choices=("global_full", "global_incremental"),
    )
    parser.add_argument("--json", dest="json_output", action="store_true")
    parser.add_argument("--recovery-only", action="store_true")
    parser.add_argument("--no-final-flush", action="store_true")
    parser.add_argument("--instrument-limit", type=int, default=None)
    progress = parser.add_mutually_exclusive_group()
    progress.add_argument("--progress", dest="progress_enabled", action="store_true")
    progress.add_argument(
        "--no-progress", dest="progress_enabled", action="store_false"
    )
    parser.set_defaults(progress_enabled=None)
    parser.add_argument(
        "--progress-style",
        choices=("auto", "rich", "text"),
        default="auto",
    )
    parser.add_argument("--progress-interval-seconds", type=float, default=None)
    parser.add_argument("--coalesce-scope-flushes", action="store_true")
    parser.add_argument("--trust-rsync-success", action="store_true")
    parser.add_argument("--ssh-multiplex", action="store_true")
    parser.add_argument("--compact-after-dataset", action="store_true")
    parser.add_argument("--site-workers", type=int, default=2)
    parser.add_argument("--global-writer-workers", type=int, default=2)
    parser.add_argument("--global-upload-workers", type=int, default=2)
    parser.add_argument("--global-catalog-workers", type=int, default=2)
    parser.add_argument(
        "--run-manifest-path",
        type=Path,
        default=None,
        help=(
            "Path for the atomic platform manifest. Defaults to "
            "state/run_manifests/platform_<profile>_<timestamp>.json."
        ),
    )
    return parser


def parse_platform_args(
    argv: Sequence[str] | None = None,
) -> PlatformCLIOptions:
    values = list(sys.argv[1:] if argv is None else argv)
    if values and values[0] == "run-platform":
        values = values[1:]
    parser = build_platform_parser()
    args = parser.parse_args(values)
    if args.instrument_limit is not None and args.instrument_limit <= 0:
        parser.error("--instrument-limit must be positive")
    if (
        args.progress_interval_seconds is not None
        and args.progress_interval_seconds <= 0
    ):
        parser.error("--progress-interval-seconds must be positive")
    for name in (
        "site_workers",
        "global_writer_workers",
        "global_upload_workers",
        "global_catalog_workers",
    ):
        if getattr(args, name) < 1:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    if args.global_upload_workers > 4:
        parser.error("--global-upload-workers must not exceed 4")
    return PlatformCLIOptions(
        profile=args.profile,
        flush_at_end=not args.no_final_flush,
        json_output=bool(args.json_output),
        recovery_only=bool(args.recovery_only),
        progress_enabled=args.progress_enabled,
        progress_style=args.progress_style,
        progress_interval_seconds=args.progress_interval_seconds,
        instrument_limit=args.instrument_limit,
        coalesce_scope_flushes=bool(args.coalesce_scope_flushes),
        trust_rsync_success=bool(args.trust_rsync_success),
        ssh_multiplex=bool(args.ssh_multiplex),
        compact_after_dataset=bool(args.compact_after_dataset),
        site_workers=args.site_workers,
        global_writer_workers=args.global_writer_workers,
        global_upload_workers=args.global_upload_workers,
        global_catalog_workers=args.global_catalog_workers,
        run_manifest_path=args.run_manifest_path,
    )


def platform_site_options(
    platform: PlatformCLIOptions,
    plan: PlatformSitePlan,
) -> CLIOptions:
    argv = ["--site", plan.site_id, "--profile", plan.profile]
    if platform.json_output:
        argv.append("--json")
    if not platform.flush_at_end:
        argv.append("--no-final-flush")
    if platform.instrument_limit is not None and plan.site_id != "kabutan":
        argv.extend(("--instrument-limit", str(platform.instrument_limit)))
    if platform.progress_enabled is True:
        argv.append("--progress")
    elif platform.progress_enabled is False:
        argv.append("--no-progress")
    argv.extend(("--progress-style", platform.progress_style))
    if platform.progress_interval_seconds is not None:
        argv.extend((
            "--progress-interval-seconds",
            str(platform.progress_interval_seconds),
        ))
    if platform.coalesce_scope_flushes:
        argv.append("--coalesce-scope-flushes")
    if platform.trust_rsync_success:
        argv.append("--trust-rsync-success")
    if platform.ssh_multiplex:
        argv.append("--ssh-multiplex")
    if platform.compact_after_dataset:
        argv.append("--compact-after-dataset")
    return parse_args(argv)

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

    profile = args.profile

    if (
        profile is not None
        and not (
            profile.startswith("naver_")
            and site == "naver_finance"
        )
        and not (
            profile.startswith("toss_")
            and site == "tossinvest"
        )
        and not (
            profile.startswith("kabutan_")
            and site == "kabutan"
        )
        and not (
            profile.startswith("hkex_reports_")
            and site == "hkexnews"
        )
    ):

        parser.error(
            "--profile does not match --site"
        )

    if profile == "naver_full":

        if args.datasets is None:

            args.datasets = list(
                NAVER_FULL_PROFILE_DATASETS
            )

        if args.instruments is None and args.instruments_file is None:

            args.instruments_file = str(
                NAVER_ROLLOUT_UNIVERSE_PATH
            )

        if args.news_mode is None:

            args.news_mode = "full"

        if args.research_mode is None:

            args.research_mode = "full"

        if args.research_categories is None:

            args.research_categories = [
                "all",
            ]

        if args.forum_max_pages is None:

            args.forum_max_pages = 1

        if args.download_research_pdf is None:

            args.download_research_pdf = True

    elif profile == "naver_incremental":

        if args.datasets is None:

            args.datasets = list(
                NAVER_INCREMENTAL_PROFILE_DATASETS
            )

        if args.instruments is None and args.instruments_file is None:

            args.instruments_file = str(
                NAVER_ROLLOUT_UNIVERSE_PATH
            )

        if args.news_mode is None:

            args.news_mode = "incremental"

        if args.research_mode is None:

            args.research_mode = "incremental"

        if args.research_categories is None:

            args.research_categories = [
                "all",
            ]

        if args.download_research_pdf is None:

            args.download_research_pdf = True

    elif profile in {
        "toss_full",
        "toss_incremental",
        "toss_historical_backfill",
    }:

        if args.datasets is None:

            args.datasets = list(
                TOSS_PROFILE_DATASETS
            )

        if args.instruments is None and args.instruments_file is None:

            args.instruments_file = str(
                TOSS_ROLLOUT_UNIVERSE_PATH
            )

        if args.forum_mode is None:

            args.forum_mode = (
                "incremental"
                if profile == "toss_incremental"
                else "full"
            )

        if args.news_mode is None:

            args.news_mode = (
                "incremental"
                if profile == "toss_incremental"
                else "full"
            )

        if profile != "toss_historical_backfill":

            if args.forum_max_pages is None:

                args.forum_max_pages = 1

            if args.news_max_pages is None:

                args.news_max_pages = 1

    elif profile in {"kabutan_incremental", "kabutan_free_full", "kabutan_full"}:
        if args.datasets is None:
            args.datasets = list(KABUTAN_PROFILE_DATASETS)
        if args.kabutan_overlap_months is None:
            args.kabutan_overlap_months = 1
        if args.kabutan_max_pages_per_month is None:
            args.kabutan_max_pages_per_month = 500

    elif profile in {"hkex_reports_incremental", "hkex_reports_full"}:
        if args.download_report_pdf is None:
            args.download_report_pdf = True
        if args.datasets is None:
            args.datasets = list(HKEX_REPORT_PROFILE_DATASETS)
            if not args.download_report_pdf:
                args.datasets.remove("attachment")
        elif "attachment" in args.datasets and not args.download_report_pdf:
            parser.error("attachment dataset conflicts with --no-download-report-pdf")
        if args.instruments is None and args.instruments_file is None:
            args.instruments_file = str(HKEX_ROLLOUT_UNIVERSE_PATH)
        if args.report_types is None:
            args.report_types = ["all"]
        if profile == "hkex_reports_full" and args.report_date_from is None:
            args.report_date_from = "1999-04-01"
        if profile == "hkex_reports_incremental" and args.report_lookback_days is None:
            args.report_lookback_days = 400

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

    research_categories = normalize_repeated(
        args.research_categories
    )

    report_types = normalize_repeated(args.report_types)
    if report_types and "all" in report_types:
        report_types = ("annual", "interim", "quarterly")

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
    instrument_offset = int(args.instrument_offset)

    if instrument_offset < 0:
        parser.error("--instrument-offset must be >= 0")

    if instruments is not None and instrument_offset:
        instruments = instruments[instrument_offset:]

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

    attachment_limit = positive_optional_int(
        "attachment_limit"
    )

    forum_max_pages = positive_optional_int(
        "forum_max_pages"
    )

    news_max_pages = positive_optional_int(
        "news_max_pages"
    )

    research_max_pages = positive_optional_int(
        "research_max_pages"
    )

    research_detail_workers = positive_optional_int(
        "research_detail_workers"
    )

    pdf_workers = positive_optional_int(
        "pdf_workers"
    )

    forum_crawl_workers = positive_optional_int(
        "forum_crawl_workers"
    )

    news_crawl_workers = positive_optional_int(
        "news_crawl_workers"
    )

    research_crawl_workers = positive_optional_int(
        "research_crawl_workers"
    )

    forum_http_concurrency = positive_optional_int(
        "forum_http_concurrency"
    )

    news_http_concurrency = positive_optional_int(
        "news_http_concurrency"
    )

    research_http_concurrency = positive_optional_int(
        "research_http_concurrency"
    )

    kabutan_max_pages_per_month = positive_optional_int(
        "kabutan_max_pages_per_month"
    )

    kabutan_overlap_months = args.kabutan_overlap_months
    if kabutan_overlap_months is not None and kabutan_overlap_months < 0:
        parser.error("--kabutan-overlap-months must be >= 0")

    from crawl_framework.sites.kabutan.market_news import parse_month

    for name in ("kabutan_start_month", "kabutan_end_month"):
        value = getattr(args, name)
        if value is not None:
            try:
                parse_month(value)
            except ValueError as exc:
                parser.error(f"--{name.replace('_', '-')} {exc}")

    if args.kabutan_start_month and args.kabutan_end_month:
        if parse_month(args.kabutan_start_month) > parse_month(args.kabutan_end_month):
            parser.error("--kabutan-start-month must not be after --kabutan-end-month")

    report_lookback_days = positive_optional_int("report_lookback_days")
    from crawl_framework.sites.hkexnews import normalize_hkex_search_date

    for name in ("report_date_from", "report_date_to"):
        value = getattr(args, name)
        if value is not None:
            try:
                setattr(args, name, normalize_hkex_search_date(value))
            except ValueError as exc:
                parser.error(str(exc))
    if args.report_date_from and args.report_date_to:
        if args.report_date_from > args.report_date_to:
            parser.error("--report-date-from must not be after --report-date-to")

    progress_interval_seconds = (
        args.progress_interval_seconds
    )

    if progress_interval_seconds is not None:

        progress_interval_seconds = float(
            progress_interval_seconds
        )

        if progress_interval_seconds <= 0:

            parser.error(
                "--progress-interval-seconds "
                "must be positive"
            )

    # ========================================================
    # Return
    # ========================================================

    return CLIOptions(
        site=site,
        profile=profile,
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
        forum_max_pages=forum_max_pages,
        news_max_pages=news_max_pages,
        research_max_pages=research_max_pages,
        news_mode=args.news_mode,
        forum_mode=args.forum_mode,
        research_mode=args.research_mode,
        download_research_pdf=args.download_research_pdf,
        research_detail_workers=research_detail_workers,
        pdf_workers=pdf_workers,
        forum_crawl_workers=forum_crawl_workers,
        news_crawl_workers=news_crawl_workers,
        research_crawl_workers=research_crawl_workers,
        forum_http_concurrency=forum_http_concurrency,
        news_http_concurrency=news_http_concurrency,
        research_http_concurrency=research_http_concurrency,
        progress_interval_seconds=progress_interval_seconds,
        progress_enabled=args.progress_enabled,
        progress_style=args.progress_style,
        run_manifest=(
            bool(args.run_manifest)
            or profile is not None
        ),
        run_manifest_path=(
            Path(
                args.run_manifest_path
            ).expanduser()
            if args.run_manifest_path
            else None
        ),
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
        coalesce_scope_flushes=(
            bool(args.coalesce_scope_flushes)
        ),
        trust_rsync_success=(
            bool(args.trust_rsync_success)
        ),
        ssh_multiplex=(
            bool(args.ssh_multiplex)
        ),
        compact_after_dataset=(
            bool(args.compact_after_dataset)
        ),
        compaction_min_file_count=positive_optional_int(
            "compaction_min_file_count"
        ),
        instrument_limit=instrument_limit,
        instrument_offset=instrument_offset,
        research_categories=(
            research_categories
        ),
        attachment_limit=attachment_limit,
        kabutan_start_month=args.kabutan_start_month,
        kabutan_end_month=args.kabutan_end_month,
        kabutan_overlap_months=kabutan_overlap_months,
        kabutan_max_pages_per_month=kabutan_max_pages_per_month,
        report_types=report_types,
        report_date_from=args.report_date_from,
        report_date_to=args.report_date_to,
        report_lookback_days=report_lookback_days,
        download_report_pdf=args.download_report_pdf,
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
        "managed_resource_stats": _json_safe(
            result.managed_resource_stats
        ),
    }


def build_run_manifest(
    *,
    options: CLIOptions,
    result: BootstrapResult,
    run_id: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> dict[
    str,
    Any,
]:

    run_id = (
        run_id
        or uuid.uuid4().hex
    )

    started_at = (
        started_at
        or datetime.now(
            timezone.utc
        )
    )

    finished_at = (
        finished_at
        or datetime.now(
            timezone.utc
        )
    )

    universe = _universe_metadata(
        options.instruments_file
    )

    scope_plan = None
    if options.site == "kabutan":
        from crawl_framework.sites.kabutan.market_news import resolve_month_window

        mode = (
            "free_full"
            if options.profile in {"kabutan_free_full", "kabutan_full"}
            else "incremental"
        )
        runtime_payload = bootstrap_result_to_dict(result).get("runtime")
        runtime_scopes = (
            runtime_payload.get("runtime", [])
            if isinstance(runtime_payload, dict)
            else []
        )
        month_ids = tuple(dict.fromkeys(
            str(item.get("scope_id"))
            for item in runtime_scopes
            if isinstance(item, dict)
            and item.get("dataset") == "news_article"
            and item.get("scope_type") == "month"
            and item.get("scope_id")
        ))
        if not month_ids:
            month_ids = resolve_month_window(
                mode=mode,
                start_month=options.kabutan_start_month,
                end_month=options.kabutan_end_month,
                overlap_months=(
                    options.kabutan_overlap_months
                    if options.kabutan_overlap_months is not None
                    else 1
                ),
                now=started_at,
            )
        scope_plan = {
            "scope_type": "month",
            "scope_ids": list(month_ids),
            "count": len(month_ids),
        }

    return {
        "run_id": run_id,
        "site": options.site,
        "profile": options.profile,
        "datasets": list(
            options.datasets
            or ()
        ),
        "universe": universe,
        "scope_plan": scope_plan,
        "options": _json_safe(
            options
        ),
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "success": result.success,
        "crawler_started": result.crawler_started,
        "recovery": bootstrap_result_to_dict(
            result
        )[
            "recovery"
        ],
        "runtime": _json_safe(
            result.runtime
        ),
        "managed_resource_stats": _json_safe(
            result.managed_resource_stats
        ),
    }


def write_run_manifest(
    manifest: dict[
        str,
        Any,
    ],
    *,
    path: Path | None = None,
) -> Path:

    output_path = (
        path
        or Path(
            "state/run_manifests"
        )
        / f"{manifest['run_id']}.json"
    )

    return _write_json_atomic(output_path, manifest)


def build_platform_run_manifest(
    *,
    options: PlatformCLIOptions,
    result: PlatformRunResult,
    platform_run_id: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    site_options: Mapping[str, CLIOptions] | None = None,
) -> dict[str, Any]:
    """Build one restart-safe summary around the four independent site runs."""

    started_at = started_at or datetime.now(timezone.utc)
    finished_at = finished_at or datetime.now(timezone.utc)
    platform_run_id = platform_run_id or (
        f"platform-{options.profile}-{started_at:%Y%m%dT%H%M%S%fZ}-"
        f"{uuid.uuid4().hex[:8]}"
    )

    plans = resolve_global_profile(options.profile)
    options_by_site = dict(site_options or {})
    for plan in plans:
        if plan.site_id not in options_by_site:
            options_by_site[plan.site_id] = platform_site_options(options, plan)
    sites = [
        _platform_site_manifest(
            site_result,
            options_by_site[site_result.site_id],
        )
        for site_result in result.sites
    ]
    counters = _sum_platform_counters(sites)
    recovery = _sum_platform_recovery(sites)
    errors = [
        {
            "site_id": site["site_id"],
            "status": site["status"],
            "message": site["reason"],
        }
        for site in sites
        if site["status"] == PlatformSiteStatus.FAILED.value
        or int(site["recovery"].get("failed", 0)) > 0
        or int(site["recovery"].get("terminal_failed", 0)) > 0
    ]

    return {
        "schema_version": 1,
        "platform_run_id": platform_run_id,
        "profile": options.profile,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "final_state": _platform_final_state(result),
        "success": result.success,
        "interrupted": result.interrupted,
        "site_workers": result.site_workers,
        "resource_budget": _json_safe(result.resource_budget),
        "site_profiles": {
            site["site_id"]: site["profile"]
            for site in sites
        },
        "universe_snapshots": {
            site["site_id"]: site["universe"]
            for site in sites
        },
        "resume_plan": {
            site["site_id"]: site["resume_plan"]
            for site in sites
        },
        "counters": counters,
        "recovery": recovery,
        "blocked_sites": list(result.blocked_sites),
        "failed_sites": list(result.failed_sites),
        "errors": errors,
        "sites": sites,
    }


def write_platform_run_manifest(
    manifest: dict[str, Any],
    *,
    path: Path | None = None,
) -> Path:
    if path is None:
        timestamp = datetime.fromisoformat(manifest["started_at"]).astimezone(
            timezone.utc
        ).strftime("%Y%m%dT%H%M%S%fZ")
        path = (
            Path("state/run_manifests")
            / f"platform_{manifest['profile']}_{timestamp}.json"
        )
    return _write_json_atomic(path, manifest)


def _platform_site_manifest(
    site_result: PlatformSiteResult,
    options: CLIOptions,
) -> dict[str, Any]:
    bootstrap = (
        bootstrap_result_to_dict(site_result.result)
        if site_result.result is not None
        else None
    )
    runtime = bootstrap.get("runtime") if bootstrap is not None else None
    runtime = runtime if isinstance(runtime, dict) else {}
    recovery = (
        bootstrap["recovery"]
        if bootstrap is not None
        else _empty_recovery_summary()
    )
    return {
        "site_id": site_result.site_id,
        "profile": site_result.profile,
        "status": site_result.status.value,
        "reason": site_result.reason,
        "access_state": site_result.access_state,
        "datasets": list(options.datasets or ()),
        "universe": _universe_metadata(options.instruments_file),
        "resume_plan": _json_safe(runtime.get("resume_plans", {})),
        "counters": _platform_site_counters(runtime),
        "recovery": recovery,
        "crawler_started": (
            site_result.result.crawler_started
            if site_result.result is not None
            else False
        ),
        "runtime": _json_safe(runtime) if runtime else None,
        "managed_resource_stats": (
            bootstrap.get("managed_resource_stats")
            if bootstrap is not None
            else None
        ),
    }


def _platform_site_counters(runtime: dict[str, Any]) -> dict[str, int]:
    progress = runtime.get("progress")
    progress = progress if isinstance(progress, dict) else {}
    datasets = [
        dataset
        for site in progress.get("sites", [])
        if isinstance(site, dict)
        for dataset in site.get("datasets", [])
        if isinstance(dataset, dict)
    ]
    stats = runtime.get("production_stats")
    stats = stats if isinstance(stats, dict) else _json_safe(stats)
    stats = stats if isinstance(stats, dict) else {}

    total = _integer(progress.get("total_scopes"))
    completed = _integer(progress.get("completed_scopes"))
    incomplete = _integer(progress.get("remaining_scopes"))
    skipped = sum(_integer(item.get("skipped_scopes")) for item in datasets)
    if not progress:
        plans = [
            plan
            for plan in runtime.get("resume_plans", {}).values()
            if isinstance(plan, dict)
        ]
        total = sum(_integer(plan.get("total_scopes")) for plan in plans)
        completed = sum(
            _integer(plan.get("durable_complete_scopes")) for plan in plans
        )
        skipped = completed
        incomplete = max(0, total - completed)

    records = {
        name: sum(
            _integer(item.get("records", {}).get(name))
            for item in datasets
            if isinstance(item.get("records"), dict)
        )
        for name in ("crawled", "normalized", "new", "unchanged", "updated", "failed")
    }
    storage = {
        name: sum(
            _integer(item.get("storage", {}).get(name))
            for item in datasets
            if isinstance(item.get("storage"), dict)
        )
        for name in (
            "files_written",
            "files_uploaded",
            "files_verified",
            "files_cataloged",
        )
    }
    if not datasets:
        records["crawled"] = _integer(stats.get("records_crawled"))
        storage["files_written"] = _integer(stats.get("files_written"))
        storage["files_uploaded"] = _integer(stats.get("uploads_completed"))
        storage["files_cataloged"] = _integer(
            stats.get("catalog_jobs_completed")
        )
    pipeline_errors = _integer(runtime.get("pipeline_errors"))
    if not pipeline_errors:
        pipeline_errors = sum(
            _integer(item.get("errors"))
            for item in runtime.get("runtime", [])
            if isinstance(item, dict)
        )

    return {
        "total_scopes": total,
        "completed_scopes": completed,
        "skipped_scopes": skipped,
        "incomplete_scopes": incomplete,
        **{f"records_{name}": value for name, value in records.items()},
        **storage,
        "uploads_started": _integer(stats.get("uploads_started")),
        "uploads_completed": _integer(stats.get("uploads_completed")),
        "catalog_jobs_started": _integer(stats.get("catalog_jobs_started")),
        "catalog_jobs_completed": _integer(stats.get("catalog_jobs_completed")),
        "pipeline_errors": pipeline_errors,
    }


def _sum_platform_counters(sites: list[dict[str, Any]]) -> dict[str, int]:
    keys = tuple(next(iter(sites), {"counters": {}})["counters"])
    return {
        key: sum(_integer(site["counters"].get(key)) for site in sites)
        for key in keys
    }


def _sum_platform_recovery(sites: list[dict[str, Any]]) -> dict[str, Any]:
    numeric = (
        "scanned",
        "attempted",
        "recovered",
        "skipped",
        "failed",
        "terminal_failed",
        "remaining_pending",
    )
    result = {
        key: sum(_integer(site["recovery"].get(key)) for site in sites)
        for key in numeric
    }
    result["can_continue"] = all(
        bool(site["recovery"].get("can_continue", True))
        for site in sites
    )
    return result


def _empty_recovery_summary() -> dict[str, Any]:
    return {
        "scanned": 0,
        "attempted": 0,
        "recovered": 0,
        "skipped": 0,
        "failed": 0,
        "terminal_failed": 0,
        "remaining_pending": 0,
        "can_continue": True,
    }


def _platform_final_state(result: PlatformRunResult) -> str:
    if result.interrupted:
        return "INTERRUPTED"
    if result.failed_sites:
        return "FAILED"
    if result.blocked_sites:
        return "BLOCKED"
    return "COMPLETE"


def _integer(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return path


def _universe_metadata(
    path: Path | None,
) -> dict[
    str,
    Any,
] | None:

    if path is None:

        return None

    try:

        data = path.read_bytes()

    except OSError as exc:

        return {
            "path": str(
                path
            ),
            "error": str(
                exc
            ),
        }

    count = 0

    for line in data.decode(
        "utf-8",
        errors="replace",
    ).splitlines():

        stripped = line.strip()

        if (
            stripped
            and not stripped.startswith(
                "#"
            )
        ):

            count += 1

    return {
        "path": str(
            path
        ),
        "sha256": hashlib.sha256(
            data
        ).hexdigest(),
        "count": count,
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


def format_platform_result(result: PlatformRunResult) -> str:
    lines = [
        f"profile={result.profile}",
        f"success={str(result.success).lower()}",
        f"interrupted={str(result.interrupted).lower()}",
        f"site_workers={result.site_workers}",
        "resource_budget=" + json.dumps(result.resource_budget, sort_keys=True),
    ]
    lines.extend(
        "site={site} profile={profile} status={status}{reason}".format(
            site=item.site_id,
            profile=item.profile,
            status=item.status.value,
            reason=f" reason={item.reason}" if item.reason else "",
        )
        for item in result.sites
    )
    return "\n".join(lines)


async def async_platform_main(
    argv: Sequence[str] | None = None,
    *,
    bootstrap_factory: BootstrapFactory,
    stdout,
    stderr,
    platform_preflight: SitePreflight | None = None,
) -> CLIResult:
    options = parse_platform_args(argv)
    shutdown_controller = ShutdownController()
    started_at = datetime.now(timezone.utc)
    site_options = {
        plan.site_id: platform_site_options(options, plan)
        for plan in resolve_global_profile(options.profile)
    }

    def report_shutdown(state: ShutdownState) -> None:
        if state is ShutdownState.DRAINING:
            print(
                "interrupt received: stopping new sites/scopes and draining "
                "durable work; press Ctrl-C again to abort faster",
                file=stderr,
            )
        else:
            print(
                "second interrupt received: cancelling workers and preserving "
                "resume state",
                file=stderr,
            )

    orchestrator = PlatformOrchestrator(
        bootstrap_factory=bootstrap_factory,
        site_options_factory=lambda plan: site_options[plan.site_id],
        shutdown_controller=shutdown_controller,
        site_preflight=platform_preflight,
        site_workers=options.site_workers,
        stage_budget=GlobalStageBudget(
            writer_workers=options.global_writer_workers,
            upload_workers=options.global_upload_workers,
            catalog_workers=options.global_catalog_workers,
        ),
    )
    with shutdown_signal_handlers(
        shutdown_controller,
        on_request=report_shutdown,
    ):
        result = await orchestrator.run(
            options.profile,
            recovery_only=options.recovery_only,
        )

    manifest = build_platform_run_manifest(
        options=options,
        result=result,
        started_at=started_at,
        finished_at=datetime.now(timezone.utc),
        site_options=site_options,
    )
    manifest_path = write_platform_run_manifest(
        manifest,
        path=options.run_manifest_path,
    )

    if options.json_output:
        payload = result.to_dict()
        payload["manifest_path"] = str(manifest_path)
        print(json.dumps(_json_safe(payload), sort_keys=True), file=stdout)
    else:
        print(format_platform_result(result), file=stdout)
        print(f"manifest_path={manifest_path}", file=stdout)

    if result.interrupted:
        exit_code = EXIT_INTERRUPTED
        message = "platform run interrupted; resume state preserved"
    elif result.failed_sites:
        exit_code = EXIT_RUNTIME_ERROR
        message = "one or more platform sites failed"
    else:
        exit_code = EXIT_SUCCESS
        message = "platform run completed"
    return CLIResult(
        exit_code=exit_code,
        message=message,
        platform_result=result,
        manifest_path=manifest_path,
    )


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
    platform_preflight: SitePreflight | None = None,
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

    if bootstrap_factory is None:

        raise (
            BootstrapFactoryNotConfiguredError(
                "bootstrap factory is not "
                "configured yet"
            )
        )

    raw_argv = list(sys.argv[1:] if argv is None else argv)
    if raw_argv and raw_argv[0] == "run-platform":
        return await async_platform_main(
            raw_argv,
            bootstrap_factory=bootstrap_factory,
            stdout=stdout,
            stderr=stderr,
            platform_preflight=platform_preflight,
        )

    options = parse_args(
        raw_argv
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

        started_at = datetime.now(
            timezone.utc
        )

        runtime = getattr(bootstrap, "runtime", None)
        shutdown_controller = getattr(runtime, "shutdown_controller", None)
        if shutdown_controller is None:
            shutdown_controller = ShutdownController()
            if runtime is not None:
                setattr(runtime, "shutdown_controller", shutdown_controller)

        def report_shutdown(state: ShutdownState) -> None:
            if state is ShutdownState.DRAINING:
                print(
                    "interrupt received: stopping new scopes and draining durable work; "
                    "press Ctrl-C again to abort faster",
                    file=stderr,
                )
            else:
                print(
                    "second interrupt received: cancelling workers and preserving resume state",
                    file=stderr,
                )

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

            with shutdown_signal_handlers(
                shutdown_controller,
                on_request=report_shutdown,
            ):
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

        finished_at = datetime.now(
            timezone.utc
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

    if options.run_manifest:

        try:

            manifest = build_run_manifest(
                options=options,
                result=result,
                started_at=started_at,
                finished_at=finished_at,
            )

            manifest_path = write_run_manifest(
                manifest,
                path=options.run_manifest_path,
            )
            del manifest_path

        except Exception as exc:

            message = (
                "run manifest write failed: "
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

        if (
            isinstance(result.runtime, dict)
            and result.runtime.get("interrupted")
        ):
            return CLIResult(
                exit_code=EXIT_INTERRUPTED,
                message=result.message or "interrupted",
                bootstrap_result=result,
            )

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
