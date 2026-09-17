import io
import json
from datetime import datetime, timezone

import pytest

from crawl_framework.cli.main import (
    EXIT_CONFIGURATION_ERROR,
    EXIT_INTERRUPTED,
    EXIT_RUNTIME_ERROR,
    EXIT_STARTUP_BLOCKED,
    EXIT_SUCCESS,
    CLIOptions,
    async_main,
    build_run_manifest,
    build_parser,
    format_text_result,
    parse_platform_args,
    platform_site_options,
    parse_args,
    run_recovery_only,
    write_run_manifest,
)
from crawl_framework.core.platform import PlatformSitePlan
from crawl_framework.core.bootstrap import (
    BootstrapResult,
)
from crawl_framework.storage.recovery_orchestrator import (
    StartupRecoveryResult,
)


# ============================================================
# Helpers
# ============================================================


def make_recovery_result(
    *,
    can_continue=True,
):

    return StartupRecoveryResult(
        scanned=1,
        attempted=1,
        recovered=1,
        skipped=0,
        failed=0,
        terminal_failed=0,
        remaining_pending=0,
        can_continue=(
            can_continue
        ),
        results=(),
    )


def make_bootstrap_result(
    *,
    success=True,
    crawler_started=True,
):

    return BootstrapResult(
        recovery=(
            make_recovery_result(
                can_continue=success
            )
        ),
        runtime={
            "done": True
        }
        if crawler_started
        else None,
        crawler_started=(
            crawler_started
        ),
        success=success,
        message=(
            "done"
            if success
            else "blocked"
        ),
    )


class FakeRecoveryOrchestrator:

    def __init__(
        self,
        result=None,
    ):

        self.result = (
            result
            or make_recovery_result()
        )

        self.run_count = 0


    def run(
        self,
    ):

        self.run_count += 1

        return self.result


class FakeBootstrap:

    def __init__(
        self,
        *,
        result=None,
        recovery_result=None,
        error=None,
    ):

        self.result = (
            result
            or make_bootstrap_result()
        )

        self.error = error

        self.calls = []

        self.recovery_orchestrator = (
            FakeRecoveryOrchestrator(
                recovery_result
            )
        )


    async def run(
        self,
        *,
        datasets=None,
        flush_at_end=True,
        startup_recovery_result=None,
    ):

        del startup_recovery_result

        self.calls.append(
            {
                "datasets": datasets,
                "flush_at_end": (
                    flush_at_end
                ),
            }
        )

        if self.error:

            raise self.error

        return self.result


# ============================================================
# Argument parsing
# ============================================================


def test_parse_minimal_args():

    options = parse_args(
        [
            "--site",
            "naver_finance",
        ]
    )

    assert (
        options.site
        == "naver_finance"
    )

    assert (
        options.datasets
        is None
    )

    assert (
        options.flush_at_end
        is True
    )

    assert (
        options.json_output
        is False
    )

    assert (
        options.recovery_only
        is False
    )

    assert (
        options.instruments
        is None
    )


def test_parse_global_incremental_platform_profile(tmp_path):
    manifest_path = tmp_path / "platform.json"
    options = parse_platform_args([
        "run-platform",
        "--profile",
        "global_incremental",
        "--instrument-limit",
        "2",
        "--no-progress",
        "--site-workers",
        "3",
        "--global-upload-workers",
        "2",
        "--run-manifest-path",
        str(manifest_path),
    ])

    assert options.profile == "global_incremental"
    assert options.instrument_limit == 2
    assert options.progress_enabled is False
    assert options.site_workers == 3
    assert options.global_writer_workers == 2
    assert options.global_upload_workers == 2
    assert options.global_catalog_workers == 2
    assert options.run_manifest_path == manifest_path


def test_platform_rejects_unsafe_global_upload_concurrency():
    with pytest.raises(SystemExit):
        parse_platform_args([
            "run-platform",
            "--profile",
            "global_incremental",
            "--global-upload-workers",
            "5",
        ])


def test_platform_profile_derives_official_single_site_options():
    platform = parse_platform_args([
        "run-platform",
        "--profile",
        "global_full",
        "--instrument-limit",
        "2",
        "--json",
    ])
    naver = platform_site_options(
        platform,
        PlatformSitePlan("naver_finance", "naver_full"),
    )
    kabutan = platform_site_options(
        platform,
        PlatformSitePlan("kabutan", "kabutan_free_full"),
    )

    assert naver.profile == "naver_full"
    assert naver.instrument_limit == 2
    assert len(naver.instruments) == 2
    assert naver.json_output is True
    assert kabutan.profile == "kabutan_free_full"
    assert kabutan.instrument_limit is None


def test_parse_naver_full_profile_expands_defaults():

    options = parse_args(
        [
            "crawl",
            "--site",
            "naver_finance",
            "--profile",
            "naver_full",
        ]
    )

    assert options.profile == "naver_full"
    assert options.datasets == (
        "forum_post",
        "news_article",
        "news_instrument",
        "research_report",
        "research_instrument",
        "attachment",
    )
    assert options.instruments is not None
    assert options.instruments[0] == "XKRX:005930"
    assert options.news_mode == "full"
    assert options.research_mode == "full"
    assert options.forum_max_pages == 1
    assert options.research_categories == (
        "all",
    )
    assert options.download_research_pdf is True


def test_parse_naver_incremental_profile_expands_defaults():

    options = parse_args(
        [
            "crawl",
            "--site",
            "naver_finance",
            "--profile",
            "naver_incremental",
        ]
    )

    assert options.profile == "naver_incremental"
    assert options.news_mode == "incremental"
    assert options.research_mode == "incremental"
    assert options.instruments[0] == "XKRX:005930"


def test_parse_toss_incremental_profile_expands_safe_defaults():

    options = parse_args(
        [
            "crawl",
            "--site",
            "tossinvest",
            "--profile",
            "toss_incremental",
        ]
    )

    assert options.datasets == (
        "forum_post",
        "news_article",
        "news_instrument",
    )
    assert options.instruments[0] == "XKRX:005930"
    assert options.forum_mode == "incremental"
    assert options.news_mode == "incremental"
    assert options.forum_max_pages == 1
    assert options.news_max_pages == 1
    assert options.run_manifest is True


def test_parse_toss_full_profile_is_bounded_and_overridable():

    options = parse_args(
        [
            "crawl",
            "--site",
            "tossinvest",
            "--profile",
            "toss_full",
            "--dataset",
            "forum_post",
            "--instrument",
            "005930",
            "--forum-mode",
            "incremental",
            "--forum-max-pages",
            "2",
        ]
    )

    assert options.datasets == ("forum_post",)
    assert options.instruments == ("005930",)
    assert options.forum_mode == "incremental"
    assert options.news_mode == "full"
    assert options.forum_max_pages == 2
    assert options.news_max_pages == 1


def test_parse_toss_historical_profile_does_not_impose_page_bounds():

    options = parse_args(
        [
            "crawl",
            "--site",
            "tossinvest",
            "--profile",
            "toss_historical_backfill",
        ]
    )

    assert options.forum_mode == "full"
    assert options.news_mode == "full"
    assert options.forum_max_pages is None
    assert options.news_max_pages is None


def test_parse_rejects_profile_for_the_wrong_site():

    with pytest.raises(SystemExit):

        parse_args(
            [
                "crawl",
                "--site",
                "naver_finance",
                "--profile",
                "toss_full",
            ]
        )


def test_parse_kabutan_incremental_profile_needs_no_instrument():
    options = parse_args([
        "--site", "kabutan",
        "--profile", "kabutan_incremental",
    ])

    assert options.datasets == ("news_article",)
    assert options.instruments is None
    assert options.instruments_file is None
    assert options.kabutan_start_month is None
    assert options.kabutan_end_month is None
    assert options.kabutan_overlap_months == 1
    assert options.kabutan_max_pages_per_month == 500
    assert options.run_manifest is True


def test_parse_kabutan_full_profile_and_explicit_overrides():
    options = parse_args([
        "--site", "kabutan",
        "--profile", "kabutan_full",
        "--kabutan-start-month", "2025-12",
        "--kabutan-end-month", "2026-02",
        "--kabutan-overlap-months", "0",
        "--kabutan-max-pages-per-month", "7",
    ])

    assert options.kabutan_start_month == "2025-12"
    assert options.kabutan_end_month == "2026-02"
    assert options.kabutan_overlap_months == 0
    assert options.kabutan_max_pages_per_month == 7


def test_parse_kabutan_free_full_profile_needs_no_premium_or_instrument():
    options = parse_args([
        "--site", "kabutan",
        "--profile", "kabutan_free_full",
    ])

    assert options.datasets == ("news_article",)
    assert options.instruments is None
    assert options.instruments_file is None
    assert options.kabutan_start_month is None
    assert options.kabutan_max_pages_per_month == 500


def test_parse_kabutan_rejects_invalid_month_range():
    with pytest.raises(SystemExit):
        parse_args([
            "--site", "kabutan",
            "--kabutan-start-month", "2026-13",
        ])


def test_kabutan_run_manifest_freezes_month_scope_plan():
    options = CLIOptions(
        site="kabutan",
        profile="kabutan_free_full",
        datasets=("news_article",),
        flush_at_end=True,
        json_output=True,
        recovery_only=False,
        kabutan_start_month="2025-12",
        kabutan_end_month="2026-02",
    )
    result = BootstrapResult(
        recovery=make_recovery_result(),
        runtime={
            "runtime": [
                {
                    "dataset": "news_article",
                    "scope_type": "month",
                    "scope_id": "2026-02",
                },
                {
                    "dataset": "news_article",
                    "scope_type": "month",
                    "scope_id": "2026-01",
                },
            ],
            "files_registered": 1,
        },
        crawler_started=True,
        success=True,
    )

    manifest = build_run_manifest(
        options=options,
        result=result,
        run_id="kabutan-run",
        started_at=datetime(2026, 2, 15, tzinfo=timezone.utc),
    )

    assert manifest["scope_plan"] == {
        "scope_type": "month",
        "scope_ids": ["2026-02", "2026-01"],
        "count": 2,
    }
    with pytest.raises(SystemExit):
        parse_args([
            "--site", "kabutan",
            "--kabutan-start-month", "2026-03",
            "--kabutan-end-month", "2026-02",
        ])


def test_parse_profile_allows_explicit_scope_overrides():

    options = parse_args(
        [
            "crawl",
            "--site",
            "naver_finance",
            "--profile",
            "naver_full",
            "--dataset",
            "news_article",
            "--instrument",
            "005930",
            "--news-mode",
            "incremental",
            "--no-download-research-pdf",
        ]
    )

    assert options.datasets == (
        "news_article",
    )
    assert options.instruments == (
        "005930",
    )
    assert options.news_mode == "incremental"
    assert options.download_research_pdf is False


def test_parse_run_manifest_options(
    tmp_path,
):

    path = tmp_path / "manifest.json"

    options = parse_args(
        [
            "crawl",
            "--site",
            "naver_finance",
            "--run-manifest",
            "--run-manifest-path",
            str(
                path
            ),
        ]
    )

    assert options.run_manifest is True
    assert options.run_manifest_path == path


def test_parse_multiple_datasets():

    options = parse_args(
        [
            "--site",
            "naver_finance",
            "--dataset",
            "forum_post",
            "--dataset",
            "news_article",
        ]
    )

    assert (
        options.datasets
        == (
            "forum_post",
            "news_article",
        )
    )


def test_duplicate_datasets_removed():

    options = parse_args(
        [
            "--site",
            "naver_finance",
            "--dataset",
            "forum_post",
            "--dataset",
            "forum_post",
        ]
    )

    assert (
        options.datasets
        == (
            "forum_post",
        )
    )


def test_no_final_flush():

    options = parse_args(
        [
            "--site",
            "demo",
            "--no-final-flush",
        ]
    )

    assert (
        options.flush_at_end
        is False
    )


def test_json_flag():

    options = parse_args(
        [
            "--site",
            "demo",
            "--json",
        ]
    )

    assert (
        options.json_output
        is True
    )


def test_recovery_only_flag():

    options = parse_args(
        [
            "--site",
            "demo",
            "--recovery-only",
        ]
    )

    assert (
        options.recovery_only
        is True
    )


# ============================================================
# Normal execution
# ============================================================


@pytest.mark.asyncio
async def test_async_main_success():

    bootstrap = FakeBootstrap()

    captured_options = []


    def factory(
        options,
    ):

        captured_options.append(
            options
        )

        return bootstrap


    stdout = io.StringIO()

    stderr = io.StringIO()

    result = await async_main(
        [
            "--site",
            "naver_finance",
        ],
        bootstrap_factory=factory,
        stdout=stdout,
        stderr=stderr,
    )

    assert (
        result.exit_code
        == EXIT_SUCCESS
    )

    assert (
        len(captured_options)
        == 1
    )

    assert (
        captured_options[0].site
        == "naver_finance"
    )

    assert (
        len(bootstrap.calls)
        == 1
    )

    assert (
        stderr.getvalue()
        == ""
    )


@pytest.mark.asyncio
async def test_datasets_passed_to_bootstrap():

    bootstrap = FakeBootstrap()


    def factory(
        options,
    ):

        return bootstrap


    await async_main(
        [
            "--site",
            "naver_finance",
            "--dataset",
            "forum_post",
            "--dataset",
            "news_article",
        ],
        bootstrap_factory=factory,
        stdout=io.StringIO(),
        stderr=io.StringIO(),
    )

    assert (
        bootstrap.calls[0][
            "datasets"
        ]
        == (
            "forum_post",
            "news_article",
        )
    )


@pytest.mark.asyncio
async def test_flush_option_passed():

    bootstrap = FakeBootstrap()


    def factory(
        options,
    ):

        return bootstrap


    await async_main(
        [
            "--site",
            "demo",
            "--no-final-flush",
        ],
        bootstrap_factory=factory,
        stdout=io.StringIO(),
        stderr=io.StringIO(),
    )

    assert (
        bootstrap.calls[0][
            "flush_at_end"
        ]
        is False
    )


# ============================================================
# Recovery-only
# ============================================================


@pytest.mark.asyncio
async def test_recovery_only_does_not_run_runtime():

    bootstrap = FakeBootstrap()


    def factory(
        options,
    ):

        return bootstrap


    result = await async_main(
        [
            "--site",
            "demo",
            "--recovery-only",
        ],
        bootstrap_factory=factory,
        stdout=io.StringIO(),
        stderr=io.StringIO(),
    )

    assert (
        result.exit_code
        == EXIT_SUCCESS
    )

    assert (
        bootstrap.calls
        == []
    )

    assert (
        bootstrap
        .recovery_orchestrator
        .run_count
        == 1
    )


# ============================================================
# Blocked
# ============================================================


@pytest.mark.asyncio
async def test_blocked_returns_exit_2():

    bootstrap = FakeBootstrap(
        result=(
            make_bootstrap_result(
                success=False,
                crawler_started=False,
            )
        )
    )


    def factory(
        options,
    ):

        return bootstrap


    result = await async_main(
        [
            "--site",
            "demo",
        ],
        bootstrap_factory=factory,
        stdout=io.StringIO(),
        stderr=io.StringIO(),
    )

    assert (
        result.exit_code
        == EXIT_STARTUP_BLOCKED
    )


# ============================================================
# Factory error
# ============================================================


@pytest.mark.asyncio
async def test_factory_error_returns_exit_4():

    def factory(
        options,
    ):

        raise RuntimeError(
            "bad config"
        )


    stderr = io.StringIO()

    result = await async_main(
        [
            "--site",
            "demo",
        ],
        bootstrap_factory=factory,
        stdout=io.StringIO(),
        stderr=stderr,
    )

    assert (
        result.exit_code
        == EXIT_CONFIGURATION_ERROR
    )

    assert (
        "bad config"
        in stderr.getvalue()
    )


# ============================================================
# Runtime error
# ============================================================


@pytest.mark.asyncio
async def test_runtime_error_returns_exit_3():

    bootstrap = FakeBootstrap(
        error=RuntimeError(
            "crawl exploded"
        )
    )


    def factory(
        options,
    ):

        return bootstrap


    stderr = io.StringIO()

    result = await async_main(
        [
            "--site",
            "demo",
        ],
        bootstrap_factory=factory,
        stdout=io.StringIO(),
        stderr=stderr,
    )

    assert (
        result.exit_code
        == EXIT_RUNTIME_ERROR
    )

    assert (
        "crawl exploded"
        in stderr.getvalue()
    )


# ============================================================
# JSON
# ============================================================


@pytest.mark.asyncio
async def test_json_output():

    bootstrap = FakeBootstrap()


    def factory(
        options,
    ):

        return bootstrap


    stdout = io.StringIO()

    result = await async_main(
        [
            "--site",
            "demo",
            "--json",
        ],
        bootstrap_factory=factory,
        stdout=stdout,
        stderr=io.StringIO(),
    )

    assert (
        result.exit_code
        == EXIT_SUCCESS
    )

    payload = json.loads(
        stdout.getvalue()
    )

    assert (
        payload[
            "success"
        ]
        is True
    )

    assert (
        payload[
            "recovery"
        ][
            "recovered"
        ]
        == 1
    )


# ============================================================
# Text output
# ============================================================


def test_format_text_result():

    text = format_text_result(
        make_bootstrap_result()
    )

    assert (
        "startup recovery:"
        in text
    )

    assert (
        "crawler_started=True"
        in text
    )

    assert (
        "success=True"
        in text
    )


def test_build_run_manifest_includes_universe_hash(
    tmp_path,
):

    universe = tmp_path / "universe.txt"
    universe.write_text(
        "# header\nXKRX:005930\nXKRX:000660\n",
        encoding="utf-8",
    )

    options = CLIOptions(
        site="naver_finance",
        profile="naver_incremental",
        datasets=(
            "news_article",
        ),
        flush_at_end=True,
        json_output=True,
        recovery_only=False,
        instruments_file=universe,
    )

    manifest = build_run_manifest(
        options=options,
        result=make_bootstrap_result(),
        run_id="run-1",
    )

    assert manifest[
        "run_id"
    ] == "run-1"

    assert manifest[
        "site"
    ] == "naver_finance"

    assert manifest[
        "profile"
    ] == "naver_incremental"

    assert manifest[
        "datasets"
    ] == [
        "news_article",
    ]

    assert manifest[
        "universe"
    ][
        "count"
    ] == 2

    assert len(
        manifest[
            "universe"
        ][
            "sha256"
        ]
    ) == 64


def test_write_run_manifest(
    tmp_path,
):

    path = write_run_manifest(
        {
            "run_id": "run-1",
            "site": "naver_finance",
        },
        path=tmp_path / "run.json",
    )

    assert path.exists()

    payload = json.loads(
        path.read_text(
            encoding="utf-8"
        )
    )

    assert payload[
        "run_id"
    ] == "run-1"


@pytest.mark.asyncio
async def test_async_main_writes_run_manifest(
    tmp_path,
):

    bootstrap = FakeBootstrap()
    manifest_path = tmp_path / "manifest.json"

    def factory(
        options,
    ):

        return bootstrap

    result = await async_main(
        [
            "crawl",
            "--site",
            "naver_finance",
            "--dataset",
            "news_article",
            "--run-manifest",
            "--run-manifest-path",
            str(
                manifest_path
            ),
        ],
        bootstrap_factory=factory,
        stdout=io.StringIO(),
        stderr=io.StringIO(),
    )

    assert result.exit_code == EXIT_SUCCESS
    assert manifest_path.exists()

    payload = json.loads(
        manifest_path.read_text(
            encoding="utf-8"
        )
    )

    assert payload[
        "site"
    ] == "naver_finance"

    assert payload[
        "datasets"
    ] == [
        "news_article",
    ]


@pytest.mark.asyncio
async def test_async_main_writes_interrupted_manifest_and_exits_130(tmp_path):
    interrupted = make_bootstrap_result(success=False)
    interrupted = BootstrapResult(
        recovery=interrupted.recovery,
        runtime={
            "interrupted": True,
            "shutdown_state": "draining",
            "shutdown_requests": 1,
        },
        crawler_started=True,
        success=False,
        message="crawler interrupted; resume state preserved",
    )
    bootstrap = FakeBootstrap(result=interrupted)
    manifest_path = tmp_path / "interrupted.json"

    result = await async_main(
        [
            "crawl",
            "--site",
            "naver_finance",
            "--run-manifest",
            "--run-manifest-path",
            str(manifest_path),
        ],
        bootstrap_factory=lambda options: bootstrap,
        stdout=io.StringIO(),
        stderr=io.StringIO(),
    )

    assert result.exit_code == EXIT_INTERRUPTED
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["success"] is False
    assert payload["runtime"]["interrupted"] is True
    assert payload["runtime"]["shutdown_state"] == "draining"


@pytest.mark.asyncio
async def test_run_platform_cli_outputs_clean_json_and_maps_four_profiles(tmp_path):
    observed = []

    def factory(options):
        observed.append((options.site, options.profile, options.json_output))
        return FakeBootstrap()

    async def preflight(plan, bootstrap):
        del bootstrap
        return "AVAILABLE" if plan.site_id == "kabutan" else None

    stdout = io.StringIO()
    result = await async_main(
        [
            "run-platform",
            "--profile",
            "global_incremental",
            "--json",
            "--instrument-limit",
            "2",
            "--run-manifest-path",
            str(tmp_path / "platform.json"),
        ],
        bootstrap_factory=factory,
        platform_preflight=preflight,
        stdout=stdout,
        stderr=io.StringIO(),
    )

    assert result.exit_code == EXIT_SUCCESS
    assert observed == [
        ("naver_finance", "naver_incremental", True),
        ("tossinvest", "toss_incremental", True),
        ("kabutan", "kabutan_incremental", True),
        ("hkexnews", "hkex_reports_incremental", True),
    ]
    payload = json.loads(stdout.getvalue())
    assert payload["profile"] == "global_incremental"
    assert payload["success"] is True
    assert len(payload["sites"]) == 4
    assert payload["site_workers"] == 2
    assert payload["resource_budget"]["writer"]["limit"] == 2
    assert payload["resource_budget"]["upload"]["limit"] == 2
    assert payload["resource_budget"]["catalog"]["limit"] == 2
    assert payload["manifest_path"] == str(tmp_path / "platform.json")
    assert result.manifest_path == tmp_path / "platform.json"


@pytest.mark.asyncio
async def test_run_platform_cli_treats_kabutan_waf_as_nonfatal_block(tmp_path):
    observed = []

    def factory(options):
        bootstrap = FakeBootstrap()
        observed.append((options.site, bootstrap))
        return bootstrap

    async def preflight(plan, bootstrap):
        del bootstrap
        return "WAF_BLOCKED" if plan.site_id == "kabutan" else None

    result = await async_main(
        [
            "run-platform",
            "--profile",
            "global_full",
            "--json",
            "--run-manifest-path",
            str(tmp_path / "blocked.json"),
        ],
        bootstrap_factory=factory,
        platform_preflight=preflight,
        stdout=io.StringIO(),
        stderr=io.StringIO(),
    )

    assert result.exit_code == EXIT_SUCCESS
    assert result.platform_result.blocked_sites == ("kabutan",)
    kabutan = next(item for site, item in observed if site == "kabutan")
    assert kabutan.calls == []
    hkex = next(item for site, item in observed if site == "hkexnews")
    assert len(hkex.calls) == 1
    manifest = json.loads(
        (tmp_path / "blocked.json").read_text(encoding="utf-8")
    )
    assert manifest["final_state"] == "BLOCKED"
    assert manifest["blocked_sites"] == ["kabutan"]


@pytest.mark.asyncio
async def test_run_platform_cli_persists_interrupted_manifest(tmp_path):
    interrupted = BootstrapResult(
        recovery=make_recovery_result(),
        runtime={
            "interrupted": True,
            "shutdown_state": "draining",
            "resume_plans": {},
        },
        crawler_started=True,
        success=False,
        message="platform site interrupted",
    )

    result = await async_main(
        [
            "run-platform",
            "--profile",
            "global_incremental",
            "--site-workers",
            "1",
            "--run-manifest-path",
            str(tmp_path / "interrupted-platform.json"),
        ],
        bootstrap_factory=lambda options: FakeBootstrap(result=interrupted),
        platform_preflight=lambda plan, bootstrap: _available_preflight(),
        stdout=io.StringIO(),
        stderr=io.StringIO(),
    )

    assert result.exit_code == EXIT_INTERRUPTED
    payload = json.loads(
        (tmp_path / "interrupted-platform.json").read_text(encoding="utf-8")
    )
    assert payload["final_state"] == "INTERRUPTED"
    assert payload["interrupted"] is True
    assert payload["sites"][0]["runtime"]["shutdown_state"] == "draining"


async def _available_preflight():
    return None

# ============================================================
# Instrument arguments
# ============================================================


def test_parse_single_instrument():

    options = parse_args(
        [
            "--site",
            "naver_finance",
            "--instrument",
            "005930",
        ]
    )

    assert (
        options.instruments
        == (
            "005930",
        )
    )


def test_parse_multiple_instruments():

    options = parse_args(
        [
            "--site",
            "naver_finance",
            "--instrument",
            "005930",
            "--instrument",
            "000660",
            "--instrument",
            "042700",
        ]
    )

    assert (
        options.instruments
        == (
            "005930",
            "000660",
            "042700",
        )
    )


def test_duplicate_instruments_removed():

    options = parse_args(
        [
            "--site",
            "naver_finance",
            "--instrument",
            "005930",
            "--instrument",
            "005930",
            "--instrument",
            "000660",
        ]
    )

    assert (
        options.instruments
        == (
            "005930",
            "000660",
        )
    )


def test_instruments_file_appends_after_instruments(
    tmp_path,
):

    path = (
        tmp_path
        / "instruments.txt"
    )

    path.write_text(
        "000660\n\n005930\n042700\n",
        encoding="utf-8",
    )

    options = parse_args(
        [
            "--site",
            "naver_finance",
            "--instrument",
            "005930",
            "--instruments-file",
            str(
                path
            ),
        ]
    )

    assert (
        options.instruments
        == (
            "005930",
            "000660",
            "042700",
        )
    )

    assert (
        options.instruments_file
        == path
    )


def test_instruments_file_rejects_empty_file(
    tmp_path,
):

    path = (
        tmp_path
        / "empty.txt"
    )

    path.write_text(
        "\n  \n",
        encoding="utf-8",
    )

    with pytest.raises(
        SystemExit
    ):

        parse_args(
            [
                "--site",
                "naver_finance",
                "--instruments-file",
                str(
                    path
                ),
            ]
        )


def test_instruments_file_ignores_snapshot_comments(
    tmp_path,
):

    path = (
        tmp_path
        / "universe.txt"
    )

    path.write_text(
        "# crawl_framework rollout universe snapshot\n"
        "# metadata: {\"count\": 2}\n"
        "XKRX:005930\n"
        "XKRX:000660\n",
        encoding="utf-8",
    )

    options = parse_args(
        [
            "--site",
            "naver_finance",
            "--instruments-file",
            str(
                path
            ),
        ]
    )

    assert (
        options.instruments
        == (
            "XKRX:005930",
            "XKRX:000660",
        )
    )


def test_instrument_whitespace_removed():

    options = parse_args(
        [
            "--site",
            "naver_finance",
            "--instrument",
            " 005930 ",
        ]
    )

    assert (
        options.instruments
        == (
            "005930",
        )
    )


def test_no_instrument_is_none():

    options = parse_args(
        [
            "--site",
            "naver_finance",
        ]
    )

    assert (
        options.instruments
        is None
    )


def test_non_numeric_instrument_allowed():

    options = parse_args(
        [
            "--site",
            "hotcopper",
            "--instrument",
            "BHP",
        ]
    )

    assert (
        options.instruments
        == (
            "BHP",
        )
    )

def test_parse_max_pages():

    from crawl_framework.cli.main import (
        parse_args,
    )

    options = parse_args(
        [
            "--site",
            "naver_finance",
            "--dataset",
            "forum_post",
            "--instrument",
            "005930",
            "--max-pages",
            "3",
        ]
    )

    assert (
        options.max_pages
        == 3
    )


def test_parse_dataset_specific_runtime_options():

    options = parse_args(
        [
            "crawl",
            "--site",
            "naver_finance",
            "--forum-max-pages",
            "2",
            "--forum-mode",
            "full",
            "--news-max-pages",
            "3",
            "--news-mode",
            "full",
            "--research-max-pages",
            "4",
            "--research-mode",
            "incremental",
            "--download-research-pdf",
            "--research-detail-workers",
            "5",
            "--pdf-workers",
            "6",
            "--forum-crawl-workers",
            "7",
            "--news-crawl-workers",
            "8",
            "--research-crawl-workers",
            "9",
            "--forum-http-concurrency",
            "10",
            "--news-http-concurrency",
            "11",
            "--research-http-concurrency",
            "12",
        ]
    )

    assert options.forum_max_pages == 2
    assert options.forum_mode == "full"
    assert options.news_max_pages == 3
    assert options.news_mode == "full"
    assert options.research_max_pages == 4
    assert options.research_mode == "incremental"
    assert options.download_research_pdf is True
    assert options.research_detail_workers == 5
    assert options.pdf_workers == 6
    assert options.forum_crawl_workers == 7
    assert options.news_crawl_workers == 8
    assert options.research_crawl_workers == 9
    assert options.forum_http_concurrency == 10
    assert options.news_http_concurrency == 11
    assert options.research_http_concurrency == 12


def test_parse_progress_interval_seconds():

    options = parse_args(
        [
            "crawl",
            "--site",
            "naver_finance",
            "--progress-interval-seconds",
            "0.5",
        ]
    )

    assert options.progress_interval_seconds == 0.5


def test_parse_progress_interval_seconds_rejects_zero():

    with pytest.raises(
        SystemExit
    ):

        parse_args(
            [
                "crawl",
                "--site",
                "naver_finance",
                "--progress-interval-seconds",
                "0",
            ]
            )


def test_parse_progress_flags_and_style():
    options = parse_args([
        "--site", "naver_finance",
        "--progress",
        "--progress-style", "text",
    ])

    assert options.progress_enabled is True
    assert options.progress_style == "text"


def test_parse_no_progress():
    options = parse_args([
        "--site", "naver_finance",
        "--no-progress",
    ])

    assert options.progress_enabled is False
    assert options.progress_style == "auto"


def test_progress_and_no_progress_are_mutually_exclusive():
    with pytest.raises(SystemExit):
        parse_args([
            "--site", "naver_finance",
            "--progress",
            "--no-progress",
        ])


def test_parse_no_download_research_pdf():

    options = parse_args(
        [
            "crawl",
            "--site",
            "naver_finance",
            "--no-download-research-pdf",
        ]
    )

    assert options.download_research_pdf is False


def test_parse_dataset_specific_max_pages_rejects_zero():

    with pytest.raises(
        SystemExit
    ):

        parse_args(
            [
                "crawl",
                "--site",
                "naver_finance",
                "--news-max-pages",
                "0",
            ]
        )


def test_parse_max_pages_rejects_zero():

    import pytest

    from crawl_framework.cli.main import (
        parse_args,
    )

    with pytest.raises(
        SystemExit
    ):

        parse_args(
            [
                "--site",
                "naver_finance",
                "--max-pages",
                "0",
            ]
        )


def test_parse_max_pages_rejects_negative():

    import pytest

    from crawl_framework.cli.main import (
        parse_args,
    )

    with pytest.raises(
        SystemExit
    ):

        parse_args(
            [
                "--site",
                "naver_finance",
                "--max-pages",
                "-2",
            ]
        )


def test_parse_generic_runtime_overrides():

    options = parse_args(
        [
            "--site",
            "naver_finance",
            "--crawl-workers",
            "32",
            "--attachment-workers",
            "8",
            "--writer-workers",
            "2",
            "--upload-workers",
            "4",
            "--catalog-workers",
            "3",
            "--http-concurrency",
            "64",
            "--target-file-size-mb",
            "128",
        ]
    )

    assert (
        options.crawl_workers
        == 32
    )

    assert (
        options.attachment_workers
        == 8
    )

    assert (
        options.writer_workers
        == 2
    )

    assert (
        options.upload_workers
        == 4
    )

    assert (
        options.catalog_workers
        == 3
    )

    assert (
        options.http_concurrency
        == 64
    )

    assert (
        options.target_file_size_mb
        == 128
    )


def test_parse_crawl_subcommand_is_supported():

    options = parse_args(
        [
            "crawl",
            "--site",
            "naver_finance",
            "--dataset",
            "forum_post",
        ]
    )

    assert (
        options.site
        == "naver_finance"
    )

    assert (
        options.datasets
        == (
            "forum_post",
        )
    )


def test_parse_instrument_limit_uses_effective_prefix(
    tmp_path,
):

    path = (
        tmp_path
        / "universe.txt"
    )

    path.write_text(
        "XKRX:005930\nXKRX:000660\nXKRX:042700\n",
        encoding="utf-8",
    )

    options = parse_args(
        [
            "crawl",
            "--site",
            "naver_finance",
            "--instruments-file",
            str(
                path
            ),
            "--instrument-limit",
            "2",
        ]
    )

    assert (
        options.instruments
        == (
            "XKRX:005930",
            "XKRX:000660",
        )
    )

    assert (
        options.instrument_limit
        == 2
    )


def test_parse_instrument_offset_then_limit_uses_stable_snapshot_slice(tmp_path):
    path = tmp_path / "universe.txt"
    path.write_text(
        "XKRX:005930\nXKRX:000660\nXKRX:042700\nXKRX:035420\n",
        encoding="utf-8",
    )

    options = parse_args(
        [
            "crawl",
            "--site",
            "tossinvest",
            "--instruments-file",
            str(path),
            "--instrument-offset",
            "1",
            "--instrument-limit",
            "2",
        ]
    )

    assert options.instruments == ("XKRX:000660", "XKRX:042700")
    assert options.instrument_offset == 1
    assert options.instrument_limit == 2


def test_parse_crawl_can_enable_coalesced_scope_flushes():

    options = parse_args(
        [
            "crawl",
            "--site",
            "naver_finance",
            "--dataset",
            "forum_post",
            "--coalesce-scope-flushes",
        ]
    )

    assert (
        options.coalesce_scope_flushes
        is True
    )


def test_parse_crawl_can_trust_rsync_success():

    options = parse_args(
        [
            "crawl",
            "--site",
            "naver_finance",
            "--dataset",
            "forum_post",
            "--trust-rsync-success",
        ]
    )

    assert (
        options.trust_rsync_success
        is True
    )


def test_parse_crawl_can_enable_ssh_multiplex():

    options = parse_args(
        [
            "crawl",
            "--site",
            "naver_finance",
            "--dataset",
            "forum_post",
            "--ssh-multiplex",
        ]
    )

    assert (
        options.ssh_multiplex
        is True
    )


def test_parse_crawl_can_enable_post_dataset_compaction():

    options = parse_args(
        [
            "crawl",
            "--site",
            "naver_finance",
            "--dataset",
            "forum_post",
            "--compact-after-dataset",
            "--compaction-min-file-count",
            "3",
        ]
    )

    assert (
        options.compact_after_dataset
        is True
    )

    assert (
        options.compaction_min_file_count
        == 3
    )


def test_parse_research_category_preserves_stable_order():

    options = parse_args(
        [
            "crawl",
            "--site",
            "naver_finance",
            "--dataset",
            "research_report",
            "--research-category",
            "market",
            "--research-category",
            "company",
            "--research-category",
            "market",
        ]
    )

    assert options.research_categories == (
        "market",
        "company",
    )


def test_parse_attachment_limit():

    options = parse_args(
        [
            "crawl",
            "--site",
            "naver_finance",
            "--attachment-limit",
            "1",
        ]
    )

    assert options.attachment_limit == 1


def test_parse_instrument_limit_rejects_zero():

    with pytest.raises(
        SystemExit
    ):

        parse_args(
            [
                "crawl",
                "--site",
                "naver_finance",
                "--instrument-limit",
                "0",
            ]
        )


def test_parse_generic_runtime_overrides_reject_zero():

    with pytest.raises(
        SystemExit
    ):

        parse_args(
            [
                "--site",
                "naver_finance",
                "--crawl-workers",
                "0",
            ]
        )


def test_hkex_full_profile_uses_canonical_snapshot_and_all_datasets():
    options = parse_args(
        ["crawl", "--site", "hkexnews", "--profile", "hkex_reports_full"]
    )

    assert options.datasets == (
        "financial_report",
        "financial_report_instrument",
        "attachment",
    )
    assert options.instruments is not None
    assert len(options.instruments) == 2798
    assert options.instruments[0] == "XHKG:00001"
    assert options.report_types == ("annual", "interim", "quarterly")
    assert options.report_date_from == "19990401"
    assert options.download_report_pdf is True
    assert options.run_manifest is True


def test_hkex_incremental_profile_can_disable_pdf_and_override_options():
    options = parse_args(
        [
            "crawl",
            "--site",
            "hkexnews",
            "--profile",
            "hkex_reports_incremental",
            "--instrument",
            "XHKG:00005",
            "--report-type",
            "annual",
            "--report-date-from",
            "2026-01-01",
            "--report-date-to",
            "2026-09-11",
            "--no-download-report-pdf",
        ]
    )

    assert options.datasets == (
        "financial_report",
        "financial_report_instrument",
    )
    assert options.instruments == ("XHKG:00005",)
    assert options.report_types == ("annual",)
    assert options.report_date_from == "20260101"
    assert options.report_date_to == "20260911"
    assert options.download_report_pdf is False


def test_hkex_profile_must_match_site():
    with pytest.raises(SystemExit):
        parse_args(
            [
                "crawl",
                "--site",
                "naver_finance",
                "--profile",
                "hkex_reports_full",
            ]
        )
