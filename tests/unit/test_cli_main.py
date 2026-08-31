import io
import json

import pytest

from crawl_framework.cli.main import (
    EXIT_CONFIGURATION_ERROR,
    EXIT_RUNTIME_ERROR,
    EXIT_STARTUP_BLOCKED,
    EXIT_SUCCESS,
    CLIOptions,
    async_main,
    build_parser,
    format_text_result,
    parse_args,
    run_recovery_only,
)
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
    ):

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
