from datetime import (
    datetime,
    timezone,
)

import pytest

from crawl_framework.app_factory import (
    AppConfig,
    AppFactory,
    AppPaths,
    PostgresNotConfiguredError,
    SiteFactoryRegistry,
    UnknownSiteError,
    format_progress_snapshot,
    make_crawl_context,
    make_config_uploader_factory,
    _use_concurrent_production_runtime,
)

from crawl_framework.cli.main import (
    CLIOptions,
)

from crawl_framework.config import (
    load_config,
)

from crawl_framework.core.models import (
    CanonicalRecord,
)

from crawl_framework.core.concurrency import (
    DatasetResourceBudget,
    StageConcurrencyConfig,
)

from crawl_framework.core.concurrent_runtime import (
    ConcurrentProductionRuntime,
    ProgressSnapshot,
)
from crawl_framework.core.shutdown import ShutdownController

from crawl_framework.core.plugin import (
    CrawlCheckpoint,
    CrawlContext,
    CrawlScope,
    SitePlugin,
)

from crawl_framework.storage.uploader import (
    RsyncUploader,
)


# ============================================================
# Fake PostgreSQL
# ============================================================


class FakeCursor:

    def __init__(
        self,
        log,
    ):

        self.log = log


    def execute(
        self,
        sql,
        params=None,
    ):

        self.log.append(
            (
                sql,
                params,
            )
        )


    def __enter__(
        self,
    ):

        return self


    def __exit__(
        self,
        exc_type,
        exc,
        tb,
    ):

        return False


class FakeConnection:

    def __init__(
        self,
    ):

        self.executed = []

        self.commit_count = 0


    def cursor(
        self,
    ):

        return FakeCursor(
            self.executed
        )


    def commit(
        self,
    ):

        self.commit_count += 1


# ============================================================
# Demo plugin
# ============================================================

class DemoPlugin(
    SitePlugin
):
    """
    AppFactory 单元测试使用的最小 SitePlugin。

    实现当前 SitePlugin 所要求的接口：

        site_id
        country
        timezone
        datasets()
        discover()
        crawl()
        normalize()
        checkpoint_after_record()
    """

    site_id = "demo"

    country = "KR"

    timezone = "Asia/Seoul"


    def datasets(
        self,
    ) -> tuple[str, ...]:
        """
        返回该插件支持的数据集。
        """

        return (
            "forum_post",
        )


    async def discover(
        self,
        dataset,
        context,
    ):
        """
        发现需要抓取的 scope。
        """

        yield CrawlScope(
            scope_type="instrument",
            source_key="005930",
        )


    async def crawl(
        self,
        dataset,
        scope,
        checkpoint,
        context,
    ):
        """
        模拟抓取一条原始记录。
        """

        yield {
            "id": "1",
            "title": "hello",
        }


    def normalize(
        self,
        dataset,
        raw,
        scope,
    ):
        """
        将 raw record 转换成 CanonicalRecord。

        注意：
        当前 SitePlugin.normalize() 接口不接收 context。
        """

        return CanonicalRecord(
            site_id=self.site_id,
            country=self.country,
            dataset=dataset,
            source_id=raw["id"],
            scope_type=(
                scope.scope_type
            ),
            scope_id=(
                scope.scope_id
                or scope.source_key
            ),
            instrument_id=(
                "XKRX:005930"
            ),
            event_time=datetime(
                2026,
                8,
                25,
                1,
                0,
                tzinfo=timezone.utc,
            ),
            title=raw["title"],
        )


    def checkpoint_after_record(
        self,
        dataset,
        scope,
        raw,
        record,
        previous,
        context,
    ):
        """
        每处理完一条记录后更新 checkpoint。
        """

        state = {}

        if (
            previous is not None
            and isinstance(
                previous.state,
                dict,
            )
        ):

            state.update(
                previous.state
            )

        state[
            "last_id"
        ] = str(
            raw["id"]
        )

        return CrawlCheckpoint(
            state=state
        )


class AttachmentDemoPlugin(
    DemoPlugin
):

    def __init__(
        self,
    ):

        self.attachment_pipeline = None
        
# ============================================================
# Helpers
# ============================================================


def options(
    *,
    site="demo",
    **overrides,
):

    values = {
        "site": site,
        "datasets": None,
        "flush_at_end": True,
        "json_output": False,
        "recovery_only": False,
    }

    values.update(
        overrides
    )

    return CLIOptions(
        **values,
    )


# ============================================================
# AppPaths
# ============================================================


def test_app_paths(
    tmp_path,
):

    paths = (
        AppPaths.from_root(
            tmp_path
        )
    )

    assert (
        paths.state
        == tmp_path
        / "state"
    )

    assert (
        paths.warehouse
        == tmp_path
        / "warehouse"
    )

    paths.ensure()

    assert (
        paths.state.exists()
    )

    assert (
        paths.warehouse.exists()
    )

    assert (
        paths.recovery.exists()
    )


# ============================================================
# Registry
# ============================================================


def test_registry_create():

    registry = (
        SiteFactoryRegistry()
    )

    registry.register(
        "demo",
        DemoPlugin,
    )

    plugin = registry.create(
        "demo"
    )

    assert isinstance(
        plugin,
        DemoPlugin,
    )


def test_unknown_site():

    registry = (
        SiteFactoryRegistry()
    )

    with pytest.raises(
        UnknownSiteError
    ):

        registry.create(
            "missing"
        )


def test_duplicate_site_rejected():

    registry = (
        SiteFactoryRegistry()
    )

    registry.register(
        "demo",
        DemoPlugin,
    )

    with pytest.raises(
        ValueError
    ):

        registry.register(
            "demo",
            DemoPlugin,
        )


def test_replace_site():

    registry = (
        SiteFactoryRegistry()
    )

    registry.register(
        "demo",
        DemoPlugin,
    )

    registry.replace(
        "demo",
        DemoPlugin,
    )

    assert (
        registry.contains(
            "demo"
        )
    )


# ============================================================
# Build
# ============================================================


def test_build_requires_postgres(
    tmp_path,
):

    registry = (
        SiteFactoryRegistry()
    )

    registry.register(
        "demo",
        DemoPlugin,
    )

    factory = AppFactory(
        config=AppConfig(
            paths=(
                AppPaths.from_root(
                    tmp_path
                )
            )
        ),
        site_registry=registry,
    )

    with pytest.raises(
        PostgresNotConfiguredError
    ):

        factory.build(
            options()
        )


def test_build_bootstrap(
    tmp_path,
):

    registry = (
        SiteFactoryRegistry()
    )

    registry.register(
        "demo",
        DemoPlugin,
    )

    connection = (
        FakeConnection()
    )

    factory = AppFactory(
        config=AppConfig(
            paths=(
                AppPaths.from_root(
                    tmp_path
                )
            ),
            buffer_min_rows=1,
            buffer_max_rows=10,
        ),
        site_registry=registry,
        connection_factory=(
            lambda: connection
        ),
    )

    bootstrap = factory.build(
        options()
    )

    assert (
        bootstrap is not None
    )

    assert (
        bootstrap.runtime.plugin.site_id
        == "demo"
    )


def test_app_factory_wires_concurrent_runtime_when_workers_configured(
    tmp_path,
):

    registry = (
        SiteFactoryRegistry()
    )

    registry.register(
        "demo",
        DemoPlugin,
    )

    factory = AppFactory(
        config=AppConfig(
            paths=(
                AppPaths.from_root(
                    tmp_path
                )
            ),
            buffer_min_rows=1,
            buffer_max_rows=10,
            stage_concurrency=(
                StageConcurrencyConfig(
                    crawl_workers=2,
                    writer_workers=1,
                    upload_workers=2,
                    catalog_workers=2,
                )
            ),
        ),
        site_registry=registry,
        connection_factory=(
            FakeConnection
        ),
    )

    bootstrap = factory.build(
        options()
    )

    assert isinstance(
        bootstrap.runtime,
        ConcurrentProductionRuntime,
    )

    assert (
        bootstrap.runtime.crawl_workers
        == 2
    )

    assert (
        bootstrap.runtime.upload_workers
        == 2
    )

    assert bootstrap.runtime.record_queue_size == 1000
    assert bootstrap.runtime.upload_queue_size == 128
    assert bootstrap.runtime.catalog_queue_size == 128
    assert isinstance(
        bootstrap.runtime.shutdown_controller,
        ShutdownController,
    )


def test_app_factory_wires_browser_pool_for_browser_plugin(tmp_path):
    from crawl_framework.sites.tossinvest import TossInvestPlugin
    from crawl_framework.transports.playwright import BrowserWorkerPool

    registry = SiteFactoryRegistry()
    registry.register("tossinvest", TossInvestPlugin)
    bootstrap = AppFactory(
        config=AppConfig(
            paths=AppPaths.from_root(tmp_path),
            stage_concurrency=StageConcurrencyConfig(crawl_workers=2),
        ),
        site_registry=registry,
        connection_factory=FakeConnection,
    ).build(CLIOptions(
        site="tossinvest", datasets=("forum_post",), flush_at_end=True,
        json_output=True, recovery_only=False, instruments=("005930",),
    ))

    assert isinstance(bootstrap.runtime.context.browser, BrowserWorkerPool)
    assert bootstrap.managed_resources == (bootstrap.runtime.context.browser,)


def test_app_factory_wires_http_transport_for_http_plugin(tmp_path):
    from crawl_framework.sites.kabutan import KabutanPlugin
    from crawl_framework.transports.http import HttpTransport, RequestsHttpRequester

    registry = SiteFactoryRegistry()
    registry.register("kabutan", KabutanPlugin)
    bootstrap = AppFactory(
        config=AppConfig(paths=AppPaths.from_root(tmp_path)),
        site_registry=registry,
        connection_factory=FakeConnection,
    ).build(CLIOptions(
        site="kabutan", datasets=("news_article",), flush_at_end=True,
        json_output=True, recovery_only=False,
    ))

    transport = bootstrap.runtime.context.http
    assert isinstance(transport, HttpTransport)
    assert isinstance(transport.requester, RequestsHttpRequester)
    assert transport.retry_config.max_attempts == 5
    assert transport.max_concurrency == 1
    assert bootstrap.runtime.context.browser is None


def test_app_factory_passes_http_concurrency_to_transport(tmp_path):
    from crawl_framework.sites.kabutan import KabutanPlugin

    registry = SiteFactoryRegistry()
    registry.register("kabutan", KabutanPlugin)
    bootstrap = AppFactory(
        config=AppConfig(paths=AppPaths.from_root(tmp_path)),
        site_registry=registry,
        connection_factory=FakeConnection,
    ).build(CLIOptions(
        site="kabutan", datasets=("news_article",), flush_at_end=True,
        json_output=True, recovery_only=False, http_concurrency=7,
    ))

    assert bootstrap.runtime.context.http.max_concurrency == 7
    assert bootstrap.runtime.context.http.requester.pool_size == 7


def test_app_factory_wires_hkex_profile_to_concurrent_http_runtime(tmp_path):
    from crawl_framework.sites.hkexnews import HKEXNewsPlugin
    from crawl_framework.transports.http import HttpTransport

    registry = SiteFactoryRegistry()
    registry.register("hkexnews", HKEXNewsPlugin)
    bootstrap = AppFactory(
        config=AppConfig(
            paths=AppPaths.from_root(tmp_path),
            stage_concurrency=StageConcurrencyConfig(crawl_workers=2),
        ),
        site_registry=registry,
        connection_factory=FakeConnection,
    ).build(CLIOptions(
        site="hkexnews",
        profile="hkex_reports_incremental",
        datasets=("financial_report",),
        flush_at_end=True,
        json_output=True,
        recovery_only=False,
        instruments=("XHKG:00005",),
        report_types=("annual", "interim", "quarterly"),
        report_lookback_days=400,
    ))

    assert isinstance(bootstrap.runtime, ConcurrentProductionRuntime)
    assert bootstrap.runtime.resume_planner is not None
    assert bootstrap.runtime.progress_aggregator is not None
    assert isinstance(bootstrap.runtime.context.http, HttpTransport)
    assert bootstrap.runtime.plugin.attachment_pipeline is not None
    assert bootstrap.runtime.context.extra["hkex_report_mode"] == "incremental"


def test_make_crawl_context_passes_kabutan_profile_options():
    context = make_crawl_context(CLIOptions(
        site="kabutan",
        profile="kabutan_free_full",
        datasets=("news_article",),
        flush_at_end=True,
        json_output=True,
        recovery_only=False,
        kabutan_start_month="2013-09",
        kabutan_end_month="2013-11",
        kabutan_overlap_months=1,
        kabutan_max_pages_per_month=200,
    ))

    assert context.extra["kabutan_mode"] == "free_full"
    assert context.extra["kabutan_start_month"] == "2013-09"
    assert context.extra["kabutan_end_month"] == "2013-11"
    assert context.extra["kabutan_overlap_months"] == 1
    assert context.extra["kabutan_max_pages_per_month"] == 200


def test_legacy_kabutan_full_profile_is_a_free_full_alias():
    context = make_crawl_context(CLIOptions(
        site="kabutan",
        profile="kabutan_full",
        datasets=("news_article",),
        flush_at_end=True,
        json_output=True,
        recovery_only=False,
    ))

    assert context.extra["kabutan_mode"] == "free_full"


def test_app_factory_maps_pdf_workers_to_attachment_workers(
    tmp_path,
):

    registry = SiteFactoryRegistry()

    registry.register(
        "demo",
        DemoPlugin,
    )

    factory = AppFactory(
        config=AppConfig(
            paths=(
                AppPaths.from_root(
                    tmp_path
                )
            ),
            buffer_min_rows=1,
            buffer_max_rows=10,
        ),
        site_registry=registry,
        connection_factory=(
            FakeConnection
        ),
    )

    bootstrap = factory.build(
        options(
            pdf_workers=4
        )
    )

    assert isinstance(
        bootstrap.runtime,
        ConcurrentProductionRuntime,
    )


def test_app_factory_wires_progress_reporter_for_text_output(
    tmp_path,
):

    registry = SiteFactoryRegistry()

    registry.register(
        "demo",
        DemoPlugin,
    )

    factory = AppFactory(
        config=AppConfig(
            paths=(
                AppPaths.from_root(
                    tmp_path
                )
            ),
            buffer_min_rows=1,
            buffer_max_rows=10,
            stage_concurrency=StageConcurrencyConfig(
                crawl_workers=2,
            ),
        ),
        site_registry=registry,
        connection_factory=FakeConnection,
    )

    bootstrap = factory.build(
        options(
            progress_interval_seconds=1.0,
            json_output=False,
        )
    )

    assert isinstance(
        bootstrap.runtime,
        ConcurrentProductionRuntime,
    )

    assert (
        bootstrap.runtime.progress_reporter
        is not None
    )


def test_app_factory_disables_progress_reporter_for_json_output(
    tmp_path,
):

    registry = SiteFactoryRegistry()

    registry.register(
        "demo",
        DemoPlugin,
    )

    factory = AppFactory(
        config=AppConfig(
            paths=(
                AppPaths.from_root(
                    tmp_path
                )
            ),
            buffer_min_rows=1,
            buffer_max_rows=10,
            stage_concurrency=StageConcurrencyConfig(
                crawl_workers=2,
            ),
        ),
        site_registry=registry,
        connection_factory=FakeConnection,
    )

    bootstrap = factory.build(
        options(
            progress_interval_seconds=1.0,
            json_output=True,
        )
    )

    assert isinstance(
        bootstrap.runtime,
        ConcurrentProductionRuntime,
    )

    assert (
        bootstrap.runtime.progress_reporter
        is None
    )
    assert bootstrap.runtime.progress_finalizer is None


def test_app_factory_disables_progress_reporter_when_requested(tmp_path):
    registry = SiteFactoryRegistry()
    registry.register("demo", DemoPlugin)
    factory = AppFactory(
        config=AppConfig(
            paths=AppPaths.from_root(tmp_path),
            buffer_min_rows=1,
            buffer_max_rows=10,
            stage_concurrency=StageConcurrencyConfig(crawl_workers=2),
        ),
        site_registry=registry,
        connection_factory=FakeConnection,
    )

    bootstrap = factory.build(options(progress_enabled=False))

    assert bootstrap.runtime.progress_reporter is None
    assert bootstrap.runtime.progress_interval_seconds is None
    assert bootstrap.runtime.progress_finalizer is None


def test_format_progress_snapshot():

    text = format_progress_snapshot(
        ProgressSnapshot(
            dataset="forum_post",
            scopes_discovered=10,
            scopes_started=6,
            scopes_finished=4,
            records_crawled=100,
            files_written=3,
            uploads_completed=2,
            catalog_jobs_completed=1,
            record_queue_depth=5,
            upload_queue_depth=1,
            catalog_queue_depth=0,
            pending_scopes=2,
            crawl_busy_time=1.25,
            upload_busy_time=0.5,
            catalog_busy_time=0.25,
        )
    )

    assert "dataset=forum_post" in text
    assert "scopes=4/10" in text
    assert "records=100" in text
    assert "queues=record:5,upload:1,catalog:0" in text


def test_runtime_selector_keeps_legacy_single_worker_path():

    assert (
        _use_concurrent_production_runtime(
            StageConcurrencyConfig()
        )
        is False
    )

    assert (
        _use_concurrent_production_runtime(
            StageConcurrencyConfig(
                upload_workers=2
            )
        )
        is True
    )

    assert (
        _use_concurrent_production_runtime(
            StageConcurrencyConfig(
                attachment_workers=2
            )
        )
        is True
    )

    assert (
        _use_concurrent_production_runtime(
            StageConcurrencyConfig(),
            production_profile=True,
        )
        is True
    )


def test_named_profile_uses_bounded_runtime_with_single_worker_defaults(tmp_path):
    registry = SiteFactoryRegistry()
    registry.register("demo", DemoPlugin)
    bootstrap = AppFactory(
        config=AppConfig(paths=AppPaths.from_root(tmp_path)),
        site_registry=registry,
        connection_factory=FakeConnection,
    ).build(options(profile="demo_production"))

    assert isinstance(bootstrap.runtime, ConcurrentProductionRuntime)
    assert bootstrap.runtime.crawl_workers == 1
    assert bootstrap.runtime.writer_workers == 1
    assert bootstrap.runtime.upload_workers == 1
    assert bootstrap.runtime.catalog_workers == 1


@pytest.mark.asyncio
async def test_built_app_runs(
    tmp_path,
):

    registry = (
        SiteFactoryRegistry()
    )

    registry.register(
        "demo",
        DemoPlugin,
    )

    connection = (
        FakeConnection()
    )

    factory = AppFactory(
        config=AppConfig(
            paths=(
                AppPaths.from_root(
                    tmp_path
                )
            ),
            buffer_min_rows=1,
            buffer_max_rows=10,
        ),
        site_registry=registry,
        connection_factory=(
            lambda: connection
        ),
    )

    bootstrap = factory.build(
        options()
    )

    result = await bootstrap.run()

    assert (
        result.success
        is True
    )

    assert (
        result.crawler_started
        is True
    )

def test_make_crawl_context_with_instruments():

    options = CLIOptions(
        site="naver_finance",
        datasets=(
            "forum_post",
        ),
        flush_at_end=True,
        json_output=False,
        recovery_only=False,
        instruments=(
            "005930",
            "000660",
        ),
    )

    context = make_crawl_context(
        options
    )

    assert (
        context.extra[
            "instrument_codes"
        ]
        == (
            "005930",
            "000660",
        )
    )

def test_make_crawl_context_without_instruments():

    options = CLIOptions(
        site="naver_finance",
        datasets=(
            "forum_post",
        ),
        flush_at_end=True,
        json_output=False,
        recovery_only=False,
        instruments=None,
    )

    context = make_crawl_context(
        options
    )

    assert (
        context.extra[
            "instrument_codes"
        ]
        == ()
    )


def test_make_crawl_context_passes_hkex_full_profile_options():
    context = make_crawl_context(
        CLIOptions(
            site="hkexnews",
            datasets=("financial_report",),
            flush_at_end=True,
            json_output=False,
            recovery_only=False,
            profile="hkex_reports_full",
            instruments=("XHKG:00005",),
            report_types=("annual", "quarterly"),
            report_date_from="19990401",
            report_date_to="20260911",
            download_report_pdf=False,
        )
    )

    assert context.extra["hkex_report_mode"] == "full"
    assert context.extra["hkex_report_types"] == ("annual", "quarterly")
    assert context.extra["hkex_report_date_from"] == "19990401"
    assert context.extra["hkex_report_date_to"] == "20260911"
    assert context.extra["download_report_pdf"] is False


def test_make_crawl_context_hkex_incremental_builds_lookback_window():
    context = make_crawl_context(
        CLIOptions(
            site="hkexnews",
            datasets=("financial_report",),
            flush_at_end=True,
            json_output=False,
            recovery_only=False,
            profile="hkex_reports_incremental",
            report_lookback_days=400,
        )
    )

    assert context.extra["hkex_report_mode"] == "incremental"
    assert len(context.extra["hkex_report_date_from"]) == 8
    assert len(context.extra["hkex_report_date_to"]) == 8
    assert (
        context.extra["hkex_report_date_from"]
        < context.extra["hkex_report_date_to"]
    )

def test_app_factory_initializes_postgres_schema(
    tmp_path,
):

    connection = FakeConnection()

    def connection_factory():

        return connection

    registry = (
        SiteFactoryRegistry()
    )

    registry.register(
        "demo",
        DemoPlugin,
    )

    paths = AppPaths.from_root(
        tmp_path
    )

    config = AppConfig(
        paths=paths,
    )

    factory = AppFactory(
        config=config,
        site_registry=registry,
        connection_factory=(
            connection_factory
        ),
    )

    options = CLIOptions(
        site="demo",
        datasets=(
            "forum_post",
        ),
        flush_at_end=True,
        json_output=False,
        recovery_only=False,
        instruments=None,
    )

    factory.build(
        options
    )

    executed_sql = "\n".join(
        str(sql)
        for sql
        in connection.executed
    )

    assert (
        "CREATE SCHEMA"
        in executed_sql.upper()
    )

    assert (
        "DATA_FILES"
        in executed_sql.upper()
    )


def test_app_factory_injects_attachment_pipeline(
    tmp_path,
):

    plugin = AttachmentDemoPlugin()

    registry = SiteFactoryRegistry()

    registry.register(
        "demo",
        lambda: plugin,
    )

    factory = AppFactory(
        config=AppConfig(
            paths=AppPaths.from_root(
                tmp_path
            ),
        ),
        site_registry=registry,
        connection_factory=lambda: FakeConnection(),
    )

    factory.build(
        options()
    )

    assert plugin.attachment_pipeline is not None

    assert (
        plugin
        .attachment_pipeline
        .store
        .root
        ==
        tmp_path
        / "warehouse"
    )


def test_config_uploader_factory_uses_rsync(
    tmp_path,
):

    from crawl_framework.config import (
        load_config,
    )

    config_path = (
        tmp_path
        / "config.yaml"
    )

    config_path.write_text(
        """
local:
  output_dir: "./outputs"
  warehouse_dir: "./warehouse"
  index_cache_dir: "./index"

server:
  host: "192.168.1.33"
  user: "dwyao"
  data_dir: "/mnt/data/stocklake"

postgres:
  host: "192.168.1.33"
  port: 5432
  database: "stock_data"
  user: "stock"

storage:
  local_warehouse: "./warehouse"
  server_warehouse: "/mnt/data/stocklake"

sync:
  delete_after_upload: true
  method: auto
  rsync:
    enabled: true
    ssh_port: 22
""",
        encoding="utf-8",
    )

    framework_config = (
        load_config(
            config_path
        )
    )

    factory = (
        make_config_uploader_factory(
            framework_config
        )
    )

    app_config = AppConfig(
        paths=(
            AppPaths.from_root(
                tmp_path
            )
        )
    )

    uploader = factory(
        app_config
    )

    assert isinstance(
        uploader,
        RsyncUploader,
    )


def test_config_uploader_factory_passes_ssh_multiplex_options(
    tmp_path,
):

    config_path = (
        tmp_path
        / "config"
        / "config.yaml"
    )

    config_path.parent.mkdir()

    config_path.write_text(
        """
local:
  output_dir: "./outputs"
  warehouse_dir: "./warehouse"
  index_cache_dir: "./index"

server:
  host: "192.168.1.33"
  user: "dwyao"
  data_dir: "/mnt/data/stocklake"

postgres:
  host: "192.168.1.33"
  port: 5432
  database: "stock_data"
  user: "stock"

storage:
  local_warehouse: "./warehouse"
  server_warehouse: "/mnt/data/stocklake"

sync:
  delete_after_upload: true
  method: auto
  rsync:
    enabled: true
    ssh_port: 22
    ssh_multiplex: true
    ssh_control_path: "/tmp/crawl-fw-%r@%h:%p"
    ssh_control_persist: "5m"
""",
        encoding="utf-8",
    )

    framework_config = (
        load_config(
            config_path
        )
    )

    factory = (
        make_config_uploader_factory(
            framework_config
        )
    )

    uploader = factory(
        AppConfig(
            paths=(
                AppPaths.from_root(
                    tmp_path
                )
            )
        )
    )

    assert isinstance(
        uploader,
        RsyncUploader,
    )

    assert (
        uploader.ssh_multiplex
        is True
    )

    assert (
        uploader.ssh_control_path
        == "/tmp/crawl-fw-%r@%h:%p"
    )

    assert (
        uploader.ssh_control_persist
        == "5m"
    )


def test_config_uploader_factory_cli_ssh_multiplex_override(
    tmp_path,
):

    config_path = (
        tmp_path
        / "config"
        / "config.yaml"
    )

    config_path.parent.mkdir()

    config_path.write_text(
        """
local:
  output_dir: "./outputs"
  warehouse_dir: "./warehouse"
  index_cache_dir: "./index"

server:
  host: "192.168.1.33"
  user: "dwyao"
  data_dir: "/mnt/data/stocklake"

postgres:
  host: "192.168.1.33"
  port: 5432
  database: "stock_data"
  user: "stock"

storage:
  local_warehouse: "./warehouse"
  server_warehouse: "/mnt/data/stocklake"

sync:
  delete_after_upload: true
  method: auto
  rsync:
    enabled: true
    ssh_port: 22
    ssh_multiplex: false
""",
        encoding="utf-8",
    )

    framework_config = (
        load_config(
            config_path
        )
    )

    factory = (
        make_config_uploader_factory(
            framework_config,
            ssh_multiplex=True,
        )
    )

    uploader = factory(
        AppConfig(
            paths=(
                AppPaths.from_root(
                    tmp_path
                )
            )
        )
    )

    assert isinstance(
        uploader,
        RsyncUploader,
    )

    assert (
        uploader.ssh_multiplex
        is True
    )


def test_load_config_parses_runtime_and_queue_knobs(
    tmp_path,
):

    from crawl_framework.config import (
        load_config,
    )

    config_path = (
        tmp_path
        / "config.yaml"
    )

    config_path.write_text(
        """
local:
  output_dir: "./outputs"
  warehouse_dir: "./warehouse"
  index_cache_dir: "./index"

server:
  host: "192.168.1.33"
  user: "dwyao"
  data_dir: "/mnt/data/stocklake"

postgres:
  host: "192.168.1.33"
  port: 5432
  database: "stock_data"
  user: "stock"

storage:
  local_warehouse: "./warehouse"
  server_warehouse: "/mnt/data/stocklake"
  target_file_size_mb: 128
  max_rows_per_file: 200000
  max_buffer_age_seconds: 60

runtime:
  crawl_workers: 32
  attachment_workers: 8
  writer_workers: 2
  upload_workers: 4
  catalog_workers: 3

http:
  concurrency: 64

queues:
  records: 5000
  durable_files: 256
  uploads: 128
  catalog: 64
  attachments: 512
""",
        encoding="utf-8",
    )

    config = load_config(
        config_path
    )

    assert (
        config.runtime.crawl_workers
        == 32
    )

    assert (
        config.storage.target_file_size_mb
        == 128
    )

    assert (
        config.storage.max_rows
        == 200000
    )

    assert (
        config.storage.max_buffer_age_seconds
        == 60
    )

    assert (
        config.runtime.http_concurrency
        == 64
    )

    assert (
        config.runtime.attachment_workers
        == 8
    )

    assert (
        config.runtime.catalog_workers
        == 3
    )

    assert (
        config.queues.records
        == 5000
    )

    assert (
        config.queues.attachments
        == 512
    )

def test_make_crawl_context_passes_max_pages():

    from crawl_framework.app_factory import (
        make_crawl_context,
    )

    from crawl_framework.cli.main import (
        CLIOptions,
    )

    options = CLIOptions(
        site="naver_finance",
        datasets=(
            "forum_post",
        ),
        flush_at_end=True,
        json_output=False,
        recovery_only=False,
        instruments=(
            "005930",
        ),
        max_pages=3,
    )

    context = (
        make_crawl_context(
            options
        )
    )

    assert (
        context.extra[
            "instrument_codes"
        ]
        ==
        (
            "005930",
        )
    )

    assert (
        context.extra[
            "forum_max_pages"
        ]
        == 3
    )

    assert (
        context.extra[
            "news_max_pages"
        ]
        == 3
    )

    assert (
        context.extra[
            "research_max_pages"
        ]
        == 3
    )


def test_make_crawl_context_dataset_specific_options_override_max_pages():

    options = CLIOptions(
        site="naver_finance",
        datasets=(
            "news_article",
            "research_report",
        ),
        flush_at_end=True,
        json_output=False,
        recovery_only=False,
        max_pages=9,
        forum_max_pages=2,
        news_max_pages=3,
        research_max_pages=4,
        news_mode="full",
        research_mode="incremental",
        download_research_pdf=False,
        research_detail_workers=5,
        pdf_workers=6,
        forum_crawl_workers=7,
        news_crawl_workers=8,
        research_crawl_workers=9,
        forum_http_concurrency=10,
        news_http_concurrency=11,
        research_http_concurrency=12,
    )

    context = make_crawl_context(
        options
    )

    assert context.extra[
        "forum_max_pages"
    ] == 2

    assert context.extra[
        "news_max_pages"
    ] == 3

    assert context.extra[
        "research_max_pages"
    ] == 4

    assert context.extra[
        "news_mode"
    ] == "full"

    assert context.extra[
        "research_mode"
    ] == "incremental"

    assert context.extra[
        "download_research_pdf"
    ] is False

    assert context.extra[
        "research_detail_workers"
    ] == 5

    assert context.extra[
        "pdf_workers"
    ] == 6

    budgets = context.extra[
        "dataset_budgets"
    ]

    assert budgets[
        "forum_post"
    ] == DatasetResourceBudget(
        crawl_workers=7,
        http_concurrency=10,
    )

    assert budgets[
        "news_article"
    ] == DatasetResourceBudget(
        crawl_workers=8,
        http_concurrency=11,
    )

    assert budgets[
        "research_report"
    ] == DatasetResourceBudget(
        crawl_workers=9,
        http_concurrency=12,
        detail_workers=5,
    )

    assert budgets[
        "attachment"
    ] == DatasetResourceBudget(
        crawl_workers=9,
        http_concurrency=12,
        detail_workers=5,
        attachment_workers=6,
    )


def test_make_crawl_context_naver_full_profile_uses_bounded_forum_pages():

    options = CLIOptions(
        site="naver_finance",
        datasets=None,
        flush_at_end=True,
        json_output=False,
        recovery_only=False,
        profile="naver_full",
    )

    context = make_crawl_context(
        options
    )

    assert "forum_max_pages" not in context.extra

    assert "news_max_pages" not in context.extra

    assert "research_max_pages" not in context.extra


def test_make_crawl_context_naver_full_profile_respects_page_overrides():

    options = CLIOptions(
        site="naver_finance",
        datasets=None,
        flush_at_end=True,
        json_output=False,
        recovery_only=False,
        profile="naver_full",
        news_max_pages=2,
    )

    context = make_crawl_context(
        options
    )

    assert "forum_max_pages" not in context.extra

    assert context.extra[
        "news_max_pages"
    ] == 2


def test_make_crawl_context_passes_generic_forum_mode():

    options = CLIOptions(
        site="tossinvest",
        datasets=("forum_post",),
        flush_at_end=True,
        json_output=False,
        recovery_only=False,
        profile="toss_incremental",
        forum_mode="incremental",
    )

    context = make_crawl_context(
        options
    )

    assert context.extra[
        "forum_mode"
    ] == "incremental"


def test_make_crawl_context_passes_research_categories():

    from crawl_framework.app_factory import (
        make_crawl_context,
    )

    from crawl_framework.cli.main import (
        CLIOptions,
    )

    options = CLIOptions(
        site="naver_finance",
        datasets=(
            "research_report",
        ),
        flush_at_end=True,
        json_output=False,
        recovery_only=False,
        research_categories=(
            "market",
            "company",
        ),
    )

    context = make_crawl_context(
        options
    )

    assert context.extra[
        "research_categories"
    ] == (
        "market",
        "company",
    )


def test_make_crawl_context_passes_attachment_limit():

    options = CLIOptions(
        site="naver_finance",
        datasets=(
            "attachment",
        ),
        flush_at_end=True,
        json_output=False,
        recovery_only=False,
        attachment_limit=1,
    )

    context = make_crawl_context(
        options
    )

    assert context.extra[
        "attachment_limit"
    ] == 1

def test_make_crawl_context_omits_max_pages_when_none():

    from crawl_framework.app_factory import (
        make_crawl_context,
    )

    from crawl_framework.cli.main import (
        CLIOptions,
    )

    options = CLIOptions(
        site="naver_finance",
        datasets=(
            "forum_post",
        ),
        flush_at_end=True,
        json_output=False,
        recovery_only=False,
        instruments=(
            "005930",
        ),
        max_pages=None,
    )

    context = (
        make_crawl_context(
            options
        )
    )

    assert (
        "forum_max_pages"
        not in context.extra
    )

    
