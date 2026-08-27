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
    make_crawl_context,
    make_config_uploader_factory,
)

from crawl_framework.cli.main import (
    CLIOptions,
)

from crawl_framework.core.models import (
    CanonicalRecord,
)

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
        
# ============================================================
# Helpers
# ============================================================


def options(
    *,
    site="demo",
):

    return CLIOptions(
        site=site,
        datasets=None,
        flush_at_end=True,
        json_output=False,
        recovery_only=False,
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

    