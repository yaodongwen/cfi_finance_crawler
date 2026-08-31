from __future__ import annotations

import os

from dataclasses import (
    dataclass,
    field,
)
from pathlib import Path
from typing import (
    Callable,
    Protocol,
)

from crawl_framework.cli.main import (
    CLIOptions,
)

from crawl_framework.core.bootstrap import (
    BootstrapConfig,
    CrawlBootstrap,
)

from crawl_framework.core.concurrency import (
    QueueSizeConfig,
    StageConcurrencyConfig,
)

from crawl_framework.core.concurrent_runtime import (
    ConcurrentProductionRuntime,
)

from crawl_framework.core.plugin import (
    SitePlugin,
    CrawlContext,
)

from crawl_framework.core.runtime import (
    CrawlRuntime,
)

from crawl_framework.storage.buffer import (
    BufferConfig,
    RecordBuffer,
)

from crawl_framework.storage.checkpoint import (
    FileCheckpointStore,
)

from crawl_framework.storage.cleaner import (
    Cleaner,
)

from crawl_framework.storage.parquet_writer import (
    ParquetWriter,
)

from crawl_framework.core.pipeline import (
    StoragePipeline,
)

from crawl_framework.storage.postgres import (
    PostgresCatalog,
)

from crawl_framework.storage.recovery import (
    RecoveryManager,
    RecoveryStore,
)

from crawl_framework.storage.recovery_orchestrator import (
    RecoveryOrchestrator,
    StartupRecoveryPolicy,
)

from crawl_framework.storage.record_index import (
    RecordIndexStore,
)

from crawl_framework.storage.retry_policy import (
    RetryPolicy,
)

from crawl_framework.storage.seen_store import (
    SQLiteSeenStore,
)

from crawl_framework.storage.uploader import (
    RsyncUploader,
    BaseUploader,
    LocalUploader,
)

def make_crawl_context(
    options: CLIOptions,
) -> CrawlContext:
    """
    根据 CLIOptions 创建本次运行的 CrawlContext。

    CLI 层负责接收：

        --instrument
        --max-pages

    SitePlugin 负责解释这些通用运行参数。

    例如：

        --instrument 005930
        --max-pages 3

    将形成：

        CrawlContext(
            extra={
                "instrument_codes": (
                    "005930",
                ),
                "forum_max_pages": 3,
            }
        )

    注意：

        forum_max_pages 只在用户显式指定
        --max-pages 时写入。

        如果没有指定，则让插件继续使用：

            default_forum_max_pages
    """

    instruments = (
        tuple(
            options.instruments
        )
        if options.instruments
        else ()
    )

    extra = {
        "instrument_codes":
            instruments,
    }

    # ========================================================
    # Optional max pages override
    # ========================================================

    if (
        options.max_pages
        is not None
    ):

        extra[
            "forum_max_pages"
        ] = int(
            options.max_pages
        )

    return CrawlContext(
        extra=extra
    )
    
# ============================================================
# Errors
# ============================================================


class AppFactoryError(
    RuntimeError
):
    """
    application composition root error。
    """


class UnknownSiteError(
    AppFactoryError
):
    """
    没有注册指定 site plugin。
    """


class PostgresNotConfiguredError(
    AppFactoryError
):
    """
    当前没有配置 PostgreSQL connection factory。
    """


# ============================================================
# Protocols
# ============================================================


class SitePluginFactory(
    Protocol
):

    def __call__(
        self,
    ) -> SitePlugin:
        ...


class ConnectionFactory(
    Protocol
):

    def __call__(
        self,
    ):
        ...


# ============================================================
# Configuration
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class AppPaths:
    """
    crawl_framework 所有本地运行目录。
    """

    root: Path

    state: Path

    warehouse: Path

    remote: Path

    recovery: Path

    checkpoints: Path

    seen_db: Path


    @classmethod
    def from_root(
        cls,
        root: str | Path,
    ) -> "AppPaths":

        root = Path(
            root
        ).expanduser().resolve()

        state = (
            root
            / "state"
        )

        warehouse = (
            root
            / "warehouse"
        )

        remote = (
            root
            / "remote"
        )

        return cls(
            root=root,
            state=state,
            warehouse=warehouse,
            remote=remote,
            recovery=(
                state
                / "recovery"
            ),
            checkpoints=(
                state
                / "checkpoints"
            ),
            seen_db=(
                state
                / "seen.sqlite3"
            ),
        )


    def ensure(
        self,
    ) -> None:

        self.state.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.warehouse.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.remote.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.recovery.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.checkpoints.mkdir(
            parents=True,
            exist_ok=True,
        )


@dataclass(
    frozen=True,
    slots=True,
)
class AppConfig:
    """
    全局 AppFactory 配置。
    """

    paths: AppPaths

    buffer_target_bytes: int = (
        256
        * 1024
        * 1024
    )

    buffer_min_rows: int = 10_000

    buffer_max_rows: int = 500_000

    buffer_flush_seconds: float = 30.0

    recovery_max_retries: int = 3

    recovery_block_on_failed: bool = True

    recovery_block_on_terminal_failed: bool = False

    recovery_block_on_remaining_pending: bool = False

    stage_concurrency: StageConcurrencyConfig = field(
        default_factory=StageConcurrencyConfig
    )

    queue_sizes: QueueSizeConfig = field(
        default_factory=QueueSizeConfig
    )


# ============================================================
# Site registry
# ============================================================


class SiteFactoryRegistry:
    """
    site_id -> SitePluginFactory

    这里故意不直接 import：

        naver_finance
        tossinvest
        hotcopper
        ...

    避免 core/app 层依赖具体网站。
    """

    def __init__(
        self,
    ) -> None:

        self._factories: dict[
            str,
            SitePluginFactory,
        ] = {}


    def register(
        self,
        site_id: str,
        factory: SitePluginFactory,
    ) -> None:

        key = str(
            site_id
        ).strip()

        if not key:

            raise ValueError(
                "site_id cannot be empty"
            )

        if key in self._factories:

            raise ValueError(
                f"site already registered: {key}"
            )

        self._factories[
            key
        ] = factory


    def replace(
        self,
        site_id: str,
        factory: SitePluginFactory,
    ) -> None:

        key = str(
            site_id
        ).strip()

        if not key:

            raise ValueError(
                "site_id cannot be empty"
            )

        self._factories[
            key
        ] = factory


    def contains(
        self,
        site_id: str,
    ) -> bool:

        return (
            str(
                site_id
            ).strip()
            in self._factories
        )


    def create(
        self,
        site_id: str,
    ) -> SitePlugin:

        key = str(
            site_id
        ).strip()

        factory = (
            self._factories.get(
                key
            )
        )

        if factory is None:

            available = (
                ", ".join(
                    sorted(
                        self._factories
                    )
                )
                or "<none>"
            )

            raise UnknownSiteError(
                f"unknown site: {key!r}; "
                f"registered sites: {available}"
            )

        plugin = factory()

        if not isinstance(
            plugin,
            SitePlugin,
        ):

            raise TypeError(
                "site factory did not return "
                "a SitePlugin: "
                f"{key!r}"
            )

        return plugin


    def list_sites(
        self,
    ) -> tuple[
        str,
        ...,
    ]:

        return tuple(
            sorted(
                self._factories
            )
        )


# ============================================================
# Default global registry
# ============================================================


SITE_FACTORIES = (
    SiteFactoryRegistry()
)


def register_site(
    site_id: str,
    factory: SitePluginFactory,
) -> None:
    """
    注册一个 site plugin factory。
    """

    SITE_FACTORIES.register(
        site_id,
        factory,
    )


# ============================================================
# AppFactory
# ============================================================


class AppFactory:
    """
    整个 crawler framework 的 composition root。

    负责组装：

        SitePlugin
        SeenStore
        CheckpointStore
        RecordBuffer
        ParquetWriter
        Uploader
        PostgreSQL catalog
        RecoveryStore
        RetryPolicy
        Cleaner
        RecordIndexStore
        StoragePipeline
        RecoveryManager
        RecoveryOrchestrator
        CrawlRuntime
        CrawlBootstrap
    """

    def __init__(
        self,
        *,
        config: AppConfig,
        site_registry: SiteFactoryRegistry | None = None,
        connection_factory: ConnectionFactory | None = None,
        uploader_factory: (
            Callable[
                [
                    AppConfig
                ],
                BaseUploader,
            ]
            | None
        ) = None,
    ) -> None:

        self.config = config

        self.site_registry = (
            site_registry
            or SITE_FACTORIES
        )

        self.connection_factory = (
            connection_factory
        )

        self.uploader_factory = (
            uploader_factory
        )


    def build(
        self,
        options: CLIOptions,
    ) -> CrawlBootstrap:

        # ====================================================
        # 1. Ensure directories
        # ====================================================

        self.config.paths.ensure()

        # ====================================================
        # 2. Plugin
        # ====================================================

        plugin = (
            self.site_registry
            .create(
                options.site
            )
        )

        # ====================================================
        # 3. SeenStore
        # ====================================================

        seen_store = (
            SQLiteSeenStore(
                self.config
                .paths
                .seen_db
            )
        )

        # ====================================================
        # 4. Checkpoints
        # ====================================================

        checkpoint_store = (
            FileCheckpointStore(
                self.config
                .paths
                .checkpoints
            )
        )

        # ====================================================
        # 5. Buffer
        # ====================================================

        buffer = RecordBuffer(
            config=BufferConfig(
                target_bytes=(
                    self.config
                    .buffer_target_bytes
                ),
                min_rows=(
                    self.config
                    .buffer_min_rows
                ),
                max_rows=(
                    self.config
                    .buffer_max_rows
                ),
                flush_seconds=(
                    self.config
                    .buffer_flush_seconds
                ),
            )
        )

        # ====================================================
        # 6. Local parquet
        # ====================================================

        parquet_writer = (
            ParquetWriter(
                self.config
                .paths
                .warehouse
            )
        )

        # ====================================================
        # 7. Record sidecar
        # ====================================================

        record_index_store = (
            RecordIndexStore()
        )

        # ====================================================
        # 8. Uploader
        # ====================================================

        if (
            self.uploader_factory
            is not None
        ):

            uploader = (
                self.uploader_factory(
                    self.config
                )
            )

        else:

            # 第一版默认使用 LocalUploader。
            #
            # 下一阶段服务器配置再切 RsyncUploader。
            uploader = (
                LocalUploader(
                    self.config
                    .paths
                    .remote,
                    verify_size=True,
                    verify_sha256=True,
                )
            )

        # ====================================================
        # 9. PostgreSQL
        # ====================================================

        if (
            self.connection_factory
            is None
        ):

            raise (
                PostgresNotConfiguredError(
                    "PostgreSQL connection factory "
                    "is not configured"
                )
            )

        # ----------------------------------------------------
        # Create PostgreSQL connection
        # ----------------------------------------------------

        connection = (
            self.connection_factory()
        )

        # ----------------------------------------------------
        # Create catalog
        # ----------------------------------------------------

        catalog = (
            PostgresCatalog(
                connection
            )
        )

        # ----------------------------------------------------
        # Initialize database schema
        #
        # initialize_schema() 必须设计为幂等操作：
        #
        #     CREATE SCHEMA IF NOT EXISTS
        #     CREATE TABLE IF NOT EXISTS
        #
        # 因此每次 crawler 启动时调用都是安全的。
        #
        # 这样新环境第一次运行时无需手工建表，
        # 已存在数据库再次启动也不会破坏旧数据。
        # ----------------------------------------------------

        catalog.initialize_schema()


        # ====================================================
        # 10. Recovery state
        # ====================================================

        recovery_store = (
            RecoveryStore(
                self.config
                .paths
                .recovery
            )
        )

        retry_policy = (
            RetryPolicy(
                max_retries=(
                    self.config
                    .recovery_max_retries
                )
            )
        )

        # ====================================================
        # 11. Cleaner
        # ====================================================

        cleaner = Cleaner(
            record_index_store=(
                record_index_store
            )
        )

        # ====================================================
        # 12. StoragePipeline
        # ====================================================

        pipeline = StoragePipeline(
            seen_store=seen_store,
            buffer=buffer,
            parquet_writer=(
                parquet_writer
            ),
            uploader=uploader,
            catalog=catalog,
            recovery_store=(
                recovery_store
            ),
            cleaner=cleaner,
            checkpoint_store=(
                checkpoint_store
            ),
            record_index_store=(
                record_index_store
            ),
        )

        # ====================================================
        # 13. RecoveryManager
        # ====================================================

        recovery_manager = (
            RecoveryManager(
                store=recovery_store,
                uploader=uploader,
                catalog=catalog,
                cleaner=cleaner,
                seen_store=seen_store,
                record_index_reader=(
                    record_index_store
                    .reader
                ),
                retry_policy=(
                    retry_policy
                ),
            )
        )

        # ====================================================
        # 14. RecoveryOrchestrator
        # ====================================================

        orchestrator = (
            RecoveryOrchestrator(
                manager=(
                    recovery_manager
                ),
                policy=(
                    StartupRecoveryPolicy(
                        block_on_failed=(
                            self.config
                            .recovery_block_on_failed
                        ),
                        block_on_terminal_failed=(
                            self.config
                            .recovery_block_on_terminal_failed
                        ),
                        block_on_remaining_pending=(
                            self.config
                            .recovery_block_on_remaining_pending
                        ),
                    )
                ),
            )
        )

        # ====================================================
        # 15. Runtime
        # ====================================================

        crawl_context = make_crawl_context(
            options
        )

        concurrency = (
            self.config
            .stage_concurrency
        )

        queues = (
            self.config
            .queue_sizes
        )

        if _use_concurrent_production_runtime(
            concurrency
        ):

            runtime = ConcurrentProductionRuntime(
                plugin=plugin,
                pipeline=pipeline,
                checkpoint_store=checkpoint_store,
                context=crawl_context,
                crawl_workers=(
                    concurrency.crawl_workers
                ),
                writer_workers=(
                    concurrency.writer_workers
                ),
                upload_workers=(
                    concurrency.upload_workers
                ),
                catalog_workers=(
                    concurrency.catalog_workers
                ),
                record_queue_size=(
                    queues.records
                ),
                upload_queue_size=(
                    queues.uploads
                ),
                catalog_queue_size=(
                    queues.catalog
                ),
            )

        else:

            runtime = CrawlRuntime(
                plugin=plugin,
                pipeline=pipeline,
                checkpoint_store=checkpoint_store,
                context=crawl_context,
            )

        # ====================================================
        # 16. Bootstrap
        # ====================================================

        return CrawlBootstrap(
            recovery_orchestrator=(
                orchestrator
            ),
            runtime=runtime,
            config=BootstrapConfig(
                flush_at_end=(
                    options.flush_at_end
                ),
                datasets=(
                    options.datasets
                ),
            ),
        )


# ============================================================
# Runtime selection
# ============================================================


def _use_concurrent_production_runtime(
    concurrency: StageConcurrencyConfig,
) -> bool:
    """
    Production runtime selector.

    Keep the legacy single-worker runtime for single-scope compatibility tests
    and opt into the bounded staged runner when any production stage is
    configured for parallel work.
    """

    return any(
        worker_count > 1
        for worker_count in (
            concurrency.crawl_workers,
            concurrency.writer_workers,
            concurrency.upload_workers,
            concurrency.catalog_workers,
        )
    )


# ============================================================
# Environment configuration
# ============================================================


def default_app_root() -> Path:
    """
    默认把运行目录放在项目 cwd。

    可通过：

        CRAWL_FRAMEWORK_ROOT

    覆盖。
    """

    value = os.environ.get(
        "CRAWL_FRAMEWORK_ROOT"
    )

    if value:

        return Path(
            value
        ).expanduser().resolve()

    return Path.cwd()


def default_app_config() -> AppConfig:

    return AppConfig(
        paths=(
            AppPaths.from_root(
                default_app_root()
            )
        )
    )


# ============================================================
# Default factory holder
# ============================================================


_default_connection_factory: (
    ConnectionFactory
    | None
) = None


def configure_connection_factory(
    factory: ConnectionFactory,
) -> None:
    """
    配置默认 PostgreSQL connection factory。

    下一步将用环境变量创建真正 psycopg connection。
    """

    global _default_connection_factory

    _default_connection_factory = (
        factory
    )

def make_config_uploader_factory(
    framework_config,
):
    """
    根据 config.yaml 创建 uploader factory。

    支持：

        sync.method: auto
        sync.method: rsync

    当前规则：

        auto + rsync.enabled=true
            -> RsyncUploader

        rsync
            -> RsyncUploader

    远端目标：

        {server.user}@{server.host}:
        {storage.server_warehouse}
    """

    def factory(
        app_config: AppConfig,
    ):
        method = str(
            framework_config.sync.method
        ).strip().lower()

        rsync_enabled = bool(
            framework_config
            .sync
            .rsync_enabled
        )

        # ====================================================
        # Decide uploader
        # ====================================================

        use_rsync = False

        if method == "rsync":

            use_rsync = True

        elif method == "auto":

            use_rsync = (
                rsync_enabled
            )

        else:

            raise AppFactoryError(
                "unsupported sync.method: "
                f"{method!r}"
            )

        # ====================================================
        # Rsync uploader
        # ====================================================

        if use_rsync:

            return RsyncUploader(
                remote_host=(
                    framework_config
                    .server
                    .host
                ),
                remote_user=(
                    framework_config
                    .server
                    .user
                ),
                remote_root=(
                    framework_config
                    .storage
                    .server_warehouse
                ),
                ssh_port=(
                    framework_config
                    .sync
                    .ssh_port
                ),
                dry_run=False,
                verify_size=True,

                # 第一阶段先关闭远端 SHA256。
                #
                # 远端 Linux 虽然通常有 sha256sum，
                # 但我们先确认真实上传链路稳定。
                verify_sha256=False,
            )

        # ====================================================
        # Fallback
        # ====================================================

        return LocalUploader(
            app_config.paths.remote,
            verify_size=True,
            verify_sha256=True,
        )

    return factory

def build_default_bootstrap(
    options: CLIOptions,
) -> CrawlBootstrap:
    """
    CLI 默认 Bootstrap factory。

    默认读取：

        ./config.yaml

    不再要求使用环境变量配置 PostgreSQL。

    config.yaml 提供：

        PostgreSQL
        local warehouse
        remote server
        remote warehouse
        sync settings
    """

    # ========================================================
    # 1. Load YAML config
    # ========================================================

    from crawl_framework.config import (
        load_default_config,
    )

    framework_config = (
        load_default_config()
    )

    # ========================================================
    # 2. Register built-in sites
    # ========================================================

    from crawl_framework.sites.builtin import (
        register_builtin_sites,
    )

    register_builtin_sites(
        SITE_FACTORIES
    )

    # ========================================================
    # 3. PostgreSQL
    # ========================================================

    if (
        _default_connection_factory
        is not None
    ):

        connection_factory = (
            _default_connection_factory
        )

    else:

        from crawl_framework.storage.postgres_connection import (
            make_config_postgres_connection_factory,
        )

        connection_factory = (
            make_config_postgres_connection_factory(
                framework_config
            )
        )

    # ========================================================
    # 4. Application paths
    # ========================================================

    project_root = (
        framework_config
        .config_path
        .parent
        .parent
    )


    app_paths = AppPaths(
        root=project_root,

        state=(
            project_root
            / "state"
        ),

        warehouse=(
            framework_config
            .storage
            .local_warehouse
        ),

        remote=(
            project_root
            / "remote"
        ),

        recovery=(
            project_root
            / "state"
            / "recovery"
        ),

        checkpoints=(
            project_root
            / "state"
            / "checkpoints"
        ),

        seen_db=(
            project_root
            / "state"
            / "seen.sqlite3"
        ),
    )

    app_config = AppConfig(
        paths=app_paths,
        buffer_target_bytes=(
            (
                options.target_file_size_mb
                or framework_config
                .storage
                .target_file_size_mb
            )
            *
            1024
            *
            1024
        ),
        buffer_max_rows=(
            framework_config
            .storage
            .max_rows
        ),
        buffer_flush_seconds=(
            framework_config
            .storage
            .max_buffer_age_seconds
        ),
        stage_concurrency=(
            StageConcurrencyConfig(
                crawl_workers=(
                    options.crawl_workers
                    or framework_config.runtime.crawl_workers
                ),
                http_concurrency=(
                    options.http_concurrency
                    or framework_config.runtime.http_concurrency
                ),
                attachment_workers=(
                    options.attachment_workers
                    or framework_config.runtime.attachment_workers
                ),
                writer_workers=(
                    options.writer_workers
                    or framework_config.runtime.writer_workers
                ),
                upload_workers=(
                    options.upload_workers
                    or framework_config.runtime.upload_workers
                ),
                catalog_workers=(
                    options.catalog_workers
                    or framework_config.runtime.catalog_workers
                ),
            )
        ),
        queue_sizes=(
            framework_config
            .queues
        ),
    )

    # ========================================================
    # 5. Uploader factory
    # ========================================================

    uploader_factory = (
        make_config_uploader_factory(
            framework_config
        )
    )

    # ========================================================
    # 6. AppFactory
    # ========================================================

    factory = AppFactory(
        config=app_config,
        site_registry=(
            SITE_FACTORIES
        ),
        connection_factory=(
            connection_factory
        ),
        uploader_factory=(
            uploader_factory
        ),
    )

    return factory.build(
        options
    )

    """
    CLI 默认 Bootstrap factory。

    启动顺序：

        1. 注册 framework 内置网站插件
        2. 构建 PostgreSQL connection factory
        3. 构建 AppFactory
        4. 创建 CrawlBootstrap

    PostgreSQL connection factory 优先级：

        1. configure_connection_factory()
           显式配置

        2. 环境变量：

           CRAWL_PG_DSN

           或

           CRAWL_PG_HOST
           CRAWL_PG_PORT
           CRAWL_PG_DATABASE
           CRAWL_PG_USER
           CRAWL_PG_PASSWORD
    """

    # ========================================================
    # 1. Register built-in sites
    # ========================================================

    from crawl_framework.sites.builtin import (
        register_builtin_sites,
    )

    register_builtin_sites(
        SITE_FACTORIES
    )

    # ========================================================
    # 2. PostgreSQL connection factory
    # ========================================================

    connection_factory = (
        _default_connection_factory
    )

    if connection_factory is None:

        from crawl_framework.storage.postgres_connection import (
            make_env_postgres_connection_factory,
        )

        connection_factory = (
            make_env_postgres_connection_factory()
        )

    # ========================================================
    # 3. Application factory
    # ========================================================

    factory = AppFactory(
        config=(
            default_app_config()
        ),
        site_registry=(
            SITE_FACTORIES
        ),
        connection_factory=(
            connection_factory
        ),
    )

    # ========================================================
    # 4. Bootstrap
    # ========================================================

    return factory.build(
        options
    )
    """
    CLI 默认 Bootstrap factory。

    PostgreSQL connection factory 的选择顺序：

        1. 如果程序显式调用过
           configure_connection_factory()
           则使用显式 factory。

        2. 否则自动从环境变量创建 PostgreSQL factory。

    环境变量支持：

        CRAWL_PG_DSN

    或：

        CRAWL_PG_HOST
        CRAWL_PG_PORT
        CRAWL_PG_DATABASE
        CRAWL_PG_USER
        CRAWL_PG_PASSWORD
    """

    connection_factory = (
        _default_connection_factory
    )

    if connection_factory is None:

        from crawl_framework.storage.postgres_connection import (
            make_env_postgres_connection_factory,
        )

        connection_factory = (
            make_env_postgres_connection_factory()
        )

    factory = AppFactory(
        config=(
            default_app_config()
        ),
        site_registry=(
            SITE_FACTORIES
        ),
        connection_factory=(
            connection_factory
        ),
    )

    return factory.build(
        options
    )
    """
    CLI 默认使用的 bootstrap factory。
    """

    factory = AppFactory(
        config=(
            default_app_config()
        ),
        site_registry=(
            SITE_FACTORIES
        ),
        connection_factory=(
            _default_connection_factory
        ),
    )

    return factory.build(
        options
    )
