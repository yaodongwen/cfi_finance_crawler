from __future__ import annotations

from dataclasses import (
    dataclass,
    field,
)
from pathlib import Path
from typing import Any

import yaml

from crawl_framework.core.concurrency import (
    QueueSizeConfig,
    StageConcurrencyConfig,
)


# ============================================================
# Errors
# ============================================================


class ConfigError(
    RuntimeError
):
    """
    crawl framework 配置错误。
    """


# ============================================================
# PostgreSQL
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class PostgresConfig:

    host: str

    port: int

    database: str

    user: str

    password: str | None = None


# ============================================================
# Server
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class ServerConfig:

    host: str

    user: str

    data_dir: str


# ============================================================
# Storage
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class StorageConfig:

    local_warehouse: Path

    server_warehouse: str

    max_rows: int = 50_000

    target_file_size_mb: int = 256

    max_buffer_age_seconds: float = 30.0

    compression: str = "zstd"


# ============================================================
# Sync
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class SyncConfig:

    delete_after_upload: bool = True

    method: str = "auto"

    rsync_enabled: bool = True

    ssh_port: int = 22

    ssh_multiplex: bool = False

    ssh_control_path: str | None = None

    ssh_control_persist: str = "10m"


# ============================================================
# Local
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class LocalConfig:

    output_dir: Path

    warehouse_dir: Path

    index_cache_dir: Path


@dataclass(frozen=True, slots=True)
class BrowserConfig:
    workers: int = 2
    headless: bool = True
    profile_root: Path | None = None
    locale: str = "ko-KR"
    timezone_id: str = "Asia/Seoul"
    page_timeout_ms: int = 30_000
    navigation_timeout_ms: int = 45_000
    recycle_after_scopes: int = 50
    proxies: tuple[str, ...] = ()
    proxy_strategy: str = "round_robin"
    proxy_failure_cooldown_seconds: float = 30.0
    worker_budgets: tuple[tuple[str, int], ...] = ()


@dataclass(frozen=True, slots=True)
class HttpConfig:
    timeout_seconds: float = 20.0
    max_attempts: int = 5
    retry_backoff_seconds: float = 0.5
    proxies: tuple[str, ...] = ()
    proxy_strategy: str = "round_robin"
    proxy_failure_cooldown_seconds: float = 30.0
    base_delay_seconds: float = 0.0
    throttle_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    failure_threshold: int = 3


# ============================================================
# Root config
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class FrameworkConfig:

    config_path: Path

    local: LocalConfig

    server: ServerConfig

    postgres: PostgresConfig

    storage: StorageConfig

    sync: SyncConfig

    runtime: StageConcurrencyConfig = field(
        default_factory=StageConcurrencyConfig
    )

    queues: QueueSizeConfig = field(
        default_factory=QueueSizeConfig
    )

    browser: BrowserConfig = field(default_factory=BrowserConfig)

    http: HttpConfig = field(default_factory=HttpConfig)


# ============================================================
# Loader
# ============================================================


def load_config(
    path: str | Path,
) -> FrameworkConfig:
    """
    加载 config.yaml。

    相对路径全部相对于 config.yaml 所在目录解析。
    """

    config_path = (
        Path(path)
        .expanduser()
        .resolve()
    )

    if not config_path.exists():

        raise ConfigError(
            f"config file not found: "
            f"{config_path}"
        )

    try:

        with config_path.open(
            "r",
            encoding="utf-8",
        ) as f:

            raw = (
                yaml.safe_load(
                    f
                )
                or {}
            )

    except Exception as exc:

        raise ConfigError(
            f"failed to load config: "
            f"{config_path}: {exc}"
        ) from exc

    if not isinstance(
        raw,
        dict,
    ):

        raise ConfigError(
            "config root must be a mapping"
        )

    base_dir = (
        config_path.parent
    )

    # ========================================================
    # Local
    # ========================================================

    local_raw = _mapping(
        raw,
        "local",
    )

    local = LocalConfig(
        output_dir=_resolve_path(
            base_dir,
            _required(
                local_raw,
                "output_dir",
            ),
        ),
        warehouse_dir=_resolve_path(
            base_dir,
            _required(
                local_raw,
                "warehouse_dir",
            ),
        ),
        index_cache_dir=_resolve_path(
            base_dir,
            _required(
                local_raw,
                "index_cache_dir",
            ),
        ),
    )

    # ========================================================
    # Server
    # ========================================================

    server_raw = _mapping(
        raw,
        "server",
    )

    server = ServerConfig(
        host=str(
            _required(
                server_raw,
                "host",
            )
        ).strip(),
        user=str(
            _required(
                server_raw,
                "user",
            )
        ).strip(),
        data_dir=str(
            _required(
                server_raw,
                "data_dir",
            )
        ).strip(),
    )

    # ========================================================
    # PostgreSQL
    # ========================================================

    postgres_raw = _mapping(
        raw,
        "postgres",
    )

    postgres = PostgresConfig(
        host=str(
            _required(
                postgres_raw,
                "host",
            )
        ).strip(),
        port=int(
            postgres_raw.get(
                "port",
                5432,
            )
        ),
        database=str(
            _required(
                postgres_raw,
                "database",
            )
        ).strip(),
        user=str(
            _required(
                postgres_raw,
                "user",
            )
        ).strip(),
        password=(
            str(
                postgres_raw[
                    "password"
                ]
            )
            if postgres_raw.get(
                "password"
            )
            is not None
            else None
        ),
    )

    # ========================================================
    # Storage
    # ========================================================

    storage_raw = _mapping(
        raw,
        "storage",
    )

    partition_raw = (
        storage_raw.get(
            "partition",
            {}
        )
        or {}
    )

    compression_raw = (
        storage_raw.get(
            "compression",
            {}
        )
        or {}
    )

    storage = StorageConfig(
        local_warehouse=_resolve_path(
            base_dir,
            storage_raw.get(
                "local_warehouse",
                local.warehouse_dir,
            ),
        ),
        server_warehouse=str(
            storage_raw.get(
                "server_warehouse",
                server.data_dir,
            )
        ).strip(),
        max_rows=int(
            storage_raw.get(
                "max_rows_per_file",
                partition_raw.get(
                    "max_rows",
                    50_000,
                ),
            )
        ),
        target_file_size_mb=int(
            storage_raw.get(
                "target_file_size_mb",
                256,
            )
        ),
        max_buffer_age_seconds=float(
            storage_raw.get(
                "max_buffer_age_seconds",
                partition_raw.get(
                    "max_buffer_age_seconds",
                    30.0,
                ),
            )
        ),
        compression=str(
            compression_raw.get(
                "codec",
                "zstd",
            )
        ).strip(),
    )

    # ========================================================
    # Sync
    # ========================================================

    sync_raw = (
        raw.get(
            "sync",
            {}
        )
        or {}
    )

    rsync_raw = (
        sync_raw.get(
            "rsync",
            {}
        )
        or {}
    )

    sync = SyncConfig(
        delete_after_upload=bool(
            sync_raw.get(
                "delete_after_upload",
                True,
            )
        ),
        method=str(
            sync_raw.get(
                "method",
                "auto",
            )
        ).strip(),
        rsync_enabled=bool(
            rsync_raw.get(
                "enabled",
                True,
            )
        ),
        ssh_port=int(
            rsync_raw.get(
                "ssh_port",
                22,
            )
        ),
        ssh_multiplex=bool(
            rsync_raw.get(
                "ssh_multiplex",
                False,
            )
        ),
        ssh_control_path=(
            str(
                rsync_raw.get(
                    "ssh_control_path"
                )
            ).strip()
            if rsync_raw.get(
                "ssh_control_path"
            )
            is not None
            else None
        ),
        ssh_control_persist=str(
            rsync_raw.get(
                "ssh_control_persist",
                "10m",
            )
        ).strip(),
    )

    # ========================================================
    # Runtime / queues
    # ========================================================

    runtime_raw = (
        raw.get(
            "runtime",
            {},
        )
        or {}
    )

    http_raw = (
        raw.get(
            "http",
            {},
        )
        or {}
    )

    runtime = StageConcurrencyConfig(
        crawl_workers=int(
            runtime_raw.get(
                "crawl_workers",
                1,
            )
        ),
        http_concurrency=int(
            http_raw.get(
                "concurrency",
                runtime_raw.get(
                    "http_concurrency",
                    1,
                ),
            )
        ),
        attachment_workers=int(
            runtime_raw.get(
                "attachment_workers",
                1,
            )
        ),
        writer_workers=int(
            runtime_raw.get(
                "writer_workers",
                1,
            )
        ),
        upload_workers=int(
            runtime_raw.get(
                "upload_workers",
                1,
            )
        ),
        catalog_workers=int(
            runtime_raw.get(
                "catalog_workers",
                1,
            )
        ),
    )

    queues_raw = (
        raw.get(
            "queues",
            {},
        )
        or {}
    )

    queues = QueueSizeConfig(
        records=int(
            queues_raw.get(
                "records",
                1000,
            )
        ),
        durable_files=int(
            queues_raw.get(
                "durable_files",
                128,
            )
        ),
        uploads=int(
            queues_raw.get(
                "uploads",
                128,
            )
        ),
        catalog=int(
            queues_raw.get(
                "catalog",
                128,
            )
        ),
        attachments=int(
            queues_raw.get(
                "attachments",
                128,
            )
        ),
    )

    browser_raw = raw.get("browser", {}) or {}
    viewport_raw = browser_raw.get("viewport", {}) or {}
    del viewport_raw
    browser = BrowserConfig(
        workers=int(browser_raw.get("workers", 2)),
        headless=bool(browser_raw.get("headless", True)),
        profile_root=_resolve_path(base_dir, browser_raw["profile_root"])
        if browser_raw.get("profile_root") else None,
        locale=str(browser_raw.get("locale", "ko-KR")),
        timezone_id=str(browser_raw.get("timezone_id", "Asia/Seoul")),
        page_timeout_ms=int(browser_raw.get("page_timeout_ms", 30_000)),
        navigation_timeout_ms=int(browser_raw.get("navigation_timeout_ms", 45_000)),
        recycle_after_scopes=int(browser_raw.get("recycle_after_scopes", 50)),
        proxies=tuple(str(value) for value in browser_raw.get("proxies", ())),
        proxy_strategy=str(browser_raw.get("proxy_strategy", "round_robin")),
        proxy_failure_cooldown_seconds=float(
            browser_raw.get("proxy_failure_cooldown_seconds", 30.0)
        ),
        worker_budgets=tuple(
            (str(name), int(count))
            for name, count in (browser_raw.get("budgets", {}) or {}).items()
        ),
    )

    http_raw = raw.get("http", {}) or {}
    http = HttpConfig(
        timeout_seconds=float(http_raw.get("timeout_seconds", 20.0)),
        max_attempts=int(http_raw.get("max_attempts", 5)),
        retry_backoff_seconds=float(http_raw.get("retry_backoff_seconds", 0.5)),
        proxies=tuple(str(value) for value in http_raw.get("proxies", ())),
        proxy_strategy=str(http_raw.get("proxy_strategy", "round_robin")),
        proxy_failure_cooldown_seconds=float(
            http_raw.get("proxy_failure_cooldown_seconds", 30.0)
        ),
        base_delay_seconds=float(http_raw.get("base_delay_seconds", 0.0)),
        throttle_delay_seconds=float(http_raw.get("throttle_delay_seconds", 1.0)),
        max_delay_seconds=float(http_raw.get("max_delay_seconds", 60.0)),
        failure_threshold=int(http_raw.get("failure_threshold", 3)),
    )

    # ========================================================
    # Validate
    # ========================================================

    if postgres.port <= 0:

        raise ConfigError(
            "postgres.port must be positive"
        )

    if storage.max_rows <= 0:

        raise ConfigError(
            "storage.partition.max_rows "
            "must be positive"
        )

    if storage.target_file_size_mb <= 0:

        raise ConfigError(
            "storage.target_file_size_mb "
            "must be positive"
        )

    if storage.max_buffer_age_seconds <= 0:

        raise ConfigError(
            "storage.max_buffer_age_seconds "
            "must be positive"
        )

    if sync.ssh_port <= 0:

        raise ConfigError(
            "sync.rsync.ssh_port "
            "must be positive"
        )

    if http.timeout_seconds <= 0:
        raise ConfigError("http.timeout_seconds must be positive")
    if http.max_attempts < 1:
        raise ConfigError("http.max_attempts must be >= 1")
    if http.retry_backoff_seconds < 0:
        raise ConfigError("http.retry_backoff_seconds must be >= 0")

    return FrameworkConfig(
        config_path=config_path,
        local=local,
        server=server,
        postgres=postgres,
        storage=storage,
        sync=sync,
        runtime=runtime,
        queues=queues,
        browser=browser,
        http=http,
    )


# ============================================================
# Default config
# ============================================================


def find_default_config() -> Path:
    """
    默认寻找配置文件。

    优先级：

        1. ./config/config.yaml
        2. ./config/config.yml
        3. ./config.yaml
        4. ./config.yml

    这样既支持正式目录结构：

        crawl_framework/
        └── config/
            └── config.yaml

    也兼容旧的根目录配置文件。
    """

    cwd = Path.cwd()

    candidates = (
        cwd / "config" / "config.yaml",
        cwd / "config" / "config.yml",
        cwd / "config.yaml",
        cwd / "config.yml",
    )

    for path in candidates:

        if path.exists():

            return path.resolve()

    searched = "\n".join(
        f"  - {path}"
        for path in candidates
    )

    raise ConfigError(
        "config file not found. "
        "searched:\n"
        f"{searched}"
    )

def load_default_config() -> FrameworkConfig:

    return load_config(
        find_default_config()
    )


# ============================================================
# Helpers
# ============================================================


def _mapping(
    raw: dict[
        str,
        Any,
    ],
    key: str,
) -> dict[
    str,
    Any,
]:

    value = raw.get(
        key
    )

    if not isinstance(
        value,
        dict,
    ):

        raise ConfigError(
            f"config section "
            f"{key!r} must be a mapping"
        )

    return value


def _required(
    raw: dict[
        str,
        Any,
    ],
    key: str,
) -> Any:

    if key not in raw:

        raise ConfigError(
            f"missing config value: "
            f"{key}"
        )

    value = raw[
        key
    ]

    if value is None:

        raise ConfigError(
            f"config value "
            f"{key!r} cannot be null"
        )

    return value


def _resolve_path(
    base_dir: Path,
    value: Any,
) -> Path:

    path = Path(
        str(
            value
        )
    ).expanduser()

    if not path.is_absolute():

        path = (
            base_dir
            / path
        )

    return path.resolve()
