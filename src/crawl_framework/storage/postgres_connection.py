from __future__ import annotations

import os

from dataclasses import dataclass
from typing import Callable


# ============================================================
# Errors
# ============================================================


class PostgresConfigurationError(
    RuntimeError
):
    """
    PostgreSQL 配置错误。
    """


class PostgresDriverNotInstalledError(
    RuntimeError
):
    """
    psycopg 未安装。
    """


# ============================================================
# Settings
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class PostgresSettings:
    """
    PostgreSQL 连接配置。

    支持两种方式：

    方式一：
        CRAWL_PG_DSN

    例如：

        postgresql://user:password@host:5432/stock_data

    方式二：
        分字段配置：

        CRAWL_PG_HOST
        CRAWL_PG_PORT
        CRAWL_PG_DATABASE
        CRAWL_PG_USER
        CRAWL_PG_PASSWORD

    DSN 优先级高于分字段配置。
    """

    dsn: str | None = None

    host: str | None = None

    port: int = 5432

    database: str | None = None

    user: str | None = None

    password: str | None = None

    connect_timeout: int = 10


    def __post_init__(
        self,
    ) -> None:

        if self.port <= 0:

            raise ValueError(
                "PostgreSQL port must be positive"
            )

        if self.connect_timeout <= 0:

            raise ValueError(
                "connect_timeout must be positive"
            )


    @classmethod
    def from_env(
        cls,
    ) -> "PostgresSettings":
        """
        从环境变量读取 PostgreSQL 配置。
        """

        dsn = _clean_env(
            "CRAWL_PG_DSN"
        )

        host = _clean_env(
            "CRAWL_PG_HOST"
        )

        database = _clean_env(
            "CRAWL_PG_DATABASE"
        )

        user = _clean_env(
            "CRAWL_PG_USER"
        )

        password = os.environ.get(
            "CRAWL_PG_PASSWORD"
        )

        port = _read_int_env(
            "CRAWL_PG_PORT",
            default=5432,
        )

        connect_timeout = _read_int_env(
            "CRAWL_PG_CONNECT_TIMEOUT",
            default=10,
        )

        return cls(
            dsn=dsn,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            connect_timeout=(
                connect_timeout
            ),
        )


    @property
    def uses_dsn(
        self,
    ) -> bool:

        return bool(
            self.dsn
        )


    def validate(
        self,
    ) -> None:
        """
        检查配置是否足够建立连接。
        """

        if self.dsn:

            return

        missing = []

        if not self.host:

            missing.append(
                "CRAWL_PG_HOST"
            )

        if not self.database:

            missing.append(
                "CRAWL_PG_DATABASE"
            )

        if not self.user:

            missing.append(
                "CRAWL_PG_USER"
            )

        if missing:

            raise (
                PostgresConfigurationError(
                    "PostgreSQL is not configured. "
                    "Set CRAWL_PG_DSN or provide: "
                    + ", ".join(
                        missing
                    )
                )
            )


# ============================================================
# Connection
# ============================================================


def connect_postgres(
    settings: PostgresSettings,
):
    """
    根据 PostgresSettings 创建 psycopg3 connection。

    这里使用 lazy import。

    好处：

        1. 单元测试不需要真的安装/连接 PostgreSQL。
        2. import crawl_framework 时不会立刻加载数据库驱动。
    """

    settings.validate()

    try:

        import psycopg

    except ImportError as exc:

        raise (
            PostgresDriverNotInstalledError(
                "psycopg is not installed. "
                "Install psycopg 3 before using PostgreSQL."
            )
        ) from exc

    # ========================================================
    # DSN mode
    # ========================================================

    if settings.dsn:

        return psycopg.connect(
            settings.dsn,
            connect_timeout=(
                settings.connect_timeout
            ),
        )

    # ========================================================
    # Keyword mode
    # ========================================================

    kwargs = {
        "host": settings.host,
        "port": settings.port,
        "dbname": settings.database,
        "user": settings.user,
        "connect_timeout": (
            settings.connect_timeout
        ),
    }

    # password=None 时不传。
    #
    # 这样可以继续支持：
    #
    # pg_hba trust
    # .pgpass
    # peer / other auth
    if settings.password is not None:

        kwargs[
            "password"
        ] = settings.password

    return psycopg.connect(
        **kwargs
    )


# ============================================================
# Connection factory
# ============================================================


def make_postgres_connection_factory(
    settings: PostgresSettings,
) -> Callable:
    """
    创建可以交给 AppFactory 的 connection_factory。

    注意：

    这里只返回 factory，
    不立即连接数据库。

    真正调用：

        factory()

    时才创建 PostgreSQL connection。
    """

    settings.validate()


    def connection_factory():

        return connect_postgres(
            settings
        )


    return connection_factory


def make_env_postgres_connection_factory() -> Callable:
    """
    使用环境变量构建默认 PostgreSQL connection factory。
    """

    settings = (
        PostgresSettings
        .from_env()
    )

    return (
        make_postgres_connection_factory(
            settings
        )
    )


# ============================================================
# Environment helpers
# ============================================================


def _clean_env(
    name: str,
) -> str | None:

    value = os.environ.get(
        name
    )

    if value is None:

        return None

    value = value.strip()

    if not value:

        return None

    return value


def _read_int_env(
    name: str,
    *,
    default: int,
) -> int:

    raw = _clean_env(
        name
    )

    if raw is None:

        return default

    try:

        value = int(
            raw
        )

    except ValueError as exc:

        raise (
            PostgresConfigurationError(
                f"{name} must be an integer: "
                f"{raw!r}"
            )
        ) from exc

    if value <= 0:

        raise (
            PostgresConfigurationError(
                f"{name} must be positive"
            )
        )

    return value

def make_config_postgres_connection_factory(
    config,
) -> Callable:
    """
    从 FrameworkConfig 创建 PostgreSQL factory。
    """

    pg = config.postgres

    settings = PostgresSettings(
        host=pg.host,
        port=pg.port,
        database=pg.database,
        user=pg.user,
        password=pg.password,
    )

    return (
        make_postgres_connection_factory(
            settings
        )
    )