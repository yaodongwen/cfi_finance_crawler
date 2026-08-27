import pytest

from crawl_framework.storage.postgres_connection import (
    PostgresConfigurationError,
    PostgresSettings,
    make_postgres_connection_factory,
)


# ============================================================
# Validation
# ============================================================


def test_dsn_is_valid():

    settings = PostgresSettings(
        dsn=(
            "postgresql://"
            "user:pass@localhost/db"
        )
    )

    settings.validate()

    assert (
        settings.uses_dsn
        is True
    )


def test_keyword_config_is_valid():

    settings = PostgresSettings(
        host="localhost",
        database="stock_data",
        user="yaodongdong",
    )

    settings.validate()

    assert (
        settings.uses_dsn
        is False
    )


def test_missing_host_rejected():

    settings = PostgresSettings(
        database="stock_data",
        user="user",
    )

    with pytest.raises(
        PostgresConfigurationError
    ):

        settings.validate()


def test_missing_database_rejected():

    settings = PostgresSettings(
        host="localhost",
        user="user",
    )

    with pytest.raises(
        PostgresConfigurationError
    ):

        settings.validate()


def test_missing_user_rejected():

    settings = PostgresSettings(
        host="localhost",
        database="stock_data",
    )

    with pytest.raises(
        PostgresConfigurationError
    ):

        settings.validate()


def test_invalid_port_rejected():

    with pytest.raises(
        ValueError
    ):

        PostgresSettings(
            host="localhost",
            database="stock_data",
            user="user",
            port=0,
        )


def test_invalid_timeout_rejected():

    with pytest.raises(
        ValueError
    ):

        PostgresSettings(
            host="localhost",
            database="stock_data",
            user="user",
            connect_timeout=0,
        )


# ============================================================
# Environment
# ============================================================


def test_from_env_dsn(
    monkeypatch,
):

    monkeypatch.setenv(
        "CRAWL_PG_DSN",
        (
            "postgresql://"
            "user:pass@localhost/stock_data"
        ),
    )

    settings = (
        PostgresSettings
        .from_env()
    )

    assert settings.dsn == (
        "postgresql://"
        "user:pass@localhost/stock_data"
    )


def test_from_env_fields(
    monkeypatch,
):

    monkeypatch.delenv(
        "CRAWL_PG_DSN",
        raising=False,
    )

    monkeypatch.setenv(
        "CRAWL_PG_HOST",
        "127.0.0.1",
    )

    monkeypatch.setenv(
        "CRAWL_PG_PORT",
        "5433",
    )

    monkeypatch.setenv(
        "CRAWL_PG_DATABASE",
        "stock_data",
    )

    monkeypatch.setenv(
        "CRAWL_PG_USER",
        "crawler",
    )

    monkeypatch.setenv(
        "CRAWL_PG_PASSWORD",
        "secret",
    )

    settings = (
        PostgresSettings
        .from_env()
    )

    assert (
        settings.host
        == "127.0.0.1"
    )

    assert (
        settings.port
        == 5433
    )

    assert (
        settings.database
        == "stock_data"
    )

    assert (
        settings.user
        == "crawler"
    )

    assert (
        settings.password
        == "secret"
    )


def test_default_port(
    monkeypatch,
):

    monkeypatch.delenv(
        "CRAWL_PG_PORT",
        raising=False,
    )

    settings = (
        PostgresSettings
        .from_env()
    )

    assert (
        settings.port
        == 5432
    )


def test_invalid_env_port(
    monkeypatch,
):

    monkeypatch.setenv(
        "CRAWL_PG_PORT",
        "hello",
    )

    with pytest.raises(
        PostgresConfigurationError
    ):

        PostgresSettings.from_env()


# ============================================================
# Factory
# ============================================================


def test_factory_creation():

    settings = PostgresSettings(
        host="localhost",
        database="stock_data",
        user="crawler",
    )

    factory = (
        make_postgres_connection_factory(
            settings
        )
    )

    assert callable(
        factory
    )


def test_factory_requires_valid_config():

    settings = PostgresSettings()

    with pytest.raises(
        PostgresConfigurationError
    ):

        make_postgres_connection_factory(
            settings
        )