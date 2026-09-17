import pytest

from crawl_framework.config import (
    ConfigError,
    load_config,
)


def write_config(
    tmp_path,
    text,
):

    path = (
        tmp_path
        / "config.yaml"
    )

    path.write_text(
        text,
        encoding="utf-8",
    )

    return path


def test_load_config(
    tmp_path,
):

    path = write_config(
        tmp_path,
        """
local:
  output_dir: "./outputs"
  warehouse_dir: "./warehouse"
  index_cache_dir: "./index_cache"

server:
  host: "192.168.1.33"
  user: "dwyao"
  data_dir: "/data/stocklake"

postgres:
  host: "192.168.1.33"
  port: 5432
  database: "stock_data"
  user: "stock"
  password: "secret"

storage:
  local_warehouse: "./warehouse"
  server_warehouse: "/data/stocklake"
  partition:
    max_rows: 50000
  compression:
    codec: "zstd"

sync:
  delete_after_upload: true
  method: auto
  rsync:
    enabled: true
    ssh_port: 22

browser:
  workers: 3
  headless: true
  profile_root: "./browser_profiles"
  recycle_after_scopes: 25
  proxy_strategy: round_robin
  proxy_failure_cooldown_seconds: 45
  proxies:
    - "http://proxy-a"
    - "http://proxy-b"
  budgets:
    forum: 2
    news_detail: 1
""",
    )

    config = load_config(
        path
    )

    assert (
        config.postgres.host
        == "192.168.1.33"
    )

    assert (
        config.postgres.port
        == 5432
    )

    assert (
        config.postgres.database
        == "stock_data"
    )

    assert (
        config.postgres.user
        == "stock"
    )

    assert (
        config.postgres.password
        == "secret"
    )

    assert (
        config.storage.max_rows
        == 50000
    )

    assert (
        config.storage.compression
        == "zstd"
    )

    assert (
        config.sync.ssh_port
        == 22
    )

    assert config.browser.workers == 3
    assert config.browser.profile_root == (tmp_path / "browser_profiles").resolve()
    assert config.browser.proxies == ("http://proxy-a", "http://proxy-b")
    assert config.browser.proxy_failure_cooldown_seconds == 45
    assert dict(config.browser.worker_budgets) == {"forum": 2, "news_detail": 1}


def test_relative_paths_use_config_dir(
    tmp_path,
):

    path = write_config(
        tmp_path,
        """
local:
  output_dir: "./outputs"
  warehouse_dir: "../warehouse"
  index_cache_dir: "./index_cache"

server:
  host: "server"
  user: "user"
  data_dir: "/remote/data"

postgres:
  host: "db"
  database: "stock_data"
  user: "stock"

storage:
  local_warehouse: "../warehouse"
  server_warehouse: "/remote/data"
""",
    )

    config = load_config(
        path
    )

    assert (
        config.local.output_dir
        == (
            tmp_path
            / "outputs"
        ).resolve()
    )

    assert (
        config.local.warehouse_dir
        == (
            tmp_path
            / "../warehouse"
        ).resolve()
    )


def test_missing_postgres_rejected(
    tmp_path,
):

    path = write_config(
        tmp_path,
        """
local:
  output_dir: "./outputs"
  warehouse_dir: "./warehouse"
  index_cache_dir: "./index"

server:
  host: "server"
  user: "user"
  data_dir: "/remote"
""",
    )

    with pytest.raises(
        ConfigError
    ):

        load_config(
            path
        )


def test_bad_port_rejected(
    tmp_path,
):

    path = write_config(
        tmp_path,
        """
local:
  output_dir: "./outputs"
  warehouse_dir: "./warehouse"
  index_cache_dir: "./index"

server:
  host: "server"
  user: "user"
  data_dir: "/remote"

postgres:
  host: "db"
  port: 0
  database: "stock_data"
  user: "stock"

storage:
  local_warehouse: "./warehouse"
  server_warehouse: "/remote"
""",
    )

    with pytest.raises(
        ConfigError
    ):

        load_config(
            path
        )
