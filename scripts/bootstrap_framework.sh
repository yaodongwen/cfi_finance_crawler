#!/usr/bin/env bash

set -euo pipefail

# ============================================================
# Crawl Framework bootstrap
#
# 默认运行位置：
# /Users/yaodongdong/WorkPlace/intern/crawler/crawl_framework
#
# 本脚本：
#   - 创建 V2 项目骨架
#   - 检查现有 Naver / Toss 项目
#   - 记录 legacy 路径
#   - 不修改、不移动、不删除旧项目
# ============================================================


# ------------------------------------------------------------
# 颜色
# ------------------------------------------------------------

GREEN="\033[0;32m"
YELLOW="\033[0;33m"
RED="\033[0;31m"
BLUE="\033[0;34m"
NC="\033[0m"


info() {
    echo -e "${BLUE}[INFO]${NC} $*"
}

ok() {
    echo -e "${GREEN}[OK]${NC} $*"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

fail() {
    echo -e "${RED}[ERROR]${NC} $*" >&2
    exit 1
}


# ------------------------------------------------------------
# 项目根目录
# ------------------------------------------------------------

PROJECT_ROOT="${1:-$(pwd)}"

cd "$PROJECT_ROOT"

PROJECT_ROOT="$(pwd)"


# ------------------------------------------------------------
# Legacy 路径
# ------------------------------------------------------------

CRAWLER_ROOT="$(cd "$PROJECT_ROOT/.." && pwd)"

NAVER_ROOT="$CRAWLER_ROOT/naver"

NAVER_CRAWLER="$NAVER_ROOT/Knaver_crawler"
NAVER_STORAGE="$NAVER_ROOT/resyn_storage"
NAVER_RESEARCH="$NAVER_ROOT/naver_research"

TOSS_ROOT="$CRAWLER_ROOT/crawl_tossinvest/tossinvest_crawler_v2"


echo
echo "============================================================"
echo " Crawl Framework Bootstrap"
echo "============================================================"
echo

info "PROJECT_ROOT    = $PROJECT_ROOT"
info "CRAWLER_ROOT    = $CRAWLER_ROOT"
info "NAVER_CRAWLER   = $NAVER_CRAWLER"
info "NAVER_STORAGE   = $NAVER_STORAGE"
info "NAVER_RESEARCH  = $NAVER_RESEARCH"
info "TOSS_ROOT       = $TOSS_ROOT"

echo


# ------------------------------------------------------------
# 检查旧项目
# ------------------------------------------------------------

check_dir() {

    local path="$1"
    local label="$2"

    if [[ -d "$path" ]]; then
        ok "$label: $path"
    else
        warn "$label 不存在: $path"
    fi
}


check_file() {

    local path="$1"
    local label="$2"

    if [[ -f "$path" ]]; then
        ok "$label: $path"
    else
        warn "$label 不存在: $path"
    fi
}


check_dir "$NAVER_CRAWLER" "Naver crawler"
check_dir "$NAVER_STORAGE" "Naver storage"
check_dir "$NAVER_RESEARCH" "Naver research"

check_dir "$TOSS_ROOT" "TossInvest V2"

check_file \
    "$TOSS_ROOT/crawler.py" \
    "Toss crawler.py"

check_file \
    "$TOSS_ROOT/production_runner.py" \
    "Toss production_runner.py"

check_file \
    "$NAVER_STORAGE/storage_sync.py" \
    "Naver storage_sync.py"

echo


# ------------------------------------------------------------
# 创建目录
# ------------------------------------------------------------

info "创建目录结构..."


mkdir -p \
    src/crawl_framework/core \
    src/crawl_framework/storage \
    src/crawl_framework/transports \
    src/crawl_framework/datasets \
    src/crawl_framework/sites/naver_finance \
    src/crawl_framework/sites/tossinvest \
    src/crawl_framework/sites/hotcopper \
    src/crawl_framework/sites/stockhouse \
    src/crawl_framework/cli \
    config \
    scripts \
    tests/unit \
    tests/integration \
    state \
    spool \
    warehouse \
    logs


ok "目录创建完成"


# ------------------------------------------------------------
# 创建 __init__.py
# ------------------------------------------------------------

info "创建 Python package 文件..."


touch \
    src/crawl_framework/__init__.py \
    src/crawl_framework/core/__init__.py \
    src/crawl_framework/storage/__init__.py \
    src/crawl_framework/transports/__init__.py \
    src/crawl_framework/datasets/__init__.py \
    src/crawl_framework/sites/__init__.py \
    src/crawl_framework/sites/naver_finance/__init__.py \
    src/crawl_framework/sites/tossinvest/__init__.py \
    src/crawl_framework/sites/hotcopper/__init__.py \
    src/crawl_framework/sites/stockhouse/__init__.py \
    src/crawl_framework/cli/__init__.py


# ------------------------------------------------------------
# 创建 Core 占位文件
# ------------------------------------------------------------

touch \
    src/crawl_framework/core/models.py \
    src/crawl_framework/core/dataset.py \
    src/crawl_framework/core/plugin.py \
    src/crawl_framework/core/pipeline.py \
    src/crawl_framework/core/registry.py \
    src/crawl_framework/core/runtime.py


# ------------------------------------------------------------
# 创建 Storage 占位文件
# ------------------------------------------------------------

touch \
    src/crawl_framework/storage/buffer.py \
    src/crawl_framework/storage/seen_store.py \
    src/crawl_framework/storage/checkpoint.py \
    src/crawl_framework/storage/partition.py \
    src/crawl_framework/storage/parquet_writer.py \
    src/crawl_framework/storage/postgres.py \
    src/crawl_framework/storage/uploader.py \
    src/crawl_framework/storage/cleaner.py \
    src/crawl_framework/storage/recovery.py


# ------------------------------------------------------------
# Transport
# ------------------------------------------------------------

touch \
    src/crawl_framework/transports/http.py \
    src/crawl_framework/transports/playwright.py \
    src/crawl_framework/transports/proxy.py \
    src/crawl_framework/transports/rate_limit.py


# ------------------------------------------------------------
# Dataset
# ------------------------------------------------------------

touch \
    src/crawl_framework/datasets/news.py \
    src/crawl_framework/datasets/forum_post.py \
    src/crawl_framework/datasets/comment.py \
    src/crawl_framework/datasets/research.py \
    src/crawl_framework/datasets/author.py \
    src/crawl_framework/datasets/holding.py


# ------------------------------------------------------------
# Sites
# ------------------------------------------------------------

touch \
    src/crawl_framework/sites/naver_finance/plugin.py \
    src/crawl_framework/sites/naver_finance/news.py \
    src/crawl_framework/sites/naver_finance/comments.py \
    src/crawl_framework/sites/tossinvest/plugin.py \
    src/crawl_framework/sites/tossinvest/news.py \
    src/crawl_framework/sites/tossinvest/comments.py


# ------------------------------------------------------------
# Site YAML
# ------------------------------------------------------------

create_if_missing() {

    local path="$1"

    if [[ -e "$path" ]]; then
        warn "保留已有文件: $path"
        return 1
    fi

    return 0
}


if create_if_missing \
    "src/crawl_framework/sites/naver_finance/site.yaml"
then
cat > \
    src/crawl_framework/sites/naver_finance/site.yaml \
<<'YAML'
site_id: naver_finance

country: KR

timezone: Asia/Seoul

enabled: false

transport:
  type: http

datasets:
  - news_article
  - news_instrument
  - forum_post
YAML
fi


if create_if_missing \
    "src/crawl_framework/sites/tossinvest/site.yaml"
then
cat > \
    src/crawl_framework/sites/tossinvest/site.yaml \
<<'YAML'
site_id: tossinvest

country: KR

timezone: Asia/Seoul

enabled: false

transport:
  type: playwright

datasets:
  - news_article
  - news_instrument
  - forum_post
YAML
fi


if create_if_missing \
    "src/crawl_framework/sites/hotcopper/site.yaml"
then
cat > \
    src/crawl_framework/sites/hotcopper/site.yaml \
<<'YAML'
site_id: hotcopper

country: AU

timezone: Australia/Sydney

enabled: false

transport:
  type: auto

datasets:
  - forum_post
  - comment
  - author_profile
YAML
fi


if create_if_missing \
    "src/crawl_framework/sites/stockhouse/site.yaml"
then
cat > \
    src/crawl_framework/sites/stockhouse/site.yaml \
<<'YAML'
site_id: stockhouse

country: CA

timezone: America/Toronto

enabled: false

transport:
  type: auto

datasets:
  - news_article
  - forum_post
  - comment
YAML
fi


# ------------------------------------------------------------
# .gitignore
# ------------------------------------------------------------

if create_if_missing ".gitignore"
then
cat > .gitignore <<'EOF'
# ------------------------------------------------------------
# Python
# ------------------------------------------------------------

__pycache__/
*.py[cod]
*.pyo
*.pyd

.pytest_cache/
.mypy_cache/
.ruff_cache/

.venv/
venv/
env/


# ------------------------------------------------------------
# macOS
# ------------------------------------------------------------

.DS_Store


# ------------------------------------------------------------
# IDE
# ------------------------------------------------------------

.idea/
.vscode/


# ------------------------------------------------------------
# Secrets
# ------------------------------------------------------------

.env

config/local.yaml
config/secrets.yaml


# ------------------------------------------------------------
# Runtime data
# ------------------------------------------------------------

state/*
!state/.gitkeep

spool/*
!spool/.gitkeep

warehouse/*
!warehouse/.gitkeep

logs/*
!logs/.gitkeep


# ------------------------------------------------------------
# Temporary
# ------------------------------------------------------------

*.tmp
*.part
*.pending

.sync_pending/
EOF
fi


touch \
    state/.gitkeep \
    spool/.gitkeep \
    warehouse/.gitkeep \
    logs/.gitkeep


# ------------------------------------------------------------
# .env.example
# ------------------------------------------------------------

if create_if_missing ".env.example"
then
cat > .env.example <<'EOF'
# ============================================================
# PostgreSQL
# ============================================================

PGHOST=127.0.0.1
PGPORT=5432
PGDATABASE=stock_data
PGUSER=stock
PGPASSWORD=


# ============================================================
# Remote storage
# ============================================================

STORAGE_SSH_HOST=
STORAGE_SSH_PORT=22
STORAGE_SSH_USER=

STORAGE_REMOTE_ROOT=/mnt/nas-intern/homes/dwyao/Data/stocklake/v2


# ============================================================
# Proxy
# ============================================================

HTTP_PROXY=
HTTPS_PROXY=

PLAYWRIGHT_PROXY=
EOF
fi


# ------------------------------------------------------------
# platform.example.yaml
# ------------------------------------------------------------

if create_if_missing \
    "config/platform.example.yaml"
then
cat > \
    config/platform.example.yaml \
<<'YAML'
platform:

  schema_version: 1


storage:

  local:

    state_dir: state

    spool_dir: spool

    warehouse_dir: warehouse


  parquet:

    compression: zstd

    target_bytes: 268435456

    min_rows: 10000

    max_rows: 500000

    flush_seconds: 30


  partition:

    bucket_count: 256


upload:

  method: auto

  workers: 2

  verify:

    file_size: true

    sha256: true


postgres:

  schema: marketdata


runtime:

  queue_max_records: 100000

  storage_workers: 2

  shutdown_grace_seconds: 30
YAML
fi


# ------------------------------------------------------------
# sites.example.yaml
# ------------------------------------------------------------

if create_if_missing \
    "config/sites.example.yaml"
then
cat > \
    config/sites.example.yaml \
<<'YAML'
sites:

  naver_finance:

    enabled: false


  tossinvest:

    enabled: false


  hotcopper:

    enabled: false


  stockhouse:

    enabled: false
YAML
fi


# ------------------------------------------------------------
# local.yaml
#
# 这个文件不提交 Git。
# ------------------------------------------------------------

if create_if_missing \
    "config/local.yaml"
then
cat > \
    config/local.yaml \
<<EOF
legacy:

  naver_crawler: "$NAVER_CRAWLER"

  naver_storage: "$NAVER_STORAGE"

  naver_research: "$NAVER_RESEARCH"

  tossinvest_v2: "$TOSS_ROOT"


storage:

  remote_root: "/mnt/nas-intern/homes/dwyao/Data/stocklake/v2"
EOF
fi


# ------------------------------------------------------------
# pyproject.toml
# ------------------------------------------------------------

if create_if_missing "pyproject.toml"
then
cat > pyproject.toml <<'TOML'
[build-system]

requires = [
    "setuptools>=68",
    "wheel",
]

build-backend = "setuptools.build_meta"


[project]

name = "crawl-framework"

version = "0.1.0"

description = "Reusable financial website crawling and storage framework"

requires-python = ">=3.11"

dependencies = [
    "PyYAML>=6.0",
    "pyarrow>=15",
]


[project.optional-dependencies]

postgres = [
    "psycopg[binary]>=3.1",
]

http = [
    "httpx>=0.27",
]

playwright = [
    "playwright>=1.45",
]

dev = [
    "pytest>=8",
    "ruff>=0.5",
]


[tool.setuptools]

package-dir = {"" = "src"}


[tool.setuptools.packages.find]

where = ["src"]


[tool.pytest.ini_options]

testpaths = [
    "tests",
]


[tool.ruff]

line-length = 100
TOML
fi


# ------------------------------------------------------------
# CLI placeholder
# ------------------------------------------------------------

if create_if_missing \
    "src/crawl_framework/cli/main.py"
then
cat > \
    src/crawl_framework/cli/main.py \
<<'PY'
from __future__ import annotations

import argparse
import sys


def main() -> None:

    parser = argparse.ArgumentParser(
        prog="crawl-framework",
        description=(
            "Financial website crawling and storage framework"
        ),
    )

    sub = parser.add_subparsers(
        dest="command",
        required=True,
    )

    sub.add_parser(
        "doctor",
        help="Check framework installation.",
    )

    args = parser.parse_args()

    if args.command == "doctor":

        print(
            "crawl-framework bootstrap OK"
        )

        print(
            f"python={sys.version.split()[0]}"
        )


if __name__ == "__main__":
    main()
PY
fi


# ------------------------------------------------------------
# package version
# ------------------------------------------------------------

if [[ ! -s src/crawl_framework/__init__.py ]]
then
cat > \
    src/crawl_framework/__init__.py \
<<'PY'
__version__ = "0.1.0"
PY
fi


# ------------------------------------------------------------
# README 提示
# ------------------------------------------------------------

if [[ ! -f README.md ]]; then

    warn "README.md 尚不存在。"

    warn "请把本次提供的 README 内容保存为 README.md。"

else

    ok "README.md 已存在，不覆盖。"

fi


# ------------------------------------------------------------
# scripts
# ------------------------------------------------------------

cp "$0" \
   scripts/bootstrap_framework.sh \
   2>/dev/null || true


chmod +x \
    scripts/bootstrap_framework.sh \
    2>/dev/null || true


# ------------------------------------------------------------
# 完成
# ------------------------------------------------------------

echo
echo "============================================================"
echo " Bootstrap complete"
echo "============================================================"
echo

ok "项目骨架创建完成"

echo
echo "目录："
echo

find src/crawl_framework \
    -maxdepth 3 \
    -type d \
    | sort

echo
echo "下一步建议："
echo
echo "  1. 保存 README.md"
echo
echo "  2. pip install -e ."
echo
echo "  3. python -m crawl_framework.cli.main doctor"
echo
echo "  4. 开始实现 core/models.py"
echo
