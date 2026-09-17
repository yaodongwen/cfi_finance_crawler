# Crawl Framework

通用财经网站数据采集、标准化、去重、Parquet 存储、远端同步与 PostgreSQL 索引框架。

本项目用于逐步替代目前分别维护的：

* Naver Finance 爬虫与存储系统
* TossInvest 爬虫与存储系统
* 后续 HotCopper
* Stockhouse
* 其他国家财经网站、股票论坛、研报网站、投资社区

最终目标是实现：

> 新增一个网站时，只开发该网站自己的发现、抓取和字段映射代码，不再重复开发去重、缓存、Parquet、PostgreSQL、上传、恢复和清理系统。

---

# 0. 当前使用方法与能力范围

## 环境

本仓库当前验证环境：

```bash
source /opt/anaconda3/etc/profile.d/conda.sh
conda activate pac
```

运行单元测试：

```bash
pytest -q -p no:rerunfailures tests/unit
```

最新已验证基线：

```text
837 passed
```

## Naver 一键 Profile

当前 production CLI 的权威入口是：

```bash
python -m crawl_framework.cli.main crawl
```

Naver 支持两个一键 profile：

```text
naver_full
naver_incremental
```

`naver_full` 覆盖当前 Naver 内容面：

```text
forum_post
news_article
news_instrument
research_report
research_instrument
attachment
```

默认 instrument universe 来自：

```text
config/universes/naver_finance_kr_rollout_universe.txt
```

当前 universe 为 3924 个 canonical Korean instruments，格式：

```text
XKRX:005930
```

## TossInvest 一键 Profile

TossInvest 使用同一个 production CLI，并提供：

```text
toss_incremental
toss_full
toss_historical_backfill
```

默认 universe 来自
`config/universes/tossinvest_kr_rollout_universe.txt`，当前为 2427 个按真实
Toss Screener discovery 顺序固化的 canonical instruments。

日常增量运行：

```bash
python -m crawl_framework.cli.main crawl \
  --site tossinvest \
  --profile toss_incremental \
  --trust-rsync-success \
  --ssh-multiplex
```

安全边界的全市场运行：

```bash
python -m crawl_framework.cli.main crawl \
  --site tossinvest \
  --profile toss_full \
  --trust-rsync-success \
  --ssh-multiplex
```

两个 profile 都运行 `forum_post`、`news_article`、`news_instrument`，并默认将
forum/news 列表限制为一轮。`toss_incremental` 保留 checkpoint 中已完成的
news historical baseline，先重试 pending detail，再启用历史区早停；
`toss_full` 忽略 baseline 早停，但仍保留安全页界限。

`toss_historical_backfill` 不施加上述页界限，用于可恢复的显式历史基线任务。
它不是日常运行入口，也尚未做全历史生产验收。可用 `--forum-max-pages`、
`--news-max-pages`、`--forum-mode`、`--news-mode` 覆盖 profile 默认值。

## Kabutan 免费公开新闻 Profile

Kabutan Market News 不需要 instrument，支持：

```text
kabutan_incremental
kabutan_free_full
```

日常增量：

```bash
python -m crawl_framework.cli.main \
  --site kabutan \
  --profile kabutan_incremental \
  --crawl-workers 2 \
  --http-concurrency 8 \
  --writer-workers 2 \
  --upload-workers 2 \
  --catalog-workers 2 \
  --target-file-size-mb 64 \
  --coalesce-scope-flushes
```

扫描全部当前免费可访问范围：

```bash
python -m crawl_framework.cli.main \
  --site kabutan \
  --profile kabutan_free_full \
  --kabutan-max-pages-per-month 500 \
  --crawl-workers 2 \
  --http-concurrency 8 \
  --writer-workers 2 \
  --upload-workers 2 \
  --catalog-workers 2 \
  --target-file-size-mb 64 \
  --coalesce-scope-flushes
```

`kabutan_free_full` 从当前月向历史方向发现 month scope，遇到无真实文章
链接的 Premium 边界后正常停止。兼容名称 `kabutan_full` 具有相同的免费访问
语义。框架不会登录 Premium、猜测隐藏 news ID 或声称 2013-09 至今完整。

截至 2026-09-11，incremental production acceptance 已通过；free-full 的
最终 K13 验收因 Kabutan AWS WAF 临时返回 HTTP 405 Human Verification 而待
重试。这不是 Premium 登录要求，框架不会绕过该验证页面。

手工检查匿名访问状态：

```bash
python scripts/probe_kabutan_access.py
```

输出 `access_state=AVAILABLE`（退出码 0）后才能手工重试 K13；
`WAF_BLOCKED` 使用退出码 2。probe 每次只请求当前月份 page 1 一次，不输出
cookie 或请求头秘密，也不会自动启动 rollout。

## 推荐 Production 命令

当前推荐的 I16 full-content production 命令：

```bash
python -m crawl_framework.cli.main crawl \
  --site naver_finance \
  --profile naver_full \
  --crawl-workers 16 \
  --writer-workers 4 \
  --upload-workers 2 \
  --catalog-workers 4 \
  --attachment-workers 4 \
  --target-file-size-mb 16 \
  --coalesce-scope-flushes \
  --trust-rsync-success \
  --ssh-multiplex \
  --compact-after-dataset \
  --compaction-min-file-count 2 \
  --progress-interval-seconds 60 \
  --run-manifest \
  --run-manifest-path state/run_manifests/naver_full_i16_YYYYMMDDTHHMMSSZ.json
```

这些关键参数的含义：

```text
--coalesce-scope-flushes
  大规模 rollout 时不在每个 instrument scope 完成后强制 flush，
  降低小 Parquet 文件数量；checkpoint 会等待对应 batch 完成
  upload/Catalog 后再提交。

--trust-rsync-success
  rsync exit code 成功时跳过每文件额外 ssh stat，Catalog 仍记录
  本地 file size 和 sha256。

--ssh-multiplex
  使用 OpenSSH ControlMaster 复用 SSH 连接，降低大量小文件上传时
  每个 rsync/mkdir/stat 的连接建立成本。默认 ControlPath 为
  /tmp/cfw-%C，避免 macOS Unix socket path too long。

--compact-after-dataset
  每个 dataset drain 完成后执行 generic compaction，把同一
  site/country/dataset/day/bucket 分区内的多个 active uploaded
  小 Parquet 合成 replacement Parquet。

--compaction-min-file-count 2
  同一分区至少有 2 个 active 文件时才压缩。
```

## 中断恢复

Production run 可以中断后恢复。

恢复命令：

```bash
python -m crawl_framework.cli.main crawl \
  --site naver_finance \
  --profile naver_full \
  --recovery-only \
  --run-manifest \
  --run-manifest-path state/run_manifests/naver_full_recovery_YYYYMMDDTHHMMSSZ.json
```

期望健康输出：

```text
success=True
remaining=0
```

或者 JSON/manifest 中：

```text
remaining_pending=0
terminal_failed=0
```

恢复语义：

```text
write
-> upload
-> verify
-> Catalog/index
-> SeenStore
-> checkpoint
-> cleanup eligibility
```

中断后 recovery 会从 recovery manifest 中恢复未完成的 durable
batch，不需要重新设计 crawl 逻辑。

## 小文件与 PostgreSQL Catalog 语义

PostgreSQL Catalog 是 file-level Catalog：

```text
一行 data_files 记录 = 一个 Parquet 数据文件
```

它不是：

```text
一条新闻一行
一条讨论帖一行
一条评论一行
```

如果 Parquet 文件过小，Catalog active 行数也会被放大。因此当前
推荐 production run 启用：

```text
--coalesce-scope-flushes
--compact-after-dataset
```

自动 compaction 的安全边界：

```text
只在同一 site/country/dataset/partition_date/bucket 内合并
不跨日期合并
不跨 bucket 合并
不改变当前 query bucket pruning 语义
不直接删除 NAS 上的旧物理文件
```

发布顺序：

```text
write compacted replacement
-> upload replacement
-> register replacement
-> mark replacement uploaded
-> mark source small files superseded
```

查询侧只读取：

```text
storage_status = uploaded
lifecycle_status = active
```

因此 compaction 后，源小文件不会再作为 active 查询面出现。

## 当前能力范围

已经真实接入 production plugin/CLI 的 Naver dataset：

```text
forum_post
news_article
news_instrument
research_report
research_instrument
attachment
```

已经完成真实生产验证：

```text
full 3924-instrument forum_post rollout
50-instrument full-content smoke
research six-category small smoke
single PDF attachment smoke
```

正在进行但尚未最终验收：

```text
I16 naver_full full-market all-content production run
post-dataset compaction production effect
full news/research/PDF scale validation
```

框架通用能力：

```text
bounded concurrent production runtime
backpressure queues
configurable crawl/writer/upload/catalog workers
SeenStore
checkpoint
recovery manifests
Parquet writer
rsync/NAS uploader
PostgreSQL file Catalog
record side index
query layer
generic compaction
generic cleanup/repair/recovery
generic attachment pipeline
```

站点插件只应实现：

```text
instrument discovery
site fetch
site normalize
attachment discovery
```

---

# 1. 当前项目位置

本项目根目录：

```text
/Users/yaodongdong/WorkPlace/intern/crawler/crawl_framework
```

当前已有项目：

```text
/Users/yaodongdong/WorkPlace/intern/crawler/
│
├── crawl_framework/
│
├── naver/
│   ├── Knaver_crawler/
│   ├── naver_research/
│   └── resyn_storage/
│
└── crawl_tossinvest/
    ├── tossinvest_crawler/
    └── tossinvest_crawler_v2/
```

其中本项目第一阶段主要参考：

```text
../naver/Knaver_crawler
../naver/resyn_storage
../crawl_tossinvest/tossinvest_crawler_v2
```

`naver_research` 暂时不迁移，等新闻、评论、帖子 Storage Core 稳定后再接入研报/PDF体系。

---

# 2. 核心设计原则

整个系统分成两部分：

```text
网站专用代码
        │
        ▼
统一数据接口
        │
        ▼
通用存储系统
```

即：

```text
Naver ───────┐
             │
TossInvest ──┤
             │
HotCopper ───┼──> CanonicalRecord
             │            │
Stockhouse ──┤            ▼
             │       Unified Pipeline
其他网站 ────┘            │
                          ▼
                    Seen / Version
                          │
                          ▼
                       Buffer
                          │
                          ▼
                       Parquet
                          │
                          ▼
                     PostgreSQL
                          │
                          ▼
                      rsync/NAS
                          │
                          ▼
                  Remote Verification
                          │
                          ▼
                       Cleanup
```

网站插件不负责：

```text
Parquet
PostgreSQL
rsync
SHA256
历史去重
远端校验
本地清理
Storage Recovery
```

这些功能只能存在于 Core 中一份。

---

# 3. 项目目录

初始化完成后目录结构：

```text
crawl_framework/
│
├── README.md
├── pyproject.toml
├── .gitignore
├── .env.example
│
├── config/
│   ├── platform.example.yaml
│   ├── sites.example.yaml
│   └── local.yaml
│
├── scripts/
│   └── bootstrap_framework.sh
│
├── src/
│   └── crawl_framework/
│       │
│       ├── __init__.py
│       │
│       ├── core/
│       │   ├── __init__.py
│       │   ├── models.py
│       │   ├── dataset.py
│       │   ├── plugin.py
│       │   ├── pipeline.py
│       │   ├── registry.py
│       │   └── runtime.py
│       │
│       ├── storage/
│       │   ├── __init__.py
│       │   ├── buffer.py
│       │   ├── seen_store.py
│       │   ├── checkpoint.py
│       │   ├── partition.py
│       │   ├── parquet_writer.py
│       │   ├── postgres.py
│       │   ├── uploader.py
│       │   ├── cleaner.py
│       │   └── recovery.py
│       │
│       ├── transports/
│       │   ├── __init__.py
│       │   ├── http.py
│       │   ├── playwright.py
│       │   ├── proxy.py
│       │   └── rate_limit.py
│       │
│       ├── datasets/
│       │   ├── __init__.py
│       │   ├── news.py
│       │   ├── forum_post.py
│       │   ├── comment.py
│       │   ├── research.py
│       │   ├── author.py
│       │   └── holding.py
│       │
│       ├── sites/
│       │   ├── __init__.py
│       │   │
│       │   ├── naver_finance/
│       │   │   ├── __init__.py
│       │   │   ├── plugin.py
│       │   │   ├── news.py
│       │   │   ├── comments.py
│       │   │   └── site.yaml
│       │   │
│       │   ├── tossinvest/
│       │   │   ├── __init__.py
│       │   │   ├── plugin.py
│       │   │   ├── news.py
│       │   │   ├── comments.py
│       │   │   └── site.yaml
│       │   │
│       │   ├── hotcopper/
│       │   │   ├── __init__.py
│       │   │   └── site.yaml
│       │   │
│       │   └── stockhouse/
│       │       ├── __init__.py
│       │       └── site.yaml
│       │
│       └── cli/
│           ├── __init__.py
│           └── main.py
│
├── tests/
│   ├── unit/
│   └── integration/
│
├── state/
├── spool/
├── warehouse/
└── logs/
```

---

# 4. Legacy 项目

旧项目暂时保持原状。

初始化脚本会把旧项目路径写入：

```text
config/local.yaml
```

例如：

```yaml
legacy:
  naver_crawler: ../naver/Knaver_crawler
  naver_storage: ../naver/resyn_storage
  naver_research: ../naver/naver_research
  tossinvest_v2: ../crawl_tossinvest/tossinvest_crawler_v2
```

这些路径仅用于：

```text
代码迁移
字段对照
数据格式兼容测试
旧数据导入
```

新框架正式运行后不应该依赖旧项目代码。

---

# 5. 数据流

最终统一数据流：

```text
SitePlugin
    │
    ▼
RawRecord
    │
    ▼
Site Normalizer
    │
    ▼
CanonicalRecord
    │
    ▼
record_uid
    │
    ▼
SeenStore / VersionStore
    │
    ▼
RecordBuffer
    │
    ▼
Partitioner
    │
    ▼
ParquetWriter
    │
    ▼
PostgreSQL File Catalog
    │
    ▼
Uploader
    │
    ▼
Remote Verify
    │
    ▼
Commit
    │
    ▼
Cleaner
```

---

# 6. CanonicalRecord

所有网站最终必须转换成统一数据对象。

Core 不认识：

```text
nid
post_id
article_id
contentParams
discussionId
```

Core 只认识：

```text
site_id
country
dataset
source_id
instrument_id
event_time
updated_at
title
content
author
source_url
payload
```

例如：

```python
CanonicalRecord(
    site_id="naver_finance",
    country="KR",
    dataset="forum_post",
    source_id="12345678",
    instrument_id="XKRX:005930",
)
```

HotCopper：

```python
CanonicalRecord(
    site_id="hotcopper",
    country="AU",
    dataset="forum_post",
    source_id="987654",
    instrument_id="XASX:BHP",
)
```

---

# 7. instrument_id

不再把：

```text
005930
A005930
BHP
SHOP
```

直接作为系统内部唯一股票 ID。

统一采用：

```text
MIC:SYMBOL
```

例如：

```text
XKRX:005930
XKRX:000660

XASX:BHP

XTSE:SHOP

XNAS:AAPL

XNYS:TSLA
```

网站自己的代码通过映射表转换为全局 `instrument_id`。

---

# 8. Dataset

第一版标准 Dataset：

```text
news_article
news_instrument

forum_post
comment

research_report
research_instrument

author_profile
author_post

holding_snapshot
holding_position

instrument
instrument_relation

attachment
```

网站不支持的数据类型无需实现。

例如 Naver 第一阶段只实现：

```text
news_article
news_instrument
forum_post
```

Toss 第一阶段：

```text
news_article
news_instrument
forum_post
```

以后 HotCopper：

```text
forum_post
comment
author_profile
```

---

# 9. 新闻与股票关系分离

新闻正文不能因为关联多个股票而复制多份。

例如：

```text
Samsung + SK Hynix 新闻
```

只保存一份：

```text
news_article
```

然后建立：

```text
news_instrument
```

关系：

```text
news_123 -> XKRX:005930
news_123 -> XKRX:000660
```

研究报告采用同样模式：

```text
research_report
research_instrument
```

---

# 10. record_uid

网站 ID 只在网站内部唯一。

系统统一生成：

```text
record_uid
```

基本规则：

```text
site_id
+
dataset
+
scope
+
source_id
```

例如：

```text
naver_finance
forum_post
XKRX:005930
123456
```

计算 SHA256 后作为全局记录 ID。

因此即使：

```text
Naver post_id = 123456
HotCopper post_id = 123456
```

也不会冲突。

---

# 11. 更新数据

不能只判断：

```text
这个 ID 是否爬过
```

还必须判断：

```text
内容是否变化
```

每条记录同时维护：

```text
record_uid
version_hash
```

支持：

```text
immutable
latest
versioned
```

例如：

```text
forum_post -> versioned
holding_snapshot -> immutable snapshot
author_profile -> latest/versioned
```

这样点赞数、回复数、正文修改、大V持仓变化都可以正确处理。

---

# 12. SeenStore

统一定义 SeenStore 接口。

后端可以替换：

```text
SQLite
LMDB
PostgreSQL
其他 KV
```

Crawler 不允许自己实现一套历史 ID 系统。

Naver 当前 SQLite Seen Cache 与 Toss 当前分片 ID Cache 最终都迁移到这里。

---

# 13. Checkpoint

翻页方式属于网站插件，不属于 Storage。

例如：

Naver：

```json
{
  "page": 123
}
```

Toss：

```json
{
  "oldest_seen_id": "...",
  "baseline_complete": false
}
```

HotCopper：

```json
{
  "page": 712
}
```

Core 只负责可靠保存和恢复 checkpoint。

不解释 checkpoint 内容。

---

# 14. HTTP 与 Playwright

统一 Transport：

```text
transports/http.py
transports/playwright.py
```

HTTP 网站复用连接池：

```text
Naver
HotCopper
Stockhouse
...
```

需要浏览器的网站使用长驻 Playwright Worker：

```text
Toss Worker 1
    ├── stock 1
    ├── stock 2
    └── stock 3

Toss Worker 2
    ├── stock 4
    ├── stock 5
    └── stock 6
```

不再：

```text
一只股票
→ 启动 Python
→ 启动 Chromium
→ 爬完
→ 关闭 Chromium
```

---

# 15. Storage 不按股票运行

旧 Toss 模式：

```text
stock A
→ normalize
→ dedup
→ parquet
→ postgres
→ upload

stock B
→ normalize
→ dedup
→ parquet
→ postgres
→ upload
```

新框架：

```text
Crawler Workers
      │
      ▼
async queue
      │
      ▼
Buffer
```

满足任意条件：

```text
达到目标行数
达到目标字节数
达到 flush 时间
```

就自动写一批 Parquet。

Storage 不关心：

```text
当前是哪一只股票
```

只关心：

```text
当前有哪些 partition 可以 flush
```

---

# 16. Parquet

默认建议：

```yaml
parquet:
  target_bytes: 268435456
  min_rows: 10000
  max_rows: 500000
  flush_seconds: 30
  compression: zstd
```

主要目标：

```text
128 MB ~ 256 MB
```

而不是固定：

```text
50000 rows
```

避免长新闻、研报正文产生超大文件，也避免短评论产生大量小文件。

---

# 17. Storage 路径

V2 数据使用独立目录：

```text
stocklake/v2/
```

不直接覆盖旧 Naver/Toss 数据。

例如：

```text
stocklake/v2/
│
├── site=naver_finance/
│   └── country=KR/
│
├── site=tossinvest/
│   └── country=KR/
│
├── site=hotcopper/
│   └── country=AU/
│
└── site=stockhouse/
    └── country=CA/
```

具体：

```text
site=hotcopper/
country=AU/
dataset=forum_post/
year=2026/
month=08/
day=24/
bucket=3f/
part-xxxxxxxx.parquet
```

---

# 18. PostgreSQL

V2 不再：

```text
public
tossinvest
hotcopper
stockhouse
...
```

每个网站复制一套表。

统一使用：

```text
marketdata
```

Schema。

核心表：

```text
marketdata.sources

marketdata.instruments

marketdata.source_instruments

marketdata.data_files

marketdata.record_registry

marketdata.record_instrument

marketdata.crawl_runs

marketdata.checkpoints
```

PostgreSQL 的主要职责是：

```text
定位 Parquet 文件
记录文件状态
维护少量重要 Registry
维护运行状态
```

而不是把几十亿条评论正文全部存入 PostgreSQL。

---

# 19. 本地目录

以下目录属于运行数据，不提交 Git：

```text
state/
spool/
warehouse/
logs/
```

其中：

### state

长期保留：

```text
crawler checkpoints
seen state
version state
pipeline recovery
```

### spool

短期中间数据。

成功进入 Parquet 并完成安全提交后可以清理。

### warehouse

本地待上传 Parquet。

远端完成：

```text
size verify
+
SHA256 verify
```

以后可安全删除。

---

# 20. 配置与密码

数据库密码、SSH 密钥、代理密码等禁止写入 Git。

使用：

```text
.env
```

或系统环境变量。

示例：

```bash
export PGHOST="192.168.1.33"
export PGPORT="5432"
export PGDATABASE="stock_data"
export PGUSER="stock"
export PGPASSWORD="..."
```

仓库只提交：

```text
.env.example
```

绝不提交：

```text
.env
config/local.yaml
```

---

# 21. 初始化

进入项目：

```bash
cd /Users/yaodongdong/WorkPlace/intern/crawler/crawl_framework
```

执行：

```bash
bash bootstrap_framework.sh
```

或者：

```bash
chmod +x bootstrap_framework.sh

./bootstrap_framework.sh
```

脚本会：

```text
1. 检查当前项目路径
2. 检查 Naver Legacy 路径
3. 检查 Toss Legacy 路径
4. 创建 src/ 目录
5. 创建 core/
6. 创建 storage/
7. 创建 transports/
8. 创建 datasets/
9. 创建 sites/
10. 创建 tests/
11. 创建 state/spool/warehouse/logs
12. 创建 pyproject.toml
13. 创建 .gitignore
14. 创建 .env.example
15. 创建 config/local.yaml
```

不会：

```text
删除旧文件
移动旧项目
修改旧 Naver
修改旧 Toss
连接 PostgreSQL
上传服务器
删除任何历史数据
```

---

# 22. Python 环境

当前建议 Python：

```text
Python 3.11
```

可以继续使用现有：

```text
conda env: pac
```

也可以后续新建：

```bash
conda create -n crawler-framework python=3.11
conda activate crawler-framework
```

开发模式安装：

```bash
pip install -e .
```

---

# 23. 第一阶段迁移计划

不要同时改 Naver 和 Toss。

按照以下顺序：

```text
Step 1
项目骨架

↓

Step 2
CanonicalRecord

↓

Step 3
DatasetSpec

↓

Step 4
SitePlugin

↓

Step 5
SeenStore

↓

Step 6
CheckpointStore

↓

Step 7
Partitioner

↓

Step 8
ParquetWriter

↓

Step 9
PostgreSQL Catalog

↓

Step 10
Uploader / Cleaner / Recovery

↓

Step 11
接入 Naver

↓

Step 12
Naver 新旧双跑验证

↓

Step 13
接入 Toss

↓

Step 14
删除 Toss 重复 Storage

↓

Step 15
Playwright 长驻 Worker

↓

Step 16
HotCopper

↓

Step 17
Stockhouse
```

---

# 24. 为什么先迁 Naver

Naver 当前：

```text
HTTP
+
结构相对稳定
+
已有快速 Storage Pipeline
+
已有稳定历史数据
```

适合作为通用框架的第一个 Reference Plugin。

第一阶段需要确保：

```text
旧 Naver
```

与：

```text
NaverPlugin + V2 Core
```

抓取同一批数据时：

```text
记录数基本一致
ID 一致
时间一致
内容一致
无重复
Parquet 可查询
远端文件正确
```

验证完成后，旧 Naver Storage 才停止使用。

---

# 25. 为什么第二个迁 Toss

Toss 是对整个框架最好的压力测试，因为它包含：

```text
Playwright
虚拟列表
无限滚动
股票并发
新闻详情
评论
断点恢复
复杂缓存
动态网页
```

如果：

```text
Naver
+
Toss
```

都能用同一个 Storage Core，

那么以后：

```text
HotCopper
Stockhouse
其他财经社区
```

通常只需要新增站点插件。

---

# 26. HKEX 财务报告

框架已支持 HKEX 财务报告元数据、股票关联和 PDF attachment：

```bash
# 日常增量（默认回看 400 天并下载新 PDF）
python -m crawl_framework.cli.main crawl \
  --site hkexnews \
  --profile hkex_reports_incremental

# 完整 canonical HK universe，可从 checkpoint 恢复
python -m crawl_framework.cli.main crawl \
  --site hkexnews \
  --profile hkex_reports_full
```

只抓元数据时使用 `--no-download-report-pdf`；可用
`--report-type annual|interim|quarterly|all`、`--report-date-from` 和
`--report-date-to` 缩小范围。两个 profile 都走通用并发 durable pipeline，
输出 `financial_report`、`financial_report_instrument`，启用 PDF 时还输出
`attachment`。中断后应原命令重跑，框架会从 checkpoint/SeenStore 恢复，
不要清空状态或手工计算 offset。

# 27. 当前禁止事项

在 V2 完成验证以前：

不要删除：

```text
../naver/
../crawl_tossinvest/tossinvest_crawler_v2/
```

不要清空服务器旧数据。

不要修改旧 PostgreSQL Schema。

不要让新框架直接写入旧：

```text
stocklake/
```

数据目录。

新框架统一使用：

```text
stocklake/v2/
```

直到完成迁移验证。

---

# 28. 最终目标

以后新增一个网站，例如：

```text
sites/hotcopper/
```

开发人员只需要关心：

```text
如何发现股票/栏目
如何翻页
如何无限滚动
如何调用接口
如何下载详情
如何解析字段
如何转换成 CanonicalRecord
```

而不再开发：

```text
历史 ID Cache
跨批次 Dedup
Parquet
PostgreSQL
rsync
SHA256
远端验证
文件清理
断点恢复
运行日志
Storage Pipeline
```

最终形成：

```text
一个 Storage Core
+
一个 Scheduler
+
一套 Transport
+
N 个 Site Plugin
```

这就是本项目的核心目标。
# Phase M development status

The production CLI supports both single-site profiles and one-command
four-site orchestration. Every named production profile uses the bounded
writer/upload/Catalog runtime, including conservative one-worker settings.
Cross-site execution is concurrent and bounded. Site crawl/browser/HTTP rate
limits remain local to each adapter, while Parquet, NAS upload and PostgreSQL
Catalog work share platform-wide permits.

```bash
# Full accepted profile for each site
python -m crawl_framework.cli.main run-platform --profile global_full

# Incremental accepted profile for each site
python -m crawl_framework.cli.main run-platform --profile global_incremental

# Recovery diagnostics across all sites, with no remote access/crawl
python -m crawl_framework.cli.main run-platform \
  --profile global_incremental --recovery-only
```

Mappings are Naver, TossInvest, Kabutan free/public, then HKEX. Before Kabutan
crawl, the orchestrator performs one access probe. AWS WAF Human Verification
marks Kabutan `BLOCKED` and the remaining sites continue; it is not treated as
a parser or storage failure.

Resource controls:

```bash
python -m crawl_framework.cli.main run-platform \
  --profile global_incremental \
  --site-workers 2 \
  --global-writer-workers 2 \
  --global-upload-workers 2 \
  --global-catalog-workers 2
```

Those values are the defaults. `--global-upload-workers` accepts 1 through 4;
the conservative default is 2 because previous production runs showed SSH/NAS
connection resets at excessive concurrency. The limits cover startup recovery,
Parquet preparation, generic attachment upload, final flush, compaction,
upload verification, Catalog/index, SeenStore, and checkpoint completion.
Per-stage budget statistics are included in platform JSON/text results.

Every platform run also writes an atomic aggregate manifest automatically:

```text
state/run_manifests/platform_<profile>_<timestamp>.json
```

Use an explicit path when an external scheduler needs a predictable artifact:

```bash
python -m crawl_framework.cli.main run-platform \
  --profile global_incremental \
  --run-manifest-path state/run_manifests/nightly_incremental.json
```

The manifest includes platform/site profiles, universe snapshot hashes and
counts, ResumePlans, completed/skipped/incomplete scope totals, record and
durable-file counters, uploads, Catalog jobs, startup recovery, shared resource
budget observations, blocked/failed sites, errors, and the final or interrupted
state. JSON is committed with an atomic same-directory replace. Text and JSON
CLI output report the path that was written.

The deterministic resume/progress matrix verifies cold, partial, and fully
complete restarts; all six ResumePlan states; each durable recovery stage;
unchanged replay; zero-row scopes; attachment and compaction metrics; TTY/text
and JSON output policy; and controlled interruption. In particular, a fully
checkpoint-complete profile performs zero crawl, Parquet, upload, and Catalog
work, while restart progress begins from the durable completion percentage.

Current restart safety rule:

```text
Re-run the same accepted single-site command/profile.
Do not clear SeenStore, checkpoints, RecoveryStore, Catalog, or warehouse data.
HTTP crawl is at-least-once; unchanged logical storage is effectively-once via
record_uid + version_hash + SeenStore.
```

`--recovery-only` remains available for diagnostics. Kabutan full rollout must
remain probe-gated while AWS WAF reports Human Verification.

Phase M2 connects the read-only ResumePlanner to the production concurrent
runtime after startup recovery. Profiles now skip checkpoints that their site
plugin can prove are whole-scope durable before invoking the crawler. Naver and
Toss partial/page checkpoints remain conservative and continue through their
existing resume logic. Fine-grained recovery-stage classification was
completed in M3.

Phase M3 persists exact scope membership in new recovery manifests and resumes
each durable stage from its minimum remaining action. In particular, an
`uploaded` manifest verifies the recorded NAS object without uploading it
again; `verified` proceeds to Catalog, and `catalog_registered` proceeds to
SeenStore. Older unscoped manifests still recover globally and are never
assigned to a scope by filename heuristics.

Phase M4 adds the internal ProgressAggregator. Runtime results now include
ResumePlan-seeded platform/site/dataset progress with record, storage, queue,
recovery, and performance metrics. Interactive and plain-text dashboard
selection became user-facing in Phase M5.

Phase M5 exposes progress controls:

```bash
# Automatic: Rich Live on a capable TTY, text in logs/non-TTY
python -m crawl_framework.cli.main crawl --site naver_finance \
  --profile naver_incremental --progress

# Force periodic plain text
python -m crawl_framework.cli.main crawl --site naver_finance \
  --profile naver_incremental --progress-style text \
  --progress-interval-seconds 5

# Disable all progress output
python -m crawl_framework.cli.main crawl --site naver_finance \
  --profile naver_incremental --no-progress
```

`--json` always disables the dashboard so stdout remains machine-readable.
Rich is optional; requesting it when unavailable falls back to text.

Phase M6 Ctrl-C behavior:

```text
first Ctrl-C  stop new scopes and drain accepted durable pipeline work
second Ctrl-C cancel remaining workers faster and preserve resume state
exit code      130 for an interrupted, resumable run
```

When `--run-manifest` is enabled, the interrupted result and shutdown state are
written before exit. Resume by running the same profile/command again. Do not
clear SeenStore, checkpoints, RecoveryStore, Catalog, or warehouse files.

The M11 real four-site small smoke accepted this behavior with two instruments
per instrument-scoped site. Transport processes such as Playwright may close
during SIGINT; when shared shutdown is already active, discovery/crawl
teardown is classified as `INTERRUPTED`, while durable writer/upload/Catalog
errors remain failures. Kabutan WAF remains a non-fatal `BLOCKED` site.
