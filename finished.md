# Finished

> 本文件只记录已经由当前 `main` 分支真实代码、测试或生产运行验证完成的能力。
>
> “接口/骨架存在”不等于“真实站点爬取已完成”。

## Last verified baseline

```text
conda activate pac
pytest -q -p no:rerunfailures tests/unit
642 passed
```

## Core Framework

- Phase A core correctness 已完成。PostgreSQL `data_files` UPSERT 不再把已上传文件的物理状态回退，也不会由普通注册覆盖 `storage_status`、`remote_path`、`lifecycle_status`。
- SeenStore、checkpoint、durable barrier、recovery、cleanup 的核心语义已经建立，并有回归测试。
- Query Layer 主要能力已经完成：
  - single / multi instrument；
  - local natural date + timezone；
  - UTC partition pruning；
  - single / multi bucket pruning；
  - multi-bucket 单 SQL；
  - Arrow predicate pushdown；
  - streaming RecordBatch；
  - hard `max_rows`；
  - JSONL / Parquet export；
  - query observability；
  - large-universe query。
- Storage maintenance 已具备 generic compaction、audit、repair、retention/cleanup。
- Recovery robustness 已具备 fault injection、idempotent recovery、crash-boundary 相关测试。
- PostgreSQL file-level Catalog、Parquet record side index、SeenStore、checkpoint、query、uploader、recovery 都已经是站点无关组件。

## Generic Parallel Production Runtime

真实 production CLI 已能在 stage worker 数大于 1 时使用 `ConcurrentProductionRuntime`。

当前生产架构：

```text
CLI
-> AppFactory
-> CrawlBootstrap
-> ConcurrentProductionRuntime
-> crawl workers
-> bounded record queue
-> writer workers
-> bounded upload queue
-> upload workers
-> bounded catalog queue
-> catalog/index workers
-> SeenStore/checkpoint
```

已完成通用能力：

```text
configurable crawl workers
configurable writer workers
configurable upload workers
configurable catalog workers
bounded queues / backpressure
target file size / max rows / max age flush policy
optional coalesced scope flush for large production rollouts
parallel upload/catalog stages
recovery manifests
production pipeline overlap statistics
```

`StoragePipeline` durable lifecycle 已拆成可复用阶段：

```text
prepare_batch
upload_prepared_batch
catalog_uploaded_batch
```

I15 small-file mitigation 已落地：

- production CLI/runtime 支持 `--coalesce-scope-flushes`；
- 大规模 rollout 不再在每个 scope 完成时强制 flush Parquet；
- completed scope 等 size/final batch 完成 upload/catalog 后再
  checkpoint；
- `RsyncUploader` 支持缓存已创建远端目录，避免同目录重复
  `ssh mkdir -p`；
- production rollout 可用 `--trust-rsync-success`，在 rsync
  成功时跳过每文件额外 `ssh stat`，Catalog 仍记录本地
  size/sha256；
- `scripts/run_naver_forum_full_rollout.py` 默认启用
  `--coalesce-scope-flushes`、`--trust-rsync-success`、实时
  progress passthrough，并使用 16 crawl / 4 upload / 4 catalog
  workers；
- 50-instrument 真实 production validation 已通过：
  `success=true`，`records_crawled=991`，`files_written=44`，
  `uploads_completed=44`，checkpoint completeness audit 通过。
- 500-instrument 真实 production validation 已通过：
  `success=true`，`records_crawled=9802`，`files_written=257`，
  `uploads_completed=257`，checkpoint completeness audit 通过。
- 100-instrument 新默认并发 validation 已通过：
  `success=true`，`records_crawled=1991`，`files_written=54`，
  `uploads_completed=54`，checkpoint completeness audit 通过。
- I15 full retry 已确认真实剩余瓶颈边界：
  `upload_workers=8` 会触发 NAS/SSH
  `kex_exchange_identification: Connection reset by peer`；
  recovery-only 已清理干净：`attempted=9`，`recovered=9`，
  `remaining_pending=0`。
- I15 / H20 full-market forum rollout 已完成。使用同一
  `naver_finance_kr_rollout_universe.txt` 的全部 3924 instruments，
  真实 production run 成功：
  `success=true`，`records_crawled=74880`，`files_written=4086`，
  `uploads_completed=4086`，`catalog_jobs_completed=4086`，
  `max_record_queue_depth=8`，`max_upload_queue_depth=128`，
  `max_catalog_queue_depth=3`。Completeness audit：
  `completed_scopes=3924`，`missing_checkpoints=0`，
  `pending_recovery=0`，`terminal_failed_recovery=0`。最终
  recovery-only：`attempted=0`，`remaining_pending=0`。
- I16 preflight/runtime 修复已完成：
  `naver_full` 不再把 forum/news/research 默认覆盖为 unbounded
  historical backfill；默认使用 production plugin 的 bounded
  page policy，仍覆盖 full universe、全部内容 dataset、全部
  research categories 和 PDF attachment。`RsyncUploader` 现在为
  ssh/rsync 子命令提供 timeout 和有限 retry，避免远端命令永久
  挂住 worker。
- I16 small-file production fix 已完成代码接入并通过单测：
  production CLI 新增 `--ssh-multiplex`，`RsyncUploader` 使用
  OpenSSH ControlMaster 降低大量小文件上传时的 SSH 握手成本；
  默认 ControlPath 使用短路径 `/tmp/cfw-%C`，避免 macOS Unix
  socket path too long。
- I16 post-dataset compaction 已完成代码接入并通过单测：
  production CLI 新增 `--compact-after-dataset` 和
  `--compaction-min-file-count`；`ConcurrentProductionRuntime`
  支持每个 dataset drain 完成后执行 generic compaction hook；
  compaction 按现有安全分区
  `site/country/dataset/partition_date/bucket` 合并多个 active
  uploaded 小 Parquet，上传 replacement，注册 replacement，
  再把源小文件标记为 `superseded`。这不会改变当前 bucket/query
  语义，也不会直接删除 NAS 上的旧物理文件。
- 当前 PostgreSQL/NAS recovery 已再次验证：
  两次中断后的 recovery-only 均成功，
  `attempted=2`，`recovered=2`，`remaining=0`。
- 当前 I16 `naver_full` production run 已用新参数重启并进行中：

```text
--coalesce-scope-flushes
--trust-rsync-success
--ssh-multiplex
--compact-after-dataset
--compaction-min-file-count 2
```

  截至最新观察，run 已进入 `forum_post` 抓取阶段并持续推进；
  I16 还不能标记为生产完成，需等待 full profile 完整结束、
  post-dataset compaction summary、completeness audit、recovery-only、
  Catalog/query/PDF smoke 通过。

## Naver Instrument Universe

- Naver Finance market-sum discovery 已接入当前框架。
- 权威 universe snapshot：

```text
config/universes/naver_finance_kr_rollout_universe.txt
```

- 当前 snapshot：

```text
3924 canonical instruments
```

- canonical ID：

```text
XKRX:NNNNNN
```

## Naver Discussion Board / Forum Posts

当前框架已经接入真实 Naver discussion-board crawler：

```text
/item/board.naver
-> board_read.naver / nid discovery
-> m.stock.naver.com/front-api/discussion/detail
-> forum_post CanonicalRecord
```

旧 `yaodongwen/naver` 项目把这类数据称为“评论”；在新框架中更准确地建模为：

```text
forum_post
```

已接入/验证的字段和行为包括：

```text
nid
instrument/code
title
writer/nickname
written_at
content
view/recommend/dislike source fields
stable logical identity
checkpoint
SeenStore dedup
Parquet
upload
Catalog/index
recovery
query
```

生产验证：

- H17：3-instrument Naver smoke 成功。
- H18：first-50 Naver production run 成功。
- H18.5：真实 CLI 的 crawl/write/upload/catalog 分阶段并发已经接线并验证 overlap。
- H19：同一 snapshot 的 first-500 Naver `forum_post` rollout 已完成。
- H20 / I15：同一 snapshot 的 full 3924-instrument Naver
  `forum_post` rollout 已完成。
- 中断后 recovery 曾验证 `remaining_pending=0`。

## Naver News Articles

I1 已完成真实 Naver stock news client 迁入，并通过 production CLI smoke。

I2 已完成当前框架内的 news relation modeling：

- `news_article` 以 `office_id:article_id` 作为全局文章身份；
- 同一篇文章不再因为股票 scope 不同而生成不同 `record_uid`；
- `news_article` 顶层 `instrument_id=None`，关联证券写入 `relations_json`；
- `news_instrument` 已作为物化关系 dataset 接入 production plugin；
- query / streaming query 已支持通过 `relations_json` 查询 `news_article` by instrument/date。

当前真实路径：

```text
/item/news_news.naver
-> news_read.naver article_id / office_id discovery
-> n.news.naver.com/mnews/article/{office_id}/{article_id}
-> news_article CanonicalRecord
-> generic concurrent pipeline
-> Parquet/upload/Catalog/SeenStore/checkpoint
```

已迁入/验证的旧实现知识：

```text
finance.naver.com HTML euc-kr
n.news.naver.com UTF-8
Referer behavior
warmup sequence
article_id + office_id source identity
canonical n.news URL
title selector fallback
body selector fallback
source/author/published_at parsing
pagination
429 retry/failure behavior
full/incremental mode surface
at-least-once page checkpoint
news_instrument materialized relation records
relation-aware query
```

生产验证：

```text
python -m crawl_framework.cli.main crawl \
  --site naver_finance \
  --dataset news_article \
  --instrument 005930 \
  --max-pages 1 \
  --crawl-workers 1 \
  --writer-workers 1 \
  --upload-workers 1 \
  --catalog-workers 1 \
  --json

success=true
raw_count=13
normalized_count=13
files_written=1
files_uploaded=1
files_registered=1
errors=0
remaining_pending=0
checkpoint page=1
last source_id=014:0005568783

python -m crawl_framework.cli.main crawl \
  --site naver_finance \
  --dataset news_instrument \
  --instrument 005930 \
  --max-pages 1 \
  --crawl-workers 1 \
  --writer-workers 1 \
  --upload-workers 1 \
  --catalog-workers 1 \
  --json

success=true
raw_count=12
normalized_count=12
files_written=1
files_uploaded=1
files_registered=1
errors=0
remaining_pending=0
```

## Site Adapter / Attachment Foundations

以下基础设施/接口已经完成：

- generic `SiteAdapter` capability boundary；
- `NaverFinanceAdapter` 的 instrument/news/forum/research capability surface；
- `research_report` metadata normalization；
- `research_report -> AttachmentRequest` 的 PDF attachment discovery 接口；
- generic `AttachmentRecord`；
- SHA256；
- PDF basic validation；
- deterministic attachment path；
- local attachment store；
- generic HTTP attachment downloader；
- generic attachment download/store/upload pipeline；
- generic proxy transport 基础能力；
- worker-count / queue-size config。

## Naver Research Reports

I3 已完成真实 Naver research metadata client 迁入，并通过 production CLI smoke。

I4 已完成当前框架内的 research relation modeling：

- `research_report` 一份报告正文只存一次；
- company report 的相关股票进入 `research_report.relations_json`；
- global market/economy/industry/debenture 等无单一股票报告允许 `instrument_id=None` 且无 relations；
- `research_instrument` 已作为物化关系 dataset 接入 production plugin；
- adapter surface 也可 normalize `research_instrument`。

当前真实路径：

```text
/research/*_list.naver
-> *_read.naver?nid={report_id}
-> research_report CanonicalRecord
-> generic concurrent pipeline
-> Parquet/upload/Catalog/SeenStore/checkpoint
```

已迁入/验证的旧实现知识：

```text
six research categories:
  market
  invest
  company
  industry
  economy
  debenture
all category alias
EUC-KR/CP949/UTF-8 tolerant HTML decoding
list table parsing
nid/report_id extraction
company stock_code/stock_name extraction
industry classification extraction
detail source/date/views parsing
detail content parsing
company investment opinion / target price parsing
pdf_url metadata extraction
pagination
full/incremental mode surface
at-least-once page checkpoint
research_instrument materialized relation records
research PDF attachment download/store/upload metadata path
```

生产验证：

```text
python -m crawl_framework.cli.main crawl \
  --site naver_finance \
  --dataset research_report \
  --research-category market \
  --max-pages 1 \
  --crawl-workers 1 \
  --writer-workers 1 \
  --upload-workers 1 \
  --catalog-workers 1 \
  --json

success=true
raw_count=30
normalized_count=30
records_seen=30
records_updated=30
files_written=29
files_uploaded=29
files_registered=29
errors=0

python -m crawl_framework.cli.main crawl \
  --site naver_finance \
  --dataset research_instrument \
  --research-category company \
  --max-pages 1 \
  --crawl-workers 1 \
  --writer-workers 1 \
  --upload-workers 1 \
  --catalog-workers 1 \
  --json

success=true
raw_count=30
normalized_count=30
files_written=24
files_uploaded=24
files_registered=24
errors=0

python -m crawl_framework.cli.main crawl \
  --site naver_finance \
  --dataset attachment \
  --research-category market \
  --max-pages 1 \
  --attachment-limit 1 \
  --crawl-workers 1 \
  --writer-workers 1 \
  --upload-workers 1 \
  --catalog-workers 1 \
  --attachment-workers 1 \
  --json

success=true
raw_count=1
normalized_count=1
errors=0
attachment_id=fb9ddeaac1ea68c4175de61e0ff7de9d9c6dec6120f3cd06a8b21945fb1a2298
```

## Multi-site Reuse Proof

- H21 multi-site reuse proof 已完成。
- TossInvest mock/fixture adapter 已证明第二站点可以复用通用 runtime、storage pipeline、uploader、Catalog、record index、checkpoint、recovery、query，而不需要重新实现基础设施。

## Phase I Progress

### I0. Production stability blockers

已完成：

- Upload stderr handling 已加固：已知远端 locale warning 只有在命令 exit code 成功时才被视为 non-fatal noise。
- 非零退出码的 remote mkdir / SSH / rsync 失败仍然 fatal。
- `ConcurrentProductionRuntime` 已加固 worker exception propagation：worker 出错会被主 runtime 收集， sibling workers 会被确定性取消，并抛出统一的 `ConcurrentRuntimeError`，避免 orphan task / `Task exception was never retrieved`。

验证：

```text
pytest -q -p no:rerunfailures tests/unit/test_uploader.py tests/unit/test_concurrent_production_runtime.py
16 passed

pytest -q -p no:rerunfailures tests/unit
530 passed

python -m crawl_framework.cli.main crawl --site naver_finance --recovery-only --json
success=true
remaining_pending=0
```

### I6. Naver production entry point

已完成：

- `src/crawl_framework/sites/builtin.py` 继续把 `naver_finance`
  注册到唯一 production `SitePlugin`：`NaverFinancePlugin`；
- `NaverFinanceAdapter` 明确作为兼容 facade，而不是第二条
  production CLI 入口；
- adapter 支持的 dataset 已与当前 Naver production 内容对齐：
  `forum_post`、`news_article`、`news_instrument`、
  `research_report`、`research_instrument`、`attachment`；
- adapter 的 canonical normalization 已统一委托
  `NaverFinancePlugin.normalize()`；
- adapter 中重复的 research normalization 实现已移除。

验证：

```text
pytest -q -p no:rerunfailures tests/unit/test_naver_finance_adapter.py tests/unit/test_builtin_sites.py tests/unit/test_naver_finance_plugin.py
60 passed

pytest -q -p no:rerunfailures tests/unit
578 passed
```

### I7. Dataset-specific runtime options

已完成：

- CLI 新增 `--forum-max-pages`、`--news-max-pages`、
  `--research-max-pages`；
- CLI 新增 `--news-mode=incremental|full`、
  `--research-mode=incremental|full`；
- CLI 新增 `--download-research-pdf` /
  `--no-download-research-pdf`；
- CLI 新增 `--research-detail-workers`、`--pdf-workers`；
- `make_crawl_context()` 会把这些 dataset-specific options
  写入 `CrawlContext.extra`；
- dataset-specific max-page flags 优先于旧的 `--max-pages`；
- `--pdf-workers` 在未显式指定 `--attachment-workers` 时映射到
  generic `attachment_workers`；
- `dataset=attachment` 会尊重 `download_research_pdf=false`，
  不下载 PDF、不调用 attachment pipeline。

验证：

```text
pytest -q -p no:rerunfailures tests/unit/test_cli_main.py tests/unit/test_app_factory.py tests/unit/test_naver_finance_plugin.py
101 passed

pytest -q -p no:rerunfailures tests/unit
584 passed
```

### I8. Per-dataset resource budgets

已完成：

- 新增 generic `DatasetResourceBudget`，用于描述站点无关的
  dataset-level budget；
- CLI 新增 `--forum-crawl-workers`、`--news-crawl-workers`、
  `--research-crawl-workers`；
- CLI 新增 `--forum-http-concurrency`、
  `--news-http-concurrency`、`--research-http-concurrency`；
- `make_crawl_context()` 会生成
  `CrawlContext.extra["dataset_budgets"]`；
- `ConcurrentProductionRuntime.run_dataset()` 会读取当前 dataset
  的 `crawl_workers` budget，并真实改变该 dataset 使用的
  crawl worker 数；
- `--pdf-workers` 继续作为 generic attachment worker budget
  的 Naver 语义入口。

验证：

```text
pytest -q -p no:rerunfailures tests/unit/test_concurrency_config.py tests/unit/test_cli_main.py tests/unit/test_app_factory.py tests/unit/test_concurrent_production_runtime.py
70 passed

pytest -q -p no:rerunfailures tests/unit
587 passed
```

### I9. Adaptive rate limiting / proxy health

已完成：

- 新增 generic `AdaptiveRateLimiter`；
- 支持 per-endpoint failure/throttle state；
- 支持 403/429 throttled backoff；
- 支持 5xx/request exception failure backoff；
- 支持 jitter；
- 支持连续失败后的 circuit-open delay；
- success 会清理 endpoint delay/circuit state；
- generic `HttpTransport` 可以把 HTTP status / exception 结果报告给 limiter；
- Naver forum/news/research 真实 HTTP client 都支持注入
  `rate_limiter`；
- `NaverFinancePlugin` 默认 production 构造路径会创建一个共享
  limiter，并传给 forum/news/research clients。

验证：

```text
pytest -q -p no:rerunfailures tests/unit/test_transports.py tests/unit/test_naver_news.py tests/unit/test_naver_research.py tests/unit/test_naver_forum_post.py tests/unit/test_naver_finance_plugin.py
89 passed

pytest -q -p no:rerunfailures tests/unit
593 passed
```

### I10. Live progress logging

已完成：

- CLI 新增 `--progress-interval-seconds`；
- live progress 默认关闭，只在显式开启时输出；
- JSON 输出模式不会安装 live progress reporter，保持 stdout
  机器可解析；
- `ConcurrentProductionRuntime` 会按 interval 生成
  `ProgressSnapshot`；
- AppFactory 提供 text progress reporter，输出到 stderr；
- progress 字段包含 dataset、scope 完成/发现数量、pending
  scopes、records、files、uploads、Catalog jobs、queue depths、
  crawl/upload/catalog busy time。

验证：

```text
pytest -q -p no:rerunfailures tests/unit/test_cli_main.py tests/unit/test_app_factory.py tests/unit/test_concurrent_production_runtime.py
70 passed

pytest -q -p no:rerunfailures tests/unit
599 passed
```

### I11. Naver one-click production profiles

已完成：

- CLI 支持 `--profile naver_full`；
- CLI 支持 `--profile naver_incremental`；
- 两个 profile 默认读取同一个 deterministic rollout universe：
  `config/universes/naver_finance_kr_rollout_universe.txt`；
- 两个 profile 默认包含：
  `forum_post`、`news_article`、`news_instrument`、
  `research_report`、`research_instrument`、`attachment`；
- `naver_full` 默认使用 full news/research mode、全部 research
  categories、下载 research PDF，并把未显式设置的 page limit
  解释为 unbounded；
- `naver_incremental` 默认使用 incremental news/research mode、
  全部 research categories、下载 research PDF；
- 用户显式传入的 dataset/instrument/mode/page/PDF 参数会覆盖
  profile 默认值。

验证：

```text
pytest -q -p no:rerunfailures tests/unit/test_cli_main.py tests/unit/test_app_factory.py tests/unit/test_naver_finance_plugin.py
113 passed

pytest -q -p no:rerunfailures tests/unit
605 passed
```

### I12. Run manifest

已完成：

- CLI 支持 `--run-manifest`；
- CLI 支持 `--run-manifest-path`；
- `naver_full` / `naver_incremental` profile 默认启用 run
  manifest；
- 默认写入位置：
  `state/run_manifests/<run_id>.json`；
- manifest 记录 run_id、site、profile、datasets、options、
  start/end timestamp、universe snapshot path/count/SHA256、
  startup recovery stats、runtime stats、success flags。

验证：

```text
pytest -q -p no:rerunfailures tests/unit/test_cli_main.py
46 passed

pytest -q -p no:rerunfailures tests/unit
609 passed
```

### I13. Completeness audit

已完成：

- 新增 `scripts/check_rollout_completeness.py`；
- 可读取 run manifest、universe snapshot、本地 checkpoint 和
  recovery manifests；
- 审计 expected scopes、completed scopes、missing checkpoints、
  failed runtime scopes、pending recovery、terminal failed recovery、
  catalog-registered file count 和 pending attachment recovery；
- 缺少完整性证据时返回非零 exit code；
- 支持 JSON 输出，供后续 smoke matrix / production rollout 自动化使用。

当前边界：

- Catalog 侧目前使用 run manifest/runtime stats 作为证据；
- 尚未直接连接 PostgreSQL 做 online Catalog 深审计；
- I13 本身不代表 Naver full-content 已完成，真实 smoke matrix
  仍由 I14 验收。

验证：

```text
pytest -q -p no:rerunfailures tests/unit/test_rollout_completeness_audit.py
3 passed

pytest -q -p no:rerunfailures tests/unit
612 passed
```

### I14. Real production smoke matrix

已完成：

- 新增 `scripts/run_naver_smoke_matrix.py`；
- smoke matrix 从同一个 deterministic rollout universe 派生：
  1 instrument、3 instruments、50 instruments；
- smoke 命令走真实 production CLI、`naver_incremental` profile、
  Naver HTTP、PostgreSQL、NAS/rsync、generic concurrent runtime、
  Parquet/upload/Catalog/index、SeenStore、checkpoint/recovery、
  research PDF attachment path；
- Smoke 1、Smoke 2、Smoke 3 均真实运行通过；
- Smoke 3 的最新 manifest 已通过 completeness audit。

真实验证结果：

```text
Smoke 1:
instrument_limit=1
research_category=market
attachment_limit=1
success=true
remaining_pending=0

Smoke 2:
instrument_limit=3
research_category=all
attachment_limit=1
success=true
remaining_pending=0
research_report raw_count=180
research_instrument raw_count=29

Smoke 3:
instrument_limit=50
research_category=all
attachment_limit=3
success=true
remaining_pending=0
records_crawled=2888
catalog_jobs_completed=1017
manifest=state/run_manifests/9207d4f1241548c0aada9067523bd654.json
completeness audit complete=true
```

真实 smoke 暴露并修复：

- CLI/profile 传入 `research_category=("all",)` 时，
  `NaverResearchClient` 现在会正确展开六类 research category；
- 单篇 Naver news article 缺失可解析正文时，不再导致整个
  production run 失败；该 article 会被跳过并记录到
  `NaverNewsClient.detail_errors`，不会写入伪造正文。

验证：

```text
pytest -q -p no:rerunfailures tests/unit/test_naver_smoke_matrix.py tests/unit/test_naver_news.py tests/unit/test_naver_research.py tests/unit/test_rollout_completeness_audit.py tests/unit/test_recovery.py tests/unit/test_retry_policy.py
124 passed

pytest -q -p no:rerunfailures tests/unit
621 passed
```

## Important clarification

当前真正 production-ready 的 Naver 内容类型是：

```text
instrument discovery
forum_post
news_article
research_report
attachment
```

以下目前只有接口/normalization/通用基础设施，尚未完成真实 production crawler：

```text
none for Phase I datasets through I5, but large-scale rollout remains pending
```

# Legacy source traceability

Detailed function-level mapping is in `legacy_reference.md`.

Already migrated from `yaodongwen/naver`:

| Capability | Old source | Legacy functions/knowledge | Current status |
|---|---|---|---|
| Instrument universe | `Knaver_crawler/naver_news_core.py` | `parse_stock_page`, `get_market_stocks`, `get_all_stocks` | Migrated and improved in `web/naver/market_sum.py` |
| Discussion list | `Knaver_crawler/naver_comment_core.py` | `get_comment_list` | Migrated into current `forum_post` client |
| Discussion detail | `Knaver_crawler/naver_comment_core.py` | `get_comment_detail` and `front-api/discussion/detail` behavior | Migrated into current `forum_post` client |
| Forum pagination/incremental semantics | `Knaver_crawler/naver_comment_core.py` | `crawl_stock_comments` | Functionality represented by current plugin/checkpoint/SeenStore architecture |
| News list/detail crawler | `Knaver_crawler/naver_news_core.py` | `warmup`, `get_html`, `news_url`, `parse_news_list`, `parse_article`, `crawl_stock_news` | Migrated into `NaverNewsClient` and wired through production `NaverFinancePlugin` |
| Research six-category list/detail | `naver_research/naver_research_core.py` | `ResearchListItem`, `parse_research_list`, `parse_research_detail`, `crawl_research_category` | Migrated into `NaverResearchClient` and wired through production `NaverFinancePlugin` |
| Research category URLs/options | `naver_research/naver_research_config.py` | six category paths, retry/sleep/detail options | Category URL and controlled category selection migrated; richer dataset-specific runtime options continue in I7 |
| Research PDF attachment path | `naver_research/naver_research_core.py` | `extract_pdf_url`, PDF validation/download concepts | Migrated as generic `AttachmentPipeline` plus Naver `attachment` dataset smoke |
| Legacy cache concepts | `Knaver_crawler/naver_cache.py` | SQLite WAL, cache keys, claim/existence concepts | Superseded by stronger SeenStore inspect/commit semantics; old claim timing must not return |

Not yet migrated as real production site capability:

| Capability | Old proven source | Still missing in new production path |
|---|---|---|
| Full-scale attachment rollout | `naver_research/naver_research_core.py` | PDF semaphore/retry/full category behavior | one-PDF smoke passed; larger PDF rollout remains pending |

A feature must not be moved from TODO to Finished merely because a FakeClient test passes. The real HTTP implementation, production wiring, generic durable pipeline, and real smoke test must all pass.
