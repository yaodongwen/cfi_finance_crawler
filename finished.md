# Finished

> 本文件只记录已经由当前 `main` 分支真实代码、测试或生产运行验证完成的能力。
>
> “接口/骨架存在”不等于“真实站点爬取已完成”。

## Last verified baseline

```text
conda activate pac
pytest -q -p no:rerunfailures tests/unit
647 passed
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

2026-09-07 补齐 old `naver_research_core.py` 已验证的
repeated-final-page protection：

- 新增 `build_research_list_signature(items)`；
- `NaverResearchClient.crawl_pages()` 对每个 research category
  独立维护 page signature；
- repeated signature 检测发生在 list fetch 和 empty-page 逻辑之后、
  detail fetch 之前；
- full / incremental 两种模式都生效；
- incremental `existing_pages` stop 保持独立语义；
- deterministic unit tests 覆盖 empty page、repeated final page、
  per-category signature reset、incremental existing-pages stop；
- 只读真实 HTTP smoke 已验证 `market` full mode 前 3 页均可解析。

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

# Finished Update — TossInvest Current Facts

> Do not delete existing framework/Naver completed history.
>
> Add/update the following Toss section.
>
> Do NOT mark real Toss crawling as complete yet.

# TossInvest Completed Foundations

## Architecture compatibility proof

Completed:

```text
TossInvest site is registered in builtin site registry
TossInvestPlugin exists
canonical XKRX symbol mapping exists
forum_post normalization exists
news_article normalization exists
RecordRelation generation exists
generic runtime/storage/query architecture reuse has fixture-backed tests
```

Current plugin datasets:

```text
forum_post
news_article
```

Current significance:

```text
The generic framework can structurally host TossInvest without redesigning
Parquet, Catalog, SeenStore, checkpoint, recovery, upload, query or
ConcurrentProductionRuntime.
```

## Important limitation

The following must NOT be listed as finished yet:

```text
real Toss Screener discovery
real Toss forum/community HTTP/Playwright crawl
real Toss news list crawl
real Toss news detail crawl
full old-field parity
real Toss Playwright production transport
real Toss production rollout
```

Those remain in `problem.md` and Phase J of `todo.md`.

## Legacy project capabilities available for migration

The old repository already contains proven implementations for:

```text
domestic Screener discovery
virtual-list scrolling
forum/community crawl
forum incremental mode
news full historical baseline
news incremental mode
news pending resume
metadata-v4 news parser
Playwright concurrency
proxy use
legacy persistent ID cache
Ctrl+C resume semantics
```

These are reference inputs for Phase J, not yet completed target-framework features.

## J0 repository audit and legacy traceability

Completed on 2026-09-07 against the current worktree and the local legacy
checkout at `crawl_tossinvest/tossinvest_crawler_v2`.

The audited capability classification is:

```text
A real production-wired:
  builtin site registration and generic runtime/storage/query infrastructure

B fixture-backed only:
  TossInvestPlugin.crawl() and architecture/runtime smoke coverage

C interface/normalization only:
  XKRX symbol mapping, forum_post normalization, news_article relations

D missing before Phase J implementation:
  generic Playwright transport, real Screener discovery, real forum client,
  real news list/detail client, browser ProxyPool wiring, real rollout
```

Reviewed legacy sources include `crawler.py`, `collect_stocks.py`,
`normalizer.py`, `cache_manager.py`, `production_runner.py`, `main.py`, and
`README.md`. Only Toss-specific navigation, selectors, parsing, identity,
field provenance, and resume semantics are migration inputs; legacy JSONL,
site-owned dedup/Parquet/Catalog/upload and subprocess runner architecture are
explicitly excluded.

## J1 generic Playwright transport/browser worker pool

Completed on 2026-09-07 in
`src/crawl_framework/transports/playwright.py`.

Implemented generic Playwright/Chromium start-stop lifecycle, bounded browser
leases, isolated ephemeral contexts or per-worker persistent profile paths,
locale/timezone/viewport and timeout configuration, context recycling, crash
cleanup, graceful shutdown, and ProxyPool selection/success/failure reporting.
No Toss DOM selectors or storage behavior were added to the transport.

Validation:

```text
focused Playwright/transport tests: 11 passed
full tests/unit: 651 passed
real Chromium smoke: data page title=cfw-smoke, workers=1, max_active=1
```

## J2 real Toss instrument discovery

Completed on 2026-09-07 with the real Toss Screener and production plugin
discovery interface. `TossInvestInstrumentClient` switches to the domestic
market, protects baseline filters while removing extras, parses real virtual
rows, scrolls to a stable bottom, deduplicates by `stock_key`, and maps
`A005930` to `XKRX:005930`.

The authoritative snapshot is
`config/universes/tossinvest_kr_rollout_universe.txt`: `2427` current KR
instruments in Toss discovery order. It was generated from the real Screener,
not fixtures or a historical hard-coded list.

Validation:

```text
focused tests: 20 passed
full tests/unit: 655 passed
real full discovery: 2427 instruments
real plugin smoke: 5 instruments, expected stable prefix
```

## J3 real Toss forum/community client

Completed on 2026-09-07. The real client opens a stock community, selects the
latest ordering, parses virtual cards, supports full/incremental stopping, and
automatically warms a fresh Toss SPA profile through the Screener before
retrying a direct stock deep-link.

The final durable smoke used the formal AppFactory/Bootstrap composition and
the production TossInvestPlugin with the generic concurrent runtime, bounded
queues, Parquet, LocalUploader verification, PostgresCatalog API, record
index, SeenStore, checkpoint and RecoveryStore.

```text
success=true raw_count=7 normalized_count=7
records_new=7 unchanged=0 updated=0
files_written=1 uploaded=1 verified=1 registered=1
pipeline_errors=0 remaining_pending=0 checkpoint=complete
crawl/writer/upload/catalog workers=2/1/1/1
max queue depth record/upload/catalog=8/1/1
queue capacity record/upload/catalog=1000/128/128
focused tests=47 passed; full tests/unit=658 passed
```

## J4 Toss forum full field parity

Completed on 2026-09-07. Legacy `normalizer.py::normalize_comment`,
`parse_relative_comment_time`, and the real card parser fields are mapped to
CanonicalRecord/payload: post ID, author, profile image, approximate event
time plus quality/raw text, follower/shareholder flags, content/raw text,
engagement counts, stock name/URL and Toss stock key.

Logical identity uses the real post ID. Engagement changes retain the same
`record_uid` but produce a new `version_hash`; forum records use the framework
`latest` policy. The legacy standalone Normalizer/JSONL chain was not copied.

Validation: focused `11 passed`, full unit `659 passed`; real durable
regression smoke succeeded with 10 raw/normalized records, one file through
write/upload/verify/register, zero errors and pending recovery.

## J5 real Toss news list discovery

Completed on 2026-09-07. Production plugin wiring uses the real Playwright
news list client, Toss `contentParams.id` identity, virtual-list stable-bottom
scrolling and stock relation evidence. Historical baseline incomplete and
incremental history-only stopping remain distinct. Real Samsung smoke found
16 links in two rounds. Focused tests `14 passed`; full unit `663 passed`.
This does not yet mark the `news_article` dataset complete; detail parsing and
durable real smoke remain J6-J8 acceptance work.

## J6 real Toss news detail parser

Completed on 2026-09-07. Detail extraction follows page JSON, metadata,
article headings, list text, then auditable publisher/date/author fallbacks.
All required provenance and raw fields are preserved; absent metadata remains
blank. Real Samsung detail and AppFactory durable smokes passed.

```text
raw/normalized=1/1; new=1
files written/uploaded/verified/registered=1/1/1/1
errors=0 pending=0 checkpoint=complete
focused=44 passed; full unit=666 passed
```

## J7 Toss news article/instrument relations

Completed on 2026-09-07. `news_article` uses global identity so the same Toss
`news_id` reached from multiple stocks has one body record. `news_instrument`
materializes one `news_id:XKRX:*` relation per stock. Real relation durable
smoke persisted one record through all stages with pending 0 and complete
checkpoint. Focused `51 passed`; full unit `667 passed`.

## J8 Toss historical baseline and pending/resume semantics

Completed on 2026-09-07. Baseline-incomplete crawls never enable known-ID
history stopping. Failed details are retained in per-scope checkpoint state
and retried before newly discovered links; durable file failures remain in
the generic RecoveryStore. A generic `checkpoint_after_scope` hook allows
zero-record state updates while preserving post-Catalog checkpoint ordering.

Focused tests `72 passed`; full unit `669 passed`. Real durable smoke stored
`news_baseline_complete=false`, `pending_news_count=0`, and an empty pending
list with checkpoint complete and generic recovery pending 0.

## J9 browser worker pool and ProxyPool production wiring

Completed on 2026-09-08. Browser settings and multiple proxy endpoints load
from generic config. AppFactory creates the pool for browser-capable plugins;
Bootstrap starts it after recovery and closes it on completion/failure.
Proxy selection, success/failure reporting, cooldown, isolated profiles and
recycling remain generic. Real AppFactory forum durable smoke passed with one
record and pending 0. Focused `63 passed`; full unit `670 passed`.

## J10 dataset-specific browser concurrency/backpressure

Completed on 2026-09-08. Generic browser leases support named bounded budgets
for instrument discovery, forum, news list and news detail while retaining the
global worker bound. Toss clients select those generic channels. Existing
ConcurrentProductionRuntime queues continue to bound record/upload/catalog
stages. Focused `59 passed`; full unit `671 passed`.

## J11 one-stock real production smoke

Completed on 2026-09-08 with `XKRX:005930` / `A005930`. The real production
CLI ran `forum_post`, `news_article`, and `news_instrument` through Playwright,
CanonicalRecord, bounded ConcurrentProductionRuntime queues, Parquet, rsync
upload/verification, PostgreSQL Catalog and record index, SeenStore and scope
checkpoint. The three runs wrote, uploaded, verified and registered one file
each with zero pipeline errors.

Read-only Catalog/NAS query smoke returned 4 forum rows and 7 instrument-
matched rows from each news dataset. Final recovery-only reported
`attempted=0`, `remaining_pending=0`, and `terminal_failed=0`. Focused tests:
`70 passed`; full unit suite: `671 passed`.

## J12 four-stock concurrent production smoke

Completed on 2026-09-08 from the first four canonical IDs in the frozen
2427-instrument snapshot. Canonical snapshot input now maps correctly to Toss
`A*` source keys. Real forum smoke crawled 21 records; news article smoke
crawled 28 records; browser leases reached the configured global maximum of
2 with no lease failures or profile collisions.

Production intervals prove overlap: news upload began while later scopes were
still crawling, and Catalog registration overlapped both. The stable list-only
`news_instrument` path avoids duplicate detail fetches, and an immediate
36-record repeat wrote zero files. Generic remote compaction materialization
was fixed to use the Catalog resolver; the final active relation view contains
48 unique record UIDs in 4 files. Final recovery pending is zero. Focused tests:
`63 passed`; full unit suite: `677 passed`.

## J13 twenty-stock production smoke

Completed on 2026-09-08 using the frozen snapshot prefix. Real production
results were forum `107`, news article `180`, and news relation `180` records,
with 20/20 scopes completed and no browser lease failures. Browser concurrency
reached 4; news list/detail each reached their configured budget of 4. Peak RSS
was about 554 MB for forum, 763 MB for article, and 722 MB for relation, with
zero swap. Queue depths remained within configured bounds and recovery pending
was zero. A 20-instrument read-only query hit 20 active relation Parquet files
and returned 192 accumulated unique relations. Focused `63 passed`; full unit
`677 passed`.

## J14 one-hundred-stock production rollout

Completed on 2026-09-08 using the first 100 canonical IDs from the same frozen
2427-instrument snapshot. All 100 scopes completed for each production dataset:
forum crawled 550 records and durably registered 197 files; article crawled 900
records and registered 236 files; relation crawled 900 records and registered
82 files. Browser concurrency reached 4 with no lease failures. The forum run
observed upload-queue backpressure (`68` blocked puts), while every measured
queue depth remained within its configured bound.

All article and relation checkpoints reported zero pending news. Recovery-only
reported `remaining=0` and `terminal_failed=0`. A read-only PostgreSQL
Catalog/NAS query for `XKRX:005930` read 2 active relation Parquet files and
returned 25 rows. Peak RSS was about 547 MB for forum and 791 MB for relation,
with no swap. Focused tests: `74 passed`; full unit suite: `677 passed`.

## J15 full Toss KR universe rollout

Completed on 2026-09-09 from the unchanged authoritative
`config/universes/tossinvest_kr_rollout_universe.txt` snapshot (`2427`
instruments). The interrupted run resumed from existing checkpoints; no
durably completed prefix was cleared or regenerated.

```text
forum_post checkpoints       2427/2427
news_article checkpoints     2427/2427
news_instrument checkpoints  2427/2427
pending news scopes          0
recovery remaining/failed    0/0
```

All resumed article and relation segment manifests report `success=true` and
none failed. PostgreSQL and NAS match on all `23349` Toss Parquet paths with
zero missing, orphaned, or size-mismatched files. All active Catalog files are
uploaded: forum `9458`, article `12706`, relation `1158`.

A real read-only query for `XKRX:005930` returned three forum records from NAS
through the PostgreSQL Catalog in 1.72 seconds. Bounded upload queues reached
their configured capacity during rollout and drained successfully. Focused
tests: `99 passed`; full unit suite: `684 passed`.

## J16 one-click Toss profiles

Completed on 2026-09-09. The production CLI now provides `toss_incremental`
and `toss_full`, both defaulting to the frozen 2427-instrument Toss universe
and all three Toss datasets. Incremental mode preserves a completed per-scope
news baseline and pending-detail resume semantics. Full mode disables history
early-stop while retaining safe one-round forum/news bounds. Explicit CLI
dataset, instrument, mode, and page limits override profile defaults.

The optional `toss_historical_backfill` profile is also available without
implicit page bounds; it is intentionally separate from safe `toss_full` and
has not been represented as a completed full-history production run.

Real Samsung production smokes for both required profiles each crawled and
normalized forum/article/relation `3/9/9` rows through the generic concurrent
runtime. Each wrote, uploaded, and Catalog-registered three Parquet files with
zero pipeline errors and zero pending recovery. Focused tests: `113 passed`;
full unit suite: `691 passed`.

# Kabutan guidance for finished.md

Do NOT mark Kabutan production crawling as finished at Phase K start.

The only initial completed fact is:

```text
A user-supplied standalone reference crawler exists and proves Kabutan-specific
list/detail/pagination behavior.
```

This does not count as framework production support.

After each accepted K item, append only proven facts.

## K0 Kabutan repository and legacy audit

Completed on 2026-09-10. The standalone `get_all.py` proves the Kabutan list,
detail, monthly pagination, Japanese header, retry, and repeated-page site
knowledge described by the integration plan. The target framework currently
has no Kabutan registration or production implementation. Generic durable
runtime/storage/recovery/query components are reusable; generic HTTP
production composition still needs K6 work. Baseline: `691 passed`.

## K1 Kabutan site registration

Completed on 2026-09-10. Added the `kabutan` builtin package and registry
entry with `country=JP`, `timezone=Asia/Tokyo`, and only
`dataset=news_article`. The package is disabled by default and deliberately
does not claim parsing or production HTTP support yet. Focused: `4 passed`;
full unit: `693 passed`.

## K2 Kabutan list parsing and stable identity

Completed on 2026-09-10. The framework now parses `table.s_news_list`,
`td.news_time`, plausible categories, and the first `n\d{12}` article link,
while de-duplicating repeated tables by Kabutan `news_id`. Kabutan records keep
their month scope but use a global logical identity boundary, so page/month do
not alter the record identity. No real HTTP claim yet. Focused: `12 passed`;
full unit: `698 passed`.

## K3 Kabutan article detail parsing

Completed on 2026-09-10. Added detail parsing for `article`, `h1`,
`time.s_news_date`, fallback `time[datetime]`, category fallback, and cleaned
`div.body` content. Invalid/missing detail datetime remains absent rather than
being guessed from list time; missing article/body is explicitly represented
for later failure handling. No real HTTP claim yet. Focused: `11 passed`; full
unit: `703 passed`.

## K4 Kabutan month discovery

Completed on 2026-09-10. Month scopes are stable, inclusive, instrument-free,
and use `scope_id=YYYY-MM` plus `source_key=YYYYMM`. Full history begins at
2013-09; incremental discovery dynamically includes the Tokyo current month
and configured prior-month overlap. Focused: `16 passed`; full unit:
`708 passed`.

## K5 Kabutan protected pagination

Completed on 2026-09-10. Added page-streaming pagination with empty-page,
repeated-signature, and repeated-ID-subset termination before detail fetches.
The max-page guard records `safety_capped` and never natural completion. No
real HTTP claim yet. Focused: `20 passed`; full unit: `712 passed`.

## K6 Generic HTTP production wiring for Kabutan

Completed on 2026-09-10. AppFactory now composes HTTP-required plugins through
the existing generic transport boundary using a standard-library requester,
ProxyPool, AdaptiveRateLimiter, and configurable retries for
429/500/502/503/504. Kabutan contributes only its URL, query parameters, and
Japanese headers. Real-site acceptance remains K10. Focused: `65 passed`;
full unit: `715 passed`.

## K7 Kabutan durable month checkpoint semantics

Completed on 2026-09-10. Month state now records year, month, last completed
page, last news ID, completion, and stop reason. The plugin supplies only a
candidate checkpoint; generic Runtime persists it after the durable barrier.
Failures remain incomplete and resume starts at the next durable page. No
site-owned recovery store was added. Focused: `47 passed`; full unit:
`717 passed`.

## K8 Kabutan CLI and profiles

Completed on 2026-09-10 and amended on 2026-09-11. Added
`kabutan_incremental` and `kabutan_free_full`; `kabutan_full` is a compatibility
alias for free-full semantics. Both support
validated start/end month, overlap, and per-month safety-cap overrides.
Profiles select only `news_article`, enable run manifests, and never require an
instrument or universe file. Real one-click acceptance remains K14. Focused:
`114 passed`; full unit: `721 passed`.

## K9 Kabutan manifest and completeness audit

Completed on 2026-09-10. Run manifests freeze the exact ordered month scope
plan. The generic rollout audit now evaluates Kabutan by expected month and
requires `month_complete=true`; a partial safety-cap checkpoint cannot pass by
merely existing. Focused: `85 passed`; full unit: `723 passed`.

## K10 Kabutan real read-only HTTP smoke

Completed on 2026-09-10. The generic production HTTP stack fetched real
Kabutan current-month page 1 (15 items) and detail `n202609100904` (valid
article/body/detail datetime; 4,748 content chars). The real page omits
`<tbody>`, unlike lxml-normalized legacy DOM, so the parser now supports both
shapes. This was read-only and does not yet prove durable production storage.
Focused: `22 passed`; full unit: `724 passed`.

## K11 free-access durable production acceptance

The 2026-08/09 runs proved real HTTP through the generic durable pipeline,
interruption-safe resume, SeenStore de-duplication, Parquet upload, PostgreSQL
Catalog/index, checkpoint/recovery, and Query Layer behavior. They produced 30
active/uploaded files and 9,362 rows with no pending recovery.

Accepted on 2026-09-11 against the current free range. Real HTTP produced 30
normalized public articles, one coalesced Parquet file, one NAS upload, and one
Catalog/index job with zero pipeline errors or pending recovery. The checkpoint
ended at the public boundary (`free_access_complete=true`), and Query Layer
returned all 30 records for the Tokyo local date with real detail content and
source URLs.

## K12 free-access interruption/resume/dedup acceptance

Accepted on 2026-09-11. A production incremental run was interrupted during
HTTP crawl, recovery-only returned zero pending/failed manifests, and the
subsequent repeated run completed without writing, uploading, or cataloging a
duplicate file. The earlier durable 2026-08/09 runs also remain valid evidence
for batch resume and SeenStore identity de-duplication; they are not claims of
Premium history completeness.

## Kabutan remote-access control handling

Completed on 2026-09-11. Verified AWS WAF responses now raise the explicit
`KabutanWafBlockedError` only for HTTP 405 plus Human Verification and AWS body
markers. Free-full preflight aborts before scope discovery or durable writes,
does not advance checkpoint/free-access completion, and performs no in-run WAF
retry. `scripts/probe_kabutan_access.py` provides a secret-free, one-request
manual AVAILABLE/WAF_BLOCKED check. This does not complete K13 while remote
access remains blocked. Focused: `48 passed`; full unit: `747 passed`.

After K14, a final section may state:

```text
Kabutan Market News production integration COMPLETE

site=kabutan
country=JP
timezone=Asia/Tokyo
dataset=news_article
scope=month

real list HTTP        YES
real detail HTTP      YES
monthly pagination    YES
repeat-page guard     YES
checkpoint/recovery   YES
Parquet/upload        YES
Catalog/index         YES
query                 YES
kabutan_incremental   YES
kabutan_free_full     YES
Premium history       NOT INCLUDED
```


# Phase L guidance for finished.md

Do NOT mark HKEX financial-report integration complete at Phase L start.

At L0, the only source-backed fact is:

```text
A standalone HKEX reference implementation exists and proves:
- exact stock-code -> stockId lookup;
- report type codes 40100/40200/40300;
- HKEX title search;
- PDF URL/result metadata parsing;
- conservative retry behavior;
- PDF magic validation;
- local interruption resume.
```

This is reference knowledge only, not current framework production support.

After each accepted L item, append only what current code/tests/real production
have actually proven.

## L0 repository audit and legacy traceability

Completed on 2026-09-11. The supplied
`../finance_report/hk/get_all_finance_report.py` was traced function by
function against the current dataset, plugin, HTTP, attachment, durable
runtime, recovery, Catalog/index, manifest/completeness, and query APIs.

The audit proves only reusable source knowledge: exact five-digit HKEX prefix
matching, report codes `40100/40200/40300`, title-search parameters, PDF result
parsing, conservative HTTP retry, PDF magic validation, and interruption
resume behavior. It does not claim current HKEX production support. Legacy
CSV output, local PDF layout, failed CSV, completed-URL cache, and progress
JSON are deliberately excluded from migration. Focused architecture tests:
`47 passed`; full unit baseline: `747 passed`.

## L1 generic financial-report dataset contract

Completed on 2026-09-11. Added registered, cross-site
`financial_report` (versioned metadata) and
`financial_report_instrument` (immutable relation) datasets. Both use the
existing canonical Arrow schema, Parquet writer, Catalog/index, and query
machinery. The existing generic `attachment` contract remains the physical
binary boundary. This is schema acceptance only, not HKEX site acceptance.
Focused tests: `67 passed`; full unit: `753 passed`.

## L2 HKEXnews package and builtin registration

Completed on 2026-09-11. Added the disabled-by-default `hkexnews` site package,
manifest, plugin skeleton, and idempotent builtin registration with
`country=HK`, `timezone=Asia/Hong_Kong`, and the two logical financial-report
datasets. Unimplemented source operations fail explicitly, so this milestone
does not claim real discovery, parsing, or HTTP support. Focused: `30 passed`;
full unit: `757 passed`.

## L3 exact HKEX stockId resolution

Completed on 2026-09-11 with deterministic tests. Added five-digit stock-code
normalization, canonical `XHKG:` IDs, strict JSONP parsing, and a transport-
injected `resolve_stock()` that accepts only an exact normalized code match.
The first autocomplete suggestion is never trusted. Focused: `28 passed`;
full unit: `770 passed`. Real endpoint proof remains L12.

## L4 HKEX report-search requests

Completed on 2026-09-11 with deterministic transport tests. The generic HTTP
request contract now supports POST form bodies, and the HKEX client emits the
verified title-search form for `40100` annual, `40200` interim, and `40300`
quarterly reports with validated configurable date bounds. No parser or real
HTTP result is claimed here. Focused: `34 passed`; full unit: `778 passed`.

## L5 HKEX report-result parser and taxonomy

Completed on 2026-09-11 with deterministic HTML tests. The parser accepts
PDFs only from result rows, canonicalizes and deduplicates HKEX URLs, extracts
release/code/name fields, prefers the complete final-cell document title, and
retains annual/interim/quarterly native codes. Cross-category duplicate PDFs
remain one candidate. Focused: `39 passed`; full unit: `783 passed`; real DOM
acceptance remains L12.

## L6 HKEX release-time normalization

Completed on 2026-09-11. Source-observed Chinese, slash-date, and compact
date-time values normalize from Asia/Hong_Kong to aware UTC. Missing,
date-only, unknown, and invalid values remain null; midnight is never invented.
Focused: `41 passed`; full unit: `793 passed`.

## L7 canonical financial-report normalization

Completed on 2026-09-11. HKEX result rows normalize to versioned
`financial_report` records and immutable `financial_report_instrument`
relations. Canonical PDF URL SHA-256 is the conservative source identity;
operational instrument scope does not alter logical identity. Source/scope
instrument mismatches fail, and unproven language/fiscal fields remain null.
Focused: `74 passed`; full unit: `798 passed`.

## L8 generic PDF attachment integration

Completed on 2026-09-11. HKEX logical reports now produce parent-linked generic
attachment requests and consume the existing downloader/store/uploader result;
no HKEX-specific binary path or uploader was added. Generic PDF validation was
strengthened to require `%PDF-` magic despite MIME claims, allow source-specific
minimum size, and atomically finalize local files. Focused: `89 passed`; full
unit: `801 passed`. Real PDF/NAS acceptance remains L13.

## L9 HKEX checkpoint and recovery semantics

Completed on 2026-09-11 with deterministic failure/resume tests. Instrument
scope state is keyed by a request signature covering dataset, instrument,
report types, and date range. A complete candidate exists only after search or
attachment work naturally finishes; exceptions leave the accepted checkpoint
unchanged. Full mode resumes matching durable scopes, while changed inputs do
not get skipped. Generic Runtime/RecoveryStore retain persistence ownership.
Focused: `98 passed`; full unit: `805 passed`.

## L10 HKEX CLI, profiles, and production composition

Completed on 2026-09-11 without real HTTP. Added `hkex_reports_incremental`
and `hkex_reports_full`, report type/date/lookback/PDF controls, canonical
instrument discovery, and AppFactory wiring into the generic concurrent HTTP
runtime and attachment pipeline. A deterministic 2,798-instrument XHKG
snapshot was generated from the supplied CSV in stable row order with source
SHA metadata. Focused: `156 passed`; full unit: `814 passed`. Production
acceptance remains L12-L18.

## L11 deterministic HKEX acceptance matrix

Completed on 2026-09-11. The focused matrix covers annual/interim/quarterly
taxonomy, exact prefix collisions and malformed stock IDs, all verified/null
release-time cases, canonical report identity, cross-category identity,
HTML-as-PDF rejection, size/magic/atomic attachment handling, profiles, and
production composition. Focused: `200 passed`; full unit: `820 passed`. These
tests do not substitute for L12 real HTTP.

## L12 one-instrument real read-only HTTP smoke

Completed on 2026-09-11. The real generic HTTP client resolved
`XHKG:00005` to HKEX `stockId=5` (`HSBC HOLDINGS`) and found three annual
reports since 2024. The first real row produced a canonical report UID and
`2026-03-26T23:30:00Z` event time. The smoke exposed and fixed real cell labels
(`Release Time`, `Stock Code`, `Stock Short Name`, `Document`) before canonical
normalization. No PDF or durable state was written. Focused: `58 passed`; full
unit: `821 passed`.

## L13 one-instrument durable metadata and PDF smoke

Completed on 2026-09-11 for `XHKG:00005`, annual, March 2026. Real metadata,
relation, and attachment datasets each produced one new canonical row and one
Parquet file, NAS upload/verification, and Catalog/index registration with zero
pipeline errors. All three checkpoints completed; recovery-only found zero
pending/failed work. Query Layer read the report and parent-linked attachment;
the PDF SHA was recorded and its 12,588,090-byte NAS object matched metadata.
Focused: `144 passed`; full unit: `821 passed`.

## L14 ten-instrument mixed report-type acceptance

Completed on 2026-09-11 and audited on 2026-09-15. Ten consecutive instruments
from the snapshot's first GEM segment completed metadata and relation durable
flows: 425 records per dataset, 10/10 scopes, zero UID duplicates, zero errors,
and zero recovery pending. Catalog/NAS Query returned annual=123, interim=119,
and quarterly=183. Focused: `140 passed`; full unit: `821 passed`.

This acceptance also measured a non-correctness performance defect: 425 report
rows occupied 425 event-day Parquet files, making remote materialization take
696.34 seconds while scanning took 1.12 seconds. L15 must prevent this pattern
for future runs before expansion; no existing NAS file is modified by that fix.

## L15 financial-report interruption/resume/dedup acceptance

Completed on 2026-09-15 using three consecutive authoritative snapshot entries
at offset 2369. Financial-report metadata and relation datasets now opt into a
generic yearly, single-bucket partition policy; all other datasets retain their
existing partition defaults. The interrupted run durably completed metadata
but not relations, recovery-only was clean, and the exact resume skipped all
three metadata scopes before completing the remaining relations. The final
identical rerun produced zero crawled records and zero new files.

The new metadata packaging stored 120 reports in 13 yearly Parquet files,
compared with the prior one-file-per-report behavior; 120 relations were stored
in one file. Existing L13/L14 NAS objects were not modified or compacted.
Focused: `183 passed`; the pre-acceptance full suite passed `823` tests.

## L16 100-instrument HKEX production rollout

Completed on 2026-09-15 for the first 100 canonical instruments in the
2,798-entry snapshot. Production CLI discovery completed 200 dataset scopes
and found 3,663 report rows plus 3,663 relation rows. Existing durable state
accounted for 479 metadata and one relation; the append-only run wrote 3,184
metadata rows in 20 yearly files and 3,662 relations in one file. All 21 files
were uploaded and cataloged with zero pipeline errors and zero recovery
pending. Direct NAS materialization verified every file's size and SHA256.

Real rollout findings were fixed and regression-tested: unavailable exact
stock matches become durable empty scopes without hiding HTTP/parser failures,
and explicit HKEX dual-counter code fields are validated as a set. Query
partition pruning now reads the same DatasetSpec bucket policy as the writer,
includes declared legacy 256-bucket files, and expands yearly date pruning to
the partition boundary. Both new `XHKG:00016` data and old `XHKG:08003`
relations were read through PostgreSQL Catalog/NAS. Focused: `110 passed`;
full unit: `831 passed`.

## L17 full configured HK universe rollout

Completed on 2026-09-16 by resuming the interrupted append-only run from its
existing checkpoints. All 2,798 canonical instruments completed both
`financial_report` and `financial_report_instrument`, for 5,596/5,596 durable
scopes with zero failed, partial, or missing scopes. The resume crawled 53,657
relation rows; SeenStore retained 53,079 new rows in five coalesced Parquet
files and deduplicated 578 existing rows. All five files were uploaded and
Cataloged, and their NAS sizes and SHA256 values match Catalog. Final
recovery-only reported zero pending and terminal-failed work.

The interrupted run exposed a generic async transport bug: adaptive limiter
delays used synchronous `time.sleep` inside `HttpTransport`. The limiter now
exposes a non-blocking delay calculation and the async transport awaits its
configured sleep, while the synchronous compatibility API remains available.

## L18 profiles, query, and completeness acceptance

Completed on 2026-09-16. Completeness now treats both financial-report
datasets as instrument-scoped and reports `expected_scopes=5596`,
`completed_scopes=5596`, and zero missing/failed/partial scopes. A real
`hkex_reports_incremental` run completed HTTP discovery, normalization,
SeenStore dedup, and checkpoints. A subsequent full-snapshot
`hkex_reports_full` run skipped every matching durable scope and produced zero
records/files. Query Layer read real report metadata and instrument relations
through PostgreSQL Catalog/NAS. Final focused tests: `232 passed`; full unit:
`837 passed`.

```text
HKEX Financial Reports production integration COMPLETE

site=hkexnews
country=HK
datasets:
  financial_report
  financial_report_instrument
  attachment

annual discovery          YES
interim discovery         YES
quarterly discovery       YES
exact stockId resolution  YES
stable report identity    YES
release-time normalization YES
PDF validation/SHA        YES
Parquet/NAS               YES
Catalog/index             YES
Seen/checkpoint/recovery   YES
query                     YES
hkex_reports_incremental  YES
hkex_reports_full         YES
```

## M0 Production orchestration audit

Completed on 2026-09-16 without running a production crawl or mutating durable
state. The audit traced CLI -> AppFactory -> CrawlBootstrap -> startup recovery
-> ConcurrentProductionRuntime -> StoragePipeline and inspected all current
single-site profiles.

Confirmed existing foundations:

```text
startup recovery before crawl                  YES
bounded record/upload/catalog queues           YES
write/upload/verify/Catalog/Seen ordering       YES
scope checkpoint after associated batches      YES
unchanged replay suppressed by SeenStore        YES
Catalog file-path registration idempotent       YES
single-site production profiles                 YES
basic non-interactive progress snapshots        YES
```

M0 is an audit completion only. ResumePlanner, restart-aware progress,
graceful interruption, global profiles, shared cross-site budgets, and a
platform manifest remain Phase M work and are not claimed complete here.
Focused architecture/recovery tests: `178 passed`; full unit suite:
`837 passed`.

## M1 Generic ResumePlanner / ResumePlan

Completed on 2026-09-16. Added a generic read-only planning model for:

```text
DURABLE_COMPLETE
RECOVERY_PENDING
INCOMPLETE
BLOCKED
FAILED_RETRYABLE
FAILED_TERMINAL
```

The planner consumes explicit evidence instead of interpreting opaque site
checkpoint fields. It provides stable aggregate counts, ordered serialization,
duplicate-scope rejection, and a crawlable-scope view. The checkpoint adapter
only calls `exists()`/`load()` and delegates completion/recovery/blocking
semantics to read-only callbacks. Production startup integration is M2.
Focused: `71 passed`; full unit: `847 passed`.

## M2 Recovery-first startup integration

Completed on 2026-09-16. The real AppFactory concurrent runtime now receives a
ResumePlanner. Startup order is:

```text
RecoveryOrchestrator
-> managed resources/runtime discovery
-> load post-recovery checkpoints
-> build ResumePlan
-> skip proven DURABLE_COMPLETE scopes before plugin.crawl
-> crawl remaining scopes
```

The runtime includes per-dataset resume plans in its result/run-manifest data.
Skipped durable scopes retain zero-work scope results without checkpoint
rewrites. Existing HKEX full-signature and Kabutan old-month completion rules
were moved behind the generic plugin hook without changing their semantics.
Focused: `100 passed`; full unit: `851 passed`. No production crawl or
durable-state mutation was used for M2 acceptance.

## M3 Resumable durable-stage classification

Completed on 2026-09-16. New recovery manifests persist sorted, deduplicated
scope tokens from their exact FlushBatch. Resume planning can therefore map
scope-owned manifests to:

```text
local                 -> UPLOAD_AND_VERIFY
uploaded              -> VERIFY_REMOTE
verified              -> REGISTER_CATALOG
catalog_registered    -> COMMIT_SEEN
seen_committed         -> ADVANCE_CHECKPOINT_MARKER
checkpoint_committed  -> CLEAN_LOCAL
cleanable             -> CLEAN_LOCAL
deleted               -> NONE
```

Retryable and terminal failures map to their corresponding ResumePlan states.
`LocalUploader` and `RsyncUploader` now implement verify-existing without data
transfer. Recovery from `uploaded` requires manifest `remote_path` and explicit
size/SHA verification policy, then proceeds without calling `upload()`.
Legacy manifests without scope tokens are never assigned by filename.
Focused: `149 passed`; full unit: `864 passed`.

## M4 ProgressAggregator

Completed on 2026-09-16. Added a thread-safe, renderer-independent progress
model under `observability/progress.py`. It aggregates:

```text
platform -> site -> dataset -> scopes
records: crawled/normalized/new/unchanged/updated/failed
storage: buffered/prepared/written/uploaded/verified/cataloged/compacted
queues: current and observed maximum depth
recovery: pending/attempted/recovered/retryable/terminal
performance: rates and crawl/write/upload/catalog busy time
```

ResumePlan seeds durable completion and checkpoint skips before runtime work,
so a 5-of-10 restart snapshot begins at 50%. Percentages are clamped to
0..100, completion is idempotently capped, ETA appears only after session work
provides a rate, and snapshots serialize cleanly to JSON. The production
runtime emits events and includes final aggregated progress in its result.
Focused: `69 passed`; full unit: `872 passed`.

## M5 Rich/Text progress dashboard

Completed on 2026-09-16. Added optional Rich Live and plain-text production
renderers over the M4 snapshots. Selection policy:

```text
auto + TTY + Rich installed  -> Rich Live
auto + non-TTY/log           -> periodic text
Rich requested but missing   -> text fallback
--no-progress                -> disabled
--json                       -> disabled, clean machine stdout
```

Both renderers show platform/site/dataset scope progress and expose resume
skips, blocked/failed state, records, storage stages, queues, recovery, rates,
and busy times. Runtime emits a final snapshot and closes the renderer even
through its outer cleanup path. Rich remains optional and was real-rendered in
the current `pac` environment. Focused: `130 passed`; full unit:
`881 passed`.

## M6 Graceful Ctrl-C resume

Completed on 2026-09-16. Production CLI and ConcurrentProductionRuntime now
share a generic two-stage ShutdownController:

```text
first Ctrl-C  -> stop discovery/new scope intake, drain write/upload/Catalog
second Ctrl-C -> cancel and await all workers, preserve durable/recovery state
```

An interrupted in-flight scope flushes records already accepted by the bounded
pipeline but does not call `checkpoint_after_scope`, so it cannot be falsely
marked whole-scope complete. Record-level checkpoint progress advances only
after the associated batches cross the existing durable barrier. Forced abort
skips end-of-run buffer work, leaves replayable state intact, gathers all
tasks, closes the progress renderer, and emits an interrupted runtime result.
Bootstrap maps that result to unsuccessful/resumable completion; CLI writes an
enabled run manifest and exits with code `130`. No site adapter contains signal
or queue logic. Focused: `144 passed`; full unit: `888 passed`. No production
crawl or durable production mutation was used for M6 acceptance.

## M7 Global profiles/orchestrator

Completed on 2026-09-17. Added the one-command platform entry and official
profile mappings:

```text
global_full:
  naver_finance -> naver_full
  tossinvest    -> toss_full
  kabutan       -> kabutan_free_full
  hkexnews      -> hkex_reports_full

global_incremental:
  naver_finance -> naver_incremental
  tossinvest    -> toss_incremental
  kabutan       -> kabutan_incremental
  hkexnews      -> hkex_reports_incremental
```

The generic PlatformOrchestrator runs the existing AppFactory/Bootstrap path in
stable order, isolates site failures, shares the M6 shutdown controller, and
supports platform recovery-only operation. Kabutan performs one real access
probe before its Bootstrap; verified WAF Human Verification becomes a
non-fatal `BLOCKED` site and later sites continue. JSON output is clean and
machine-readable. Named production profiles now always select the bounded
ConcurrentProductionRuntime even when conservative configuration gives every
stage one worker. Cross-site parallelism and shared stage budgets remain M8.
Focused: `148 passed`; full unit: `899 passed`. No production crawl ran.

## M8 Global resource budgets

Completed on 2026-09-17. `run-platform` now supports:

```text
--site-workers N
--global-writer-workers N
--global-upload-workers N
--global-catalog-workers N
```

Defaults are `2/2/2/2`; global upload concurrency is rejected above 4. Sites
run concurrently in one process while retaining their own crawl, browser, HTTP
and rate-limit settings. A shared observable GlobalStageBudget gates actual
Parquet preparation, upload/verification, Catalog/index/Seen/checkpoint work,
startup recovery, attachment store/upload, end flush, and post-dataset
compaction. It reports limit, current active, observed maximum, and waits for
each durable stage. First/second Ctrl-C still uses the shared M6 controller and
awaits all site/runtime tasks. Focused: `173 passed`; full unit: `907 passed`.
No production crawl or durable-state mutation was used for M8 acceptance.

## M9 Global run manifest

Completed on 2026-09-17. Every `run-platform` invocation now writes one atomic
aggregate JSON manifest before returning, including controlled exit code 130
interrupts and runs containing blocked or failed sites. The default location
is:

```text
state/run_manifests/platform_<profile>_<timestamp>.json
```

The manifest records platform run ID, profile and timestamps, final state,
site profiles, datasets, reproducible universe path/hash/count metadata,
per-site ResumePlans, scope/record/file/upload/Catalog counters, startup
recovery totals, shared resource-budget observations, blocked/failed sites,
errors, and each site's serialized runtime evidence. `--run-manifest-path`
provides an explicit destination. The orchestrator and manifest share the same
parsed site options, and the writer uses a same-directory temporary file plus
atomic replace; the existing single-site writer now uses the same primitive.
Focused: `193 passed`; full unit: `914 passed`. No production crawl or
durable-state mutation was used for M9 acceptance.

## M10 Deterministic resume/progress matrix

Completed on 2026-09-17 without production data mutation. Added a concentrated
acceptance matrix that executes the real ResumePlanner,
ConcurrentProductionRuntime, FileCheckpointStore, ProgressAggregator, bounded
writer/upload/Catalog stages, and ShutdownController. Covered states:

```text
cold / 50% partial / 100% complete restart
DURABLE_COMPLETE / RECOVERY_PENDING / INCOMPLETE / BLOCKED
FAILED_RETRYABLE / FAILED_TERMINAL
zero-record durable scope
unchanged replay with zero new durable work
attachment dataset and compaction metrics
first-interrupt incomplete-scope preservation
TTY Rich / non-TTY text / JSON-disabled progress paths
all persisted recovery-stage next actions
```

A profile whose three scopes are checkpoint-complete performs zero crawler
calls, zero record submissions, zero Parquet writes, zero uploads, and zero
Catalog jobs while progress starts and remains at 100%. An unchanged replay
advances safe scope completion but creates no durable file. Existing focused
tests supply the real SeenStore/StoragePipeline and RecoveryManager evidence
behind those runtime matrix cases. Focused: `249 passed`; full unit:
`923 passed`.

## M11 Interrupted small global smoke

Completed on 2026-09-17 with the real production CLI and existing durable
state. Command shape:

```text
run-platform --profile global_incremental --instrument-limit 2
--site-workers 4 --global-writer-workers 2
--global-upload-workers 2 --global-catalog-workers 2
--coalesce-scope-flushes
```

The deterministic prefixes were Naver/Toss `005930,000660` and HKEX
`00001,00002`; Kabutan used its one-request access probe and remained safely
`BLOCKED` by AWS WAF. A single Ctrl-C stopped new work and drained accepted
queues. The first attempt found a real integration bug: Playwright driver
closure during SIGINT was reported as a Toss site failure. The generic runtime
now treats discovery/crawl transport teardown as interruption only when shared
shutdown is already requested, and PlatformOrchestrator has the same fallback;
writer/upload/Catalog errors remain failures.

Final retry evidence:

```text
platform state        INTERRUPTED
naver_finance         INTERRUPTED
tossinvest            INTERRUPTED
hkexnews              INTERRUPTED
kabutan               BLOCKED (waf_human_verification)
failed_sites          0
pending recovery      0
pipeline errors       0
new Parquet files     0
uploads/Catalog jobs  0/0
Catalog rows since M11 0
```

Interrupted Naver checkpoints retained only safe page cursors and no false
whole-scope marker; a naturally completed HKEX scope retained its valid
completion checkpoint. Both aggregate manifests were preserved. Focused:
`113 passed`; full unit: `926 passed`.
