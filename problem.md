# Problems

> 本文件记录当前 `main` 分支真实存在的问题和功能缺口。
>
> 重点区分：
>
> - “接口已经声明”
> - “真实 Naver HTTP crawler 已接入 production CLI”
>
> 两者不是一回事。

## 1. H20 Naver Full-market Forum Rollout 已完成，仍建议后续 compaction 降低文件数

当前已经完成：

```text
3 instruments
50 instruments
500 instruments
```

full 3924-instrument `forum_post` production rollout 已最终验收完成。

I15 最新尝试：

```text
3924 instruments
forum_max_pages=1
crawl_workers=8
writer_workers=4
upload_workers=4
catalog_workers=2
target_file_size_mb=16
```

结果：

```text
run manually stopped after about 3 hours
exit_code=143
startup recovery recovered=5
remaining_pending=0
terminal_failed=0
```

不是数据损坏，也不是 parser/runtime correctness failure。
真实阻塞点是：

```text
many small Parquet files
+ per-file rsync mkdir/stat/upload verification
+ NAS latency
= full-market forum rollout cannot complete in an acceptable time
```

已完成第一轮修复：

```text
production CLI/runtime 新增 --coalesce-scope-flushes
scripts/run_naver_forum_full_rollout.py 默认启用
scope 完成不再强制 Parquet flush
checkpoint 等对应 coalesced batch upload/catalog 后提交
```

50-instrument 真实验证：

```text
success=true
records_crawled=991
files_written=44
uploads_completed=44
checkpoint completeness audit passed
```

I15 final accepted run:

```text
3924 instruments
success=true
records_crawled=74880
files_written=4086
uploads_completed=4086
catalog_jobs_completed=4086
completed_scopes=3924
missing_checkpoints=0
pending_recovery=0
remaining_pending=0
terminal_failed=0
final recovery-only attempted=0
```

Remaining optimization opportunity:

```text
scope-level small-file flush has been fixed.
remote mkdir/stat round trips have also been reduced.
I15 now completes, but full-market forum still writes 4086 files.
Future storage maintenance should compact older forum/date partitions
or introduce a controlled coarser partition policy if query semantics allow.
```

Additional speed fixes completed:

```text
RsyncUploader caches remote mkdir directories
--trust-rsync-success skips per-file ssh stat after successful rsync
rollout script streams progress instead of capture_output black-box mode
rollout defaults raised to crawl_workers=16 and catalog_workers=4,
while upload_workers was kept at 4 after testing the NAS limit
```

Full-market retry with upload_workers=8 found the NAS/SSH limit:

```text
error:
kex_exchange_identification: read: Connection reset by peer
Connection reset by 192.168.1.33 port 22

recovery-only:
success=true
attempted=9
recovered=9
remaining_pending=0
terminal_failed=0
```

Current recommended rollout defaults:

```text
crawl_workers=16
upload_workers=4
catalog_workers=4
coalesce_scope_flushes=true
trust_rsync_success=true
```

Latest validations:

```text
500 instruments:
success=true
records_crawled=9802
files_written=257
uploads_completed=257
checkpoint completeness audit passed

100 instruments with new defaults:
success=true
records_crawled=1991
files_written=54
uploads_completed=54
checkpoint completeness audit passed
```

## 2. Production Upload stderr 处理已加固，H20 中仍需持续观察

H20 中曾出现：

```text
setlocale: LC_ALL: cannot change locale (C.UTF-8)
```

已完成：

- 已知 locale warning 在命令 exit code 成功时不再导致失败。
- 非零退出码仍然 fatal。

I15 已确认：

- locale warning 旧 manifest 可通过非破坏性 reclassify 恢复；
- startup recovery 后 `remaining_pending=0`；
- 当前阻塞已不是 locale warning，而是小文件远端操作成本。

历史风险：

- stderr 中的无害 locale warning 可能被误判为远端 mkdir/upload 失败；
- 长时间 full-market run 可能因此不必要地中断。

## 3. Concurrent Runtime Worker Exception Propagation 已加固，仍需 H20 复验

H20 中曾出现：

```text
Task exception was never retrieved
```

已完成：

- worker failure 会被主 runtime 收集；
- sibling workers 会被取消；
- 主调用会收到统一 `ConcurrentRuntimeError`。

仍需：

- 在恢复 H20 full-market run 时确认不再出现 orphan async task。

历史风险：

- worker failure 没有统一被主 runtime 回收；
- sibling workers 可能不能确定性 shutdown；
- 长任务失败信息不够清晰。

## 4. Naver Adapter 仍是兼容 facade，不是独立 production runtime

I6 已确认并收敛当前架构：

```text
builtin registry -> NaverFinancePlugin
NaverFinanceAdapter -> compatibility facade
```

因此 production CLI 的唯一权威入口是 `NaverFinancePlugin`。
adapter 不再维护第二套 normalization 语义，而是委托
`NaverFinancePlugin.normalize()`。

剩余边界：

- 如果未来 framework runtime 直接消费 `SiteAdapter`，需要新增
  adapter-backed `SitePlugin` bridge；
- 当前不应把 adapter 当成第二条 production CLI 入口。

## 5. Naver News 500/full rollout 尚未完成

当前事实：

```text
src/crawl_framework/sites/naver_finance/news.py
```

已经迁入真实 list/detail crawler，并且 `NaverFinancePlugin.crawl("news_article", ...)` 已通过真实 CLI smoke。

I2 已完成：

- 全局 article identity；
- `news_article.relations_json`；
- `news_instrument` 物化关系 dataset；
- article-by-instrument/date query path。

I14 已完成 50-instrument real smoke：

```text
forum + news_article + news_instrument
all research categories
PDF attachment
success=true
remaining_pending=0
```

仍需后续复验：

- 500/full universe 规模下的 news_article/news_instrument rollout；
- compaction 后同一 article 多 instrument relation 的查询表现；
- relation-aware query 当前基于 Parquet `relations_json` 后置过滤，不是独立 PostgreSQL relation table。

## 6. Naver Research 六类真实小批量已 smoke，规模 rollout 尚未完成

参考仓库 `yaodongwen/naver/naver_research` 已支持：

```text
market
invest
company
industry
economy
debenture
all
```

并支持：

```text
report metadata
detail crawling
PDF download
concurrent detail/PDF workers
local state
Parquet
```

当前已经完成：

```text
NaverResearchClient
research_report production plugin wiring
market category real smoke
research_instrument production plugin wiring
company category relation smoke
generic pipeline write/upload/Catalog/checkpoint
```

I14 已完成：

```text
research_category=all
research_report raw_count=180
research_instrument raw_count=29
attachment real PDF path
remaining_pending=0
```

仍需：

- 验证所有 category 的 pagination stop condition；
- 验证大规模 research rollout。

因此当前可以说 `research_report` metadata client、
`research_instrument` 关系 dataset 和六类 category 小批量 smoke
已完成，但不能宣布全 research rollout 已完成。

## 6.1 Recovery store 历史 terminal pending 已修复，需持续复验

I3 开发过程中曾中断一次未受控的六类 research smoke。

当前：

```text
python -m crawl_framework.cli.main crawl --site naver_finance --recovery-only --json
success=true remaining_pending=0 terminal_failed=0
```

I14 前已将旧 locale-warning 误判 manifest 通过非破坏性
`RecoveryStore.reclassify_failed()` 重新分类为 retryable，并由
startup recovery 成功恢复。

## 6.1 I16 Full All-content Run 尚未完成生产验收

I16 latest attempts showed two production orchestration issues:

```text
1. naver_full previously forced unbounded forum/news/research history.
   This made one-click full-content run spend long time inside a small
   number of scopes instead of validating full-market all-content flow.

2. ssh/rsync remote commands could hang for hours without timeout.
   Observed stuck command:
   ssh -p 22 ... mkdir -p ...
```

Fixes completed:

```text
naver_full no longer forces unbounded page defaults
RsyncUploader command_timeout_seconds added
RsyncUploader command_attempts added
RsyncUploader retry_sleep_seconds added
empty remote mkdir stderr now reports unknown error
```

Previous blocker:

```text
recovery-only currently fails with:
bootstrap configuration failed: connection timeout expired
```

Current status:

```text
PostgreSQL/NAS connectivity restored.
recovery-only after interrupted I16 attempts succeeded:
attempted=2
recovered=2
remaining=0
```

The latest I16 run has been restarted with:

```text
--coalesce-scope-flushes
--trust-rsync-success
--ssh-multiplex
--compact-after-dataset
--compaction-min-file-count 2
```

It is currently in progress and must not be marked complete until:

```text
full naver_full profile exits success=true
post-dataset compaction summaries are present
completeness audit passes
recovery-only reports remaining_pending=0
Catalog/storage audit passes
query smoke passes
PDF sample integrity audit passes
```

## 6.2 I16 small-file bottleneck: code fix added, production effect still under validation

User concern:

```text
小文件不应逐个长期作为 active 查询面存在。
PostgreSQL Catalog 不应因为小文件过多而膨胀为大量 active rows。
```

Clarified current architecture:

```text
Catalog data_files is file-level, not one row per news/comment record.
RecordIndex is also per Parquet file sidecar, not the primary query Catalog.
```

Real issue:

```text
Parquet files were too small.
Because Catalog is file-level, too many small files also means too many
active Catalog rows and too many upload/Catalog operations.
```

Fix now implemented in code:

```text
--compact-after-dataset
--compaction-min-file-count N
```

The new production path:

```text
dataset crawl/write/upload/catalog drains
-> list active uploaded files from Catalog
-> group by site/country/dataset/partition_date/bucket
-> write one compacted replacement Parquet per eligible group
-> upload replacement
-> register replacement in Catalog
-> mark source small files superseded
```

Important safety boundary:

```text
Compaction does not merge across partition_date or bucket.
This preserves current partition pruning and bucket query semantics.
Compaction does not delete NAS physical source files; it only removes
them from the active logical query surface by setting lifecycle_status
to superseded.
```

Remaining validation:

```text
Current I16 run must reach dataset boundaries and execute the hook.
Need to confirm replacement_files/source_files reduction in manifest.
Need to confirm active Catalog rows shrink after compaction.
Need to confirm query reads replacement active files correctly.
```

## 6.3 SSH multiplexing fixed ControlPath issue, still needs long-run observation

During I16 restart, enabling SSH multiplexing first failed on macOS:

```text
unix_listener: path ".../crawl-framework-ssh-dwyao@192.168.1.33:22...."
too long for Unix domain socket
```

Fix completed:

```text
RsyncUploader default ControlPath is now /tmp/cfw-%C
```

`%C` is OpenSSH's hashed connection token, giving a short stable socket
path. Unit tests cover the default path and explicit custom ControlPath.

Remaining validation:

```text
Current long I16 run must confirm no further SSH multiplex path failures.
NAS/SSH still has a practical upload concurrency ceiling; previous
upload_workers=8 caused connection reset, so current production command
uses upload_workers=2.
```

## 7. Research PDF 大规模 rollout 与独立 attachment stage 尚未完成

当前已经完成：

```text
real Naver report
-> discover pdf_url
-> attachment task
-> generic HTTP download
-> validate PDF
-> sha256
-> local materialize
-> upload
-> remote verify
-> attachment metadata/index
```

真实 smoke 已完成：

```text
dataset=attachment
research_category=market
max_pages=1
attachment_limit=1
raw_count=1
normalized_count=1
errors=0
```

仍需：

- 扩大到每类小批量 PDF；
- 明确 attachment worker 在 production observability 中的独立统计；
- 将 attachment queue 作为 runtime stage 更显式地接入，而不是当前通过 `attachment` dataset crawl 阶段内调用 generic pipeline；
- 完成 PDF retry/recovery 的大规模验证；
- 验证 duplicate PDF SHA 不重复上传的策略。

## 7.1 Dataset-specific HTTP/detail budgets 尚未全部接到内部执行策略

I7/I8 已新增并验证：

```text
--research-detail-workers
--pdf-workers
--forum-crawl-workers
--news-crawl-workers
--research-crawl-workers
--forum-http-concurrency
--news-http-concurrency
--research-http-concurrency
```

当前真实状态：

- `--pdf-workers` 已作为 `attachment_workers` 的 Naver 语义别名，
  能影响 runtime 选择；
- per-dataset crawl worker budget 已进入
  `ConcurrentProductionRuntime.run_dataset()`，能真实改变
  当前 dataset 的 scope crawl worker 数；
- `--research-detail-workers` 和 per-dataset HTTP budgets
  已进入 `CrawlContext.extra.dataset_budgets`，
  但 `NaverResearchClient` 目前仍是逐 item detail fetch，
  尚未消费独立 detail worker pool / HTTP semaphore。
- I9 已加入 generic adaptive limiter，并把 Naver
  forum/news/research HTTP outcome 接入 limiter，但 client-level
  并发 semaphore 仍未消费 I8 的 per-dataset HTTP budgets。

这应在后续 client-level budget wiring 中继续完成。

## 8. “评论”术语容易混淆

旧 `yaodongwen/naver` 中所谓 comments 实际抓的是：

```text
Naver stock discussion board posts
```

对应：

```text
/item/board.naver
front-api/discussion/detail
```

新框架把它建模为：

```text
forum_post
```

这部分已经真实可用。

但如果“评论”指帖子下面的二级回复/嵌套回复，当前没有证据表明 Naver 提供且本项目已经实现；`comments.py` 当前为空。

## 9. 一键 Naver All-content Orchestration 尚未完成生产验收

当前已经完成：

```text
--profile naver_full
--profile naver_incremental
run manifest
rollout completeness audit command
```

但尚未经过 I14/I16 production smoke matrix 证明一条命令同时完成：

```text
instrument universe
+ all stock news
+ all forum posts
+ all research categories
+ research PDFs
+ package/upload/catalog/index/recovery
```

根本原因不是核心框架缺失，而是 full-content profile 仍需要
真实 smoke matrix、completeness audit、PDF/category 覆盖和
H20 forum full-market 验收。

## 10. Dataset-specific Crawl Options 已存在，但内部预算消费仍不足

当前已经新增：

```text
--forum-max-pages
--news-max-pages
--research-max-pages
--news-mode
--research-mode
--research-category
--download-research-pdf / --no-download-research-pdf
--research-detail-workers
--pdf-workers
per-dataset crawl/http budget flags
```

仍需：

```text
date boundaries
client-level HTTP semaphore consumption
research detail worker pool consumption
larger production validation
```

## 11. Parallel Checkpoint Semantics 对更复杂 dataset 仍需验证

当前 Naver forum 用 scope-level completion 已经可工作。

但 news/research 引入后会出现：

```text
multi-page
global research scopes
multiple categories
attachments
out-of-order task completion
```

需要确认 checkpoint/watermark 对这些真实任务形态仍安全。

## 12. Live Production Observability 已增强，仍需真实长跑复验

当前已经有：

```text
--progress-interval-seconds
ProgressSnapshot
stderr text reporter
queue depths
crawl/upload/catalog busy time
scope/record/file/upload/catalog counters
```

仍需在全市场新闻/研报/PDF 长跑中复验：

```text
PDF completed/failed
throughput
retry counts
pending recovery
```

## 13. Stale / Placeholder Files

当前仍存在：

```text
sites/naver_finance/comments.py  # empty
plugin.py.before_dedup.bak
```

`sites/naver_finance/news.py` 已不再是空文件，真实 news client
已经迁入并通过 smoke；剩余 stale 文件需要后续清理确认。

## 14. Completeness Audit 还不是在线 PostgreSQL 深审计

I13 已新增：

```text
scripts/check_rollout_completeness.py
```

当前审计来源：

```text
run manifest
universe snapshot
local checkpoints
local recovery manifests
runtime catalog/file stats
```

仍需后续增强：

```text
direct PostgreSQL Catalog count/query consistency checks
remote attachment existence/hash spot checks
dataset-level queryability checks from Catalog/index
```

## 15. 50-instrument smoke 暴露小文件上传成本偏高

I14 Smoke 2/3 均已通过，但真实运行显示：

```text
many small Parquet files
+ rsync mkdir/stat per file
+ remote verification
= smoke runtime noticeably long
```

当前这不是 correctness blocker，但进入 I15/I16 前应继续关注：

```text
target file size / row batching
partition granularity
upload worker tuning
Catalog/index worker tuning
```

# Legacy implementation status that must not be rediscovered

See `legacy_reference.md` for the full function-by-function extraction. The key unmigrated legacy knowledge is:

## News

Old source: `yaodongwen/naver/Knaver_crawler/naver_news_core.py`.

Already migrated into the current production Naver client/plugin path:

```text
warmup
create_session
get_html
news_url
parse_news_list
parse_article
crawl_stock_news
save_failed_news
```

Supporting old sources:

```text
Knaver_crawler/naver_news_config.py
Knaver_crawler/naver_cache.py
Knaver_crawler/run_naver_news.py
Knaver_crawler/retry_failed_items.py
```

Important correction during migration: old code often deduplicates using `article_id`; the new source identity should use `office_id:article_id`, with article body and instrument relation separated.

Remaining news work is production scale validation, not re-migrating
the legacy crawler.

## Research

Old sources:

```text
naver_research/naver_research_config.py
naver_research/naver_research_core.py
naver_research/naver_research_cache.py
naver_research/run_naver_research.py
```

Already migrated into the current production Naver research
client/plugin path:

```text
get_research_list_url
ResearchListItem
build_research_list_signature
extract_report_id
extract_stock_code
parse_research_list
ResearchDetail
extract_source_metadata
extract_detail_title
extract_pdf_url
extract_content
find_research_field_value
parse_research_detail
process_research_item
process_research_items
crawl_research_category
```

Remaining research work is six-category smoke coverage, PDF scale
validation, and all-content production rollout.

Critical old bug fixes that must be preserved:

```text
Naver can repeat the final real research page for out-of-range page numbers;
stop by repeated report-ID page signature, not only by empty page.

For company reports, prefer stock code/name obtained from the list item;
do not globally take arbitrary stock links from the detail page.

Target-price selector knowledge includes em.money.
Investment-opinion selector knowledge includes em.coment (Naver spelling).
```

## PDF

Old source: `naver_research/naver_research_core.py`.

Still needs semantic migration:

```text
configure_pdf_download_limits
get_pdf_path
is_valid_pdf_file
download_research_pdf
```

Preserve `%PDF` validation, Content-Length checking, maximum-size guard, streamed chunks, `.part` temporary file, atomic replace, bounded PDF concurrency, Referer and retries. Do not restore the old local JSON/PDF storage system as the new authoritative pipeline.
