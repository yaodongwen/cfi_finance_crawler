# Problems

> 本文件记录当前 `main` 分支真实存在的问题和功能缺口。
>
> 重点区分：
>
> - “接口已经声明”
> - “真实 Naver HTTP crawler 已接入 production CLI”
>
> 两者不是一回事。

## Phase M current gaps (M0 audit, 2026-09-16)

The production path has real bounded queues and a real durable barrier. Phase
M0-M10 has now supplied the orchestration foundations and deterministic test
matrix below; real interrupted/resumed production acceptance starts at M11:

```text
generic pre-crawl ResumePlanner                 PRODUCTION-WIRED (M2)
scope skip before plugin.crawl                  GENERIC FOR PROVEN COMPLETE
post-recovery resume-plan rebuild               IMPLEMENTED (M2)
uploaded-stage verify without uploader replay   RESOLVED (M3)
restart-aware progress aggregation              IMPLEMENTED (M4)
TTY/text dashboard rendering                    RESOLVED (M5)
graceful first/second Ctrl-C semantics           RESOLVED (M6)
global_full/global_incremental orchestrator      RESOLVED (M7)
cross-site writer/upload/catalog budgets         RESOLVED (M8)
interrupt/failure platform manifest              RESOLVED (M9)
```

These gaps do not invalidate the accepted site data. `CrawlBootstrap` already
runs RecoveryOrchestrator before the crawler; ConcurrentProductionRuntime uses
bounded record/upload/catalog queues; StoragePipeline preserves
write -> upload -> verify -> Catalog -> Seen -> checkpoint ordering; SeenStore
suppresses unchanged logical replay; and PostgreSQL file registration is
idempotent by `file_path`.

Important current limitations:

- Runtime loads checkpoints only after discovery and still calls each site's
  `crawl()`; full-scope skip logic currently lives in some plugins and is not
  uniform across Naver, Toss, Kabutan, and HKEX.
- Recovery resumes exact scope-owned durable stages. New `uploaded` manifests
  use verify-existing without replaying the upload; legacy unscoped manifests
  remain deliberately conservative.
- Progress is restart-seeded and aggregates platform/site/dataset scope,
  record, storage, queue, recovery, rate, and busy-time metrics. Rich and text
  renderers consume the same model; JSON mode remains clean.
- A configured single-site run manifest is written for controlled M6
  interrupts. M9 now always writes an atomic aggregate platform manifest for
  returned normal, WAF-blocked, failed, and controlled-interrupt outcomes.
  Process-kill/power-loss before the CLI's final write can still prevent a
  final aggregate manifest; durable recovery/checkpoint state remains the
  source of truth for that hard-abort case.
- `run-platform` provides global profiles, bounded site concurrency, and shared
  writer/upload/Catalog budgets. Site HTTP/browser/rate limits remain local by
  design.

Phase M must close these gaps without clearing SeenStore, checkpoints,
RecoveryStore, Catalog, warehouse files, or accepted production state.

M1 update: the generic planner and six-state classification were added under
`core/resume.py`. At M1 completion it was intentionally not production-wired.
Existing recovery manifests
also do not persist scope-token membership for coalesced batches; M3 must map
durable stages conservatively rather than inventing scope ownership.

M2 update: the planner is now production-wired after startup recovery, and
proven durable checkpoints are skipped before crawler invocation. The earlier
M1 wiring limitation is resolved. M3 remains necessary because current
coalesced RecoveryManifest records do not identify every contributing scope;
pending/failed stage evidence therefore cannot yet be safely assigned to a
specific scope.

M3 update: all newly written recovery manifests carry exact FlushBatch
`scope_tokens`, and uploaded-stage recovery is verify-only. Legacy manifests
created before M3 have no persisted scope membership. They remain supported by
startup RecoveryOrchestrator, but ResumePlanner deliberately leaves them
unassigned instead of inferring ownership from a path or dataset.

M4 update: restart-aware aggregation is implemented and production runtime
events feed it. The remaining UX gap is rendering and CLI policy: Rich/TTY,
plain non-TTY snapshots, progress enable/disable/style flags, and clean JSON
mode belong to M5.

M5 update: renderer and CLI policy are complete. Rich is intentionally an
optional dependency; environments without it use the same snapshot model
through the text renderer.

M6 update: production concurrent runs now use a shared two-stage shutdown
controller. The first interrupt stops new scope intake and drains accepted
records through the durable stages without applying whole-scope completion;
the second cancels and awaits all stage workers. Interrupted results retain
checkpoint/recovery state, close progress output, can write the configured run
manifest, and return exit code 130. M9 subsequently closed the cross-site
aggregate manifest gap.

M7 update: `run-platform --profile global_full|global_incremental` now maps and
runs all four official single-site profiles through their existing production
Bootstrap paths. Kabutan WAF is represented as a non-fatal blocked site after
a one-request preflight, while unrelated site failures are isolated and
reported. M8 subsequently added concurrent site scheduling and shared global
writer/upload/Catalog budgets; M9 added the atomic aggregate manifest.

M8 update: platform sites can now overlap under `--site-workers`, while a
single shared budget caps the real writer/Parquet, NAS upload/verify, and
Catalog durable stages across every site. Startup recovery, generic attachment
uploads, final flush, and compaction use the same permits and cannot bypass the
global NAS/Catalog limit. Default global upload concurrency is 2 and CLI
rejects values above 4. M9 now persists returned platform outcomes atomically.

M9 update: the aggregate platform manifest gap is resolved. `run-platform`
always serializes the exact parsed site profiles/universe snapshots together
with ResumePlans, counters, recovery, resource budgets, blocked/failed sites,
errors, and final/interrupted state. Same-directory temporary write plus atomic
replace prevents a partially written JSON document. Hard process termination
before finalization cannot produce a terminal manifest and must still be
audited from checkpoints and RecoveryStore. M10 covers this deterministically;
M11/M12 own the real interrupted/resumed small global acceptance.

M10 update: the deterministic resume/progress matrix is complete. Cold,
partial, and complete starts; all resume/recovery classifications; unchanged
replay; zero-row completion; attachment/compaction telemetry; renderer/JSON
selection; and controlled interruption now have one focused acceptance set.
No production correctness defect was exposed. Real interrupted/resumed
cross-site behavior remains deliberately unclaimed until the M11/M12 small
global production acceptance.

M11 update: the first real global Ctrl-C run exposed Toss Playwright driver
teardown as `FAILED` even though shared shutdown had already been requested.
This was a generic interruption-classification gap, not a Toss selector or
durable-storage failure. Discovery/crawl transport teardown during an active
shutdown now returns `INTERRUPTED`; errors before shutdown and all
writer/upload/Catalog failures still fail normally. The repaired smoke has no
failed sites or pending recovery. Exact rerun and physical/logical dedup proof
remain M12 work.

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

2026-09-07 update:

`NaverResearchClient` 已补齐 repeated-final-page signature
protection，并通过 deterministic unit tests 和只读真实 HTTP
market 前 3 页验证。

但是 production CLI 小规模 smoke：

```shell
python -m crawl_framework.cli.main crawl \
  --site naver_finance \
  --dataset research_report \
  --research-category market \
  --research-mode full \
  --research-max-pages 3 \
  --no-download-research-pdf \
  --crawl-workers 1 \
  --writer-workers 1 \
  --upload-workers 1 \
  --catalog-workers 1 \
  --json
```

被人工中断；堆栈显示停在
`RsyncUploader._ensure_remote_directory()` 的远端目录准备阶段。
这不是 repeated-final-page 逻辑失败，但意味着 production
CLI research smoke 仍需在 rsync/NAS 链路恢复后重新跑通。

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

# Problem Update — TossInvest

> Do not delete existing Naver/framework problems that are still real.
>
> Add/update the following Toss section.

# TossInvest Current Problems

J0 audit on 2026-09-07 confirmed that the items below describe the current
code accurately. The full unit baseline is `647 passed`; the existing Toss
tests prove framework compatibility only and do not constitute real-site
acceptance.

## T1. Toss production crawler is still fixture-backed

Current `TossInvestPlugin.crawl()` reads:

```text
ctx.extra["tossinvest_raw"]
```

instead of real TossInvest.

The plugin currently proves architecture reuse, not real website collection.

## T2. Real Toss comments/forum client is missing

Current:

```text
src/crawl_framework/sites/tossinvest/comments.py
```

is empty.

Legacy real implementation exists in:

```text
toss_nvest_crawl/crawler.py
```

functions:

```text
_open_community_and_sort_latest
_extract_visible_comment_cards
crawl_comments
```

Resolved by J3 on 2026-09-07. Real fresh-profile and durable AppFactory
smokes passed. The earlier durable smoke failure was only a direct helper
constructor omission of `record_queue_size`, `upload_queue_size`, and
`catalog_queue_size`; production AppFactory already propagated defaults and
required no generic runtime/config change.

## T3. Real Toss news client is missing

Current:

```text
src/crawl_framework/sites/tossinvest/news.py
```

is empty.

Legacy real implementation exists in:

```text
toss_nvest_crawl/crawler.py
```

functions:

```text
parse_news_id
collect_news_links
parse_news_article
crawl_news
```

J5 resolved the real list-discovery half on 2026-09-07 (16-link real smoke).
News detail extraction and complete durable dataset acceptance remain open in
J6-J8, so Toss news as a whole is not yet marked complete.

J6 resolved real detail extraction and durable single-article persistence on
2026-09-07. Relation materialization and historical state acceptance remain
J7-J8.

J7 relation materialization is resolved. Historical baseline and pending
detail resume semantics remain J8.

## T4. Generic Playwright transport is not production implemented

Current:

```text
src/crawl_framework/transports/playwright.py
```

is empty.

Toss requires Playwright because of virtual lists and dynamic UI.

Resolved by J1 on 2026-09-07: the generic bounded Playwright browser worker
pool is implemented and passed a real Chromium lifecycle smoke. Remaining
production wiring to Toss discovery/crawl is tracked by J2-J10.

## T5. Real Toss instrument discovery is not integrated

Legacy real discovery exists in:

```text
collect_stocks.py
crawler.py
```

and historically found about 2458 domestic stocks.

The new framework needs current discovery and a deterministic canonical snapshot.

Resolved by J2 on 2026-09-07. Real Playwright discovery produced a canonical
2427-instrument snapshot and a real plugin-interface smoke reproduced its
first five IDs. Future runs must still treat a severe count drop as suspicious.

## T6. Toss field parity is incomplete

The current fixture plugin does not preserve all old proven fields.

Missing/at-risk forum fields include:

```text
follower_count
is_shareholder
created_at_quality
created_at_text
created_at_raw
profile_image
raw_text
stock_name
stock_url
```

Missing/at-risk news fields include:

```text
title_available
title_source
publisher_source
published_date
published_at_source
author_source
raw_text
list_text
```

Forum-field portion resolved by J4 on 2026-09-07, including follower,
shareholder, relative-time quality, profile, raw text, stock provenance and
engagement semantics. News-field parity remains open for J5-J6.

## T7. News historical-baseline semantics are not integrated

Old Toss project distinguishes:

```text
historical baseline incomplete
historical baseline complete
```

Before baseline completion, known IDs cannot trigger early stop.

This semantic must be mapped into the new checkpoint/recovery model.

Resolved by J8 on 2026-09-07. Baseline and pending detail state now live in
the durable scope checkpoint; generic file-stage failures remain RecoveryStore
responsibility. Full-market historical completion is still a rollout task.

## T8. Legacy Toss state/data migration is not defined

Old state includes:

```text
PersistentIdSet shards
_pending_news_links.json
_news_state.json
old JSONL history
production_runner state
```

If old history must be retained, explicit one-time migration scripts are required.

## T9. Browser worker/proxy lifecycle is not generic yet

The new framework has generic `ProxyPool`, but Toss browser workers are not yet wired to it.

Need:

```text
bounded browser workers
isolated profiles/contexts
proxy lease/report success/failure
graceful recycle
```

Resolved by J9 on 2026-09-08. Normal AppFactory/Bootstrap now owns the generic
browser pool lifecycle and config-backed ProxyPool wiring; Toss no longer
depends on helper injection or one `TOSS_PROXY_SERVER`.

## T10. No real Toss production rollout exists in the new framework

Resolved by J11-J16 on 2026-09-09. The original fixture-only state no longer
describes the production adapter.

Need real:

```text
1
4
20
100
full universe
```

rollout sequence.

J11 resolved the first rollout step on 2026-09-08. One real Samsung Electronics
scope completed forum, article and article/instrument relation production
writes, Catalog/NAS query smoke, checkpoints and clean recovery. J12-J15 then
completed the 4/20/100/full rollout sequence.

J12 resolved the four-stock step on 2026-09-08. It exposed and fixed canonical
snapshot double-prefixing, sequential news detail work, unstable relation
metadata, missing remote-source materialization in compaction, and compaction
choosing a null-event older version. The repaired relation path is idempotent
and the active compacted view has no duplicate record UIDs.

No production proxy endpoints are configured in `config.yaml`; J12 therefore
used direct isolated browser contexts. Proxy rotation/failure/cooldown remains
covered by generic deterministic tests, but real proxy endpoint health has not
been measured and must not be reported as production-validated.

J13 passed at 20 instruments. Small-file amplification remains visible:
forum/article/relation wrote `36/28/16` files for `107/180/180` crawled rows.
This is bounded and recoverable but should be compacted according to the
generic retention/compaction policy during larger rollouts; it is not a reason
to reintroduce per-record Catalog rows or site-specific packaging.

J14 passed at 100 instruments. Coalescing reduced the relation path to 82 files
for 900 crawled records, but forum/article still produced 197/236 files for
550/900 records because durable partition boundaries and event-day buckets
limit cross-scope aggregation. This remains a generic compaction/retention
concern for the full rollout, not a Toss-specific index design issue.

Four obsolete forum checkpoint files with the former `AXKRX:*` double-prefix
scope names remain from the pre-J12 failed smoke. The 100 canonical `A*` scope
checkpoints are complete. The obsolete files are harmless compatibility state
and were not deleted because production-state deletion requires explicit
approval.

The interrupted J15 rollout resumed from its durable checkpoints and completed
on 2026-09-09. Forum, article, and relation datasets now each cover all 2427
snapshot instruments; pending news and recovery are zero. PostgreSQL and NAS
match on all 23349 Toss Parquet paths with no missing, orphaned, or
size-mismatched files.

The remaining performance concern is query-side historical small-file
amplification. An unbounded `news_article` query may materialize many article
files selected through instrument relations and was intentionally stopped
during acceptance. A bounded single-day `forum_post` query materialized one
of eight candidate files and returned three rows in 1.72 seconds. This is a
generic compaction/query-planning concern, not missing or inconsistent J15
production data.

J16 added and real-smoke-validated the required `toss_incremental` and
`toss_full` production profiles. The separate optional
`toss_historical_backfill` entry is wired and resumable, but a full-history
production run has deliberately not been claimed as complete.

# Kabutan — append to problem.md

## KAB1. Kabutan is not yet a registered production site

Need `kabutan` SitePlugin and builtin registration.

## KAB2. Supplied crawler is standalone

Legacy output:
```text
kabutan_data/marketnews.jsonl
kabutan_data/failed.jsonl
```

Do not preserve this as a second production storage/recovery architecture.

## KAB3. Kabutan is month/global scoped

Do not force through stock universe scopes.

Required:
```text
month scope -> news_article
instrument_id=None
```

## KAB4. End date is hard-coded in legacy script

Legacy:
```text
END_YEAR=2026
END_MONTH=9
```

Production must derive current month dynamically and allow overrides.

## KAB5. Historical completeness needs durable month checkpoint semantics

Need to distinguish:
```text
natural month end
partial max-page cap
HTTP/parser failure
interruption
```

A partial/capped month must not be marked complete.

## KAB6. One-click Kabutan profiles resolved

Implemented:
```text
kabutan_incremental
kabutan_free_full
kabutan_full
```

`kabutan_full` is only a compatibility alias for free-full behavior.

## KAB7. Generic HTTP production composition resolved

`HttpTransport`, `AdaptiveRateLimiter`, and `ProxyPool` exist, but AppFactory
currently composes only browser resources. The `pac` environment does not have
the optional `httpx` package installed. K6 must provide a reusable real HTTP
requester/resource path with status retries for Kabutan and future HTTP sites;
it must not become a Kabutan-specific requests/session subsystem. This was
resolved with the generic pooled `RequestsHttpRequester` and production
AppFactory wiring.

Resolved in K6: AppFactory now supplies a generic real HTTP transport with
configured proxies, adaptive limiting, and retry policy. Real Kabutan network
compatibility remains unproven until K10.

## KAB8. Kabutan is registered but not yet crawl-capable

K1 establishes only the site metadata and builtin factory. List/detail
parsing, monthly discovery, pagination protection, and real HTTP composition
remain open in K2-K6. The disabled site registration is not production-ready.

K2 resolved list parsing and stable identity. Detail parsing, month discovery,
pagination, and HTTP production wiring remain open.

K3 resolved deterministic detail parsing. Month discovery, protected
pagination, and HTTP production wiring remain open.

K4 and K5 resolved month discovery and deterministic protected pagination.
The remaining blocker for a real crawl is K6 generic HTTP production
composition; checkpoint persistence is handled in K7.

K6 resolved production HTTP composition. K7 durable checkpoint semantics and
K8 CLI/profile exposure remain open before real acceptance.

K7 resolved month checkpoint/resume semantics on the generic barrier. CLI and
profile exposure remains open in K8; real network behavior remains K10.

K8 resolved CLI/profile exposure. The profiles are not yet production-accepted
until K9 audit support and K10-K12 real/durable smokes pass.

K9 resolved deterministic month completeness auditing. Real HTTP behavior,
durable storage, and resume/dedup remain unaccepted until K10-K12.

K10 resolved real HTTP/parser acceptance after adding direct-`tr` support for
the actual table DOM. Durable storage and multi-month resume/dedup remain open
for K11-K12.

K11 first attempt exposed a second real Kabutan detail template:
`n202608311023` stores its article text in `div.mono`, not `div.body`. The
evidence-backed fallback was added; pages lacking both containers still fail
instead of treating navigation text as content.

## KAB9. Kabutan Premium archive intentionally unsupported

On 2026-09-10, the authoritative `category=-1,date=YYYYMM00,page=1` endpoint
returned valid links for recent news, while older rows retained metadata but
no public article links. Premium history is outside Phase K. No login, cookie
bridge, hidden-ID guess,
instrument inference, or body-derived fallback will be implemented.

Follow-up inspection on 2026-09-11 identified the exact response shape. For
`date=20260700`, Kabutan returns correctly dated list rows, but Premium archive
titles are rendered as unlinked `span.fin_modal.vtlink` nodes instead of
article links carrying `nYYYYMMDDNNNN`. The page also exposes Premium/Login
UI. The old parser returned zero items and incorrectly classified this as a
natural empty page. This invalidates the prior K11/K12 complete-month
acceptance, although their durable pipeline and resume/dedup evidence remains
valid.

The client now detects timed news rows without article links before any detail
fetch and records `free_access_complete=true`,
`stop_reason=free_access_boundary`. Free-access completeness accepts this as a
normal terminal state. This is an intentional product boundary, not a blocker.

## KAB11. K13 temporarily blocked by Kabutan AWS WAF

The approved K13 `kabutan_free_full` command was started on 2026-09-11, but
Kabutan returned HTTP 405 with an AWS `Human Verification` page from
`awselb/2.0` during list discovery. A direct GET with the same production
headers and a second read-only request after a 60-second cooldown returned the
same challenge. No challenge-cookie/browser bypass will be implemented.
Recovery-only is clean (`remaining_pending=0`). K11/K12 durable data remains
healthy; retry K13 after normal anonymous HTTP access returns.

Classification: `REMOTE ACCESS CONTROL / TEMPORARY WAF BLOCK`. This is not a
parser failure, runtime correctness failure, storage failure, or Premium
archive problem. `KabutanWafBlockedError` requires the combined signature of
HTTP 405, `Human Verification`, and an AWS WAF body marker; ordinary 405
responses remain `KabutanHTTPError`. The production free-full preflight exits
before yielding a scope, writing records, or advancing checkpoints.

## KAB12. Concurrent runtime Ctrl-C gather warning resolved

The K12 interruption exposed an unretrieved `_GatheringFuture` cancellation
warning after repeated Ctrl-C. Generic `ConcurrentProductionRuntime` now
cancels and awaits both its joined stage workers and failure waiter. A focused
cancellation test proves the blocked crawl worker reaches its cleanup path.

## KAB10. Concurrent scope summary counters lag production stats

K11 production observability correctly reported 12 files/uploads/catalog jobs,
while the nested per-scope `pipeline` summary remained zero. PostgreSQL and
completeness audit proved 19 active files/5,583 rows including seven durable
pre-interruption files. The reporting discrepancy should be corrected, but it
does not change durable data state.

K12 also showed that a zero-record complete-checkpoint resume legitimately
creates no new Catalog jobs. The completeness auditor now permits that exact
case while still requiring Catalog jobs when records were crawled or
checkpoints are missing.


# Phase L / Financial Reports — append to problem.md

## FR1. Generic financial_report dataset (RESOLVED L1)

The framework now registers reusable `financial_report` and
`financial_report_instrument` contracts while retaining `attachment` for the
physical object. HKEX normalization and real production wiring remain later
Phase L work.

## FR2. HKEX standalone script is not integrated

The supplied `get_all_finance_report.py` currently owns:

```text
CSV universe input
metadata CSV
failed CSV
local PDF directories
progress JSON
requests session
PDF download
```

These must not become a second production architecture.

## FR3. HKEX source-specific discovery is not production-wired (RESOLVED)

Need real:

```text
5-digit code -> exact stockId
40100 annual
40200 interim
40300 quarterly
title-search parsing
```

L2 update: package/manifest/builtin registration now exist, but all source
operations intentionally remain unavailable pending L3-L10. Registration is
not production acceptance.

L3 update: exact stockId resolution is implemented and fixture-tested. Real
HTTP and production-plugin wiring are intentionally still unaccepted.

L4 update: title-search request construction is implemented. The prior generic
transport gap for POST form bodies is resolved without adding an HKEX-specific
HTTP stack. Result parsing and real endpoint acceptance remain open.

L5 update: deterministic result parsing/taxonomy and canonical URL dedup are
implemented. The remaining source correctness work is release-time parsing,
canonical normalization, attachments, and real-site acceptance.

L11 update: the complete deterministic matrix passes. Remaining uncertainty is
now explicitly real HKEX response/access behavior and durable production
acceptance, beginning with L12.

L12 update: anonymous HKEX prefix and title-search HTTP are available. Real
rows include visible field labels absent from the legacy fixture; parser label
cleanup is now regression-tested. Durable metadata/PDF/NAS/Catalog behavior is
still unaccepted until L13.

L13 update: one-instrument metadata/PDF/Parquet/NAS/Catalog/index/checkpoint/
recovery/query acceptance passes. Broader report-type and concurrency coverage
remains L14 onward. The manifest auditor reported all three completed scopes;
L18 corrected the financial-report scope classification, and final full-market
completeness reports 5,596 expected and completed scopes with zero problems.

## FR10. Financial-report event-day partitioning creates pathological small files

L14 wrote 425 logical `financial_report` rows into 425 Parquet files because
sparse historical release dates each formed a separate daily partition. A
real Catalog/NAS query spent 696.34 seconds materializing 4.07 MB from 425
files, then only 1.12 seconds scanning them. This is a packaging/query latency
problem, not record correctness or recovery failure. Existing durable files
must not be deleted or rewritten without separate destructive-operation
approval; future report writes need a coarser generic partition policy before
L16.

L15 resolution: DatasetSpec now supports an explicit time granularity and
bucket-count policy. Both financial-report datasets use yearly partitions and
one stable bucket; existing defaults remain daily/256 for all other datasets.
A real three-instrument run stored 120 metadata rows in 13 files and 120
relations in one file. Existing L13/L14 files remain untouched, so their legacy
small-file query cost remains until a separately approved compaction operation.

L16 acceptance: the first 100 instruments produced 6,846 new rows in only 21
files. All 21 passed remote size and SHA256 checks. The forward small-file
regression is resolved; only pre-L15 files retain legacy physical packaging.

## FR11. Real HKEX universe edge cases (RESOLVED L16)

The 100-instrument rollout exposed currently unresolved stock codes and
dual-counter result fields such as `00016 80016`. A successful prefix response
without an exact match is now an explicit durable empty scope with
`stop_reason=stock_not_found`; HTTP/parser exceptions still fail without
checkpoint advancement. Multi-code fields are accepted only when the requested
canonical scope is one of the source-provided codes.

## FR12. Query pruning after partition-policy change (RESOLVED L16)

Initial yearly/single-bucket writes revealed that Query still assumed 256
buckets, and would also have hidden pre-L15 hash-bucket relation files. Query
now derives current and declared legacy bucket counts from DatasetSpec and
expands date pruning to yearly/monthly partition boundaries. Real new-yearly,
new-relation, and legacy-relation reads pass without rewriting old NAS data.

## FR13. HKEX secondary-counter result rows (RESOLVED L17)

During the full rollout, exact lookup of `XHKG:80016` resolved successfully but
the title-search result displayed only its primary counter `00016`. The adapter
now accepts this only when the exact resolver's canonical code equals the
requested scope; page-only mismatches without that evidence still fail. The
resolver code remains transient and does not change canonical payloads or
version hashes. The full 2,798-instrument production resume completed both
datasets with no remaining scope failure.

## FR14. PostgreSQL unavailable during L17 resume (RESOLVED)

After 2,505 metadata checkpoints became durable, PostgreSQL connection attempts
timed out twice and a direct port probe did not connect. This blocks mandatory
recovery-only and production resume. It is an external infrastructure blocker,
not an HKEX parser/storage corruption signal. Local HKEX recovery manifests
show 612 terminal `deleted` entries and zero nonterminal entries; no state was
cleared or restarted.

PostgreSQL connectivity returned on 2026-09-16. Recovery-only was clean and
the exact append-only command resumed from checkpoints to completion. No
Parquet, checkpoint, SeenStore, or Catalog state was reset.

## FR15. Async adaptive limiter blocked the production event loop (RESOLVED L17)

The resumed relation rollout initially appeared serial because
`HttpTransport` called the synchronous limiter `before_request()`, whose
`time.sleep` blocked all asyncio workers during long adaptive delays.
`AdaptiveRateLimiter.delay_before_request()` now computes the delay without
blocking and `HttpTransport` awaits its async sleep. The production resume
completed five coalesced upload/Catalog batches with durable ordering intact.

## FR4. Financial report identity (RESOLVED L7)

Canonical PDF URL SHA-256 now provides stable source identity independent of
local path, crawl order, report category, and operational scope.

## FR5. Filing metadata and physical PDF are not yet modeled separately

Need:

```text
financial_report metadata
-> attachment PDF
```

rather than treating the PDF file itself as the entire logical report.

L7 update: logical metadata is now canonicalized independently. L8 still must
emit and accept the corresponding generic attachment request.

L8 update: metadata-to-attachment request/process/normalize wiring is complete
and generic PDF safety is strengthened. Real PDF, NAS, Catalog, and recovery
acceptance remains L13 rather than being inferred from fake bytes.

## FR6. Fiscal period semantics are not source-proven

The legacy script proves release time and source report category, but does not
reliably prove fiscal-period end/year. These must remain null until source-derived
logic is implemented.

L6 update: release-time parsing is implemented independently from fiscal-period
semantics. Unreliable release times remain null and fiscal fields remain
deliberately unpopulated.

## FR7. HKEX durable resume currently exists only in legacy local progress files

Need generic SeenStore/checkpoint/recovery acceptance.

L9 update: plugin candidate and full-resume semantics now use generic
checkpoint/recovery ownership; deterministic failures do not advance state.
Real interruption/recovery acceptance remains L15.

## FR8. No production profiles/query acceptance yet

Need:

```text
hkex_reports_incremental
hkex_reports_full
```

plus real query and full rollout validation.

L10 update: both required profiles and a 2,798-instrument canonical snapshot
are implemented and composition-tested. They are not production accepted until
the real HTTP/durable rollout sequence L12-L18 succeeds.

## FR9. L0 audit classification

The current framework already owns the reusable HTTP facade, attachment
download/validation/storage pipeline, SHA256, durable runtime, SeenStore,
checkpoint/recovery, Parquet, upload, Catalog/index, manifests, completeness,
and query. The missing work is the generic financial-report dataset contract
and HKEX source adapter/wiring described by L1-L10. This is an implementation
gap, not a need for a parallel HKEX storage architecture.

The legacy input CSV is available as reference data but is not yet an
authoritative canonical HKEX universe snapshot. L0 performs no real HTTP and
does not promote fixture/reference behavior to production acceptance.
