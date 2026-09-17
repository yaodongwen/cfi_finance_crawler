# Todo

> 总目标：
>
> **让 Naver 成为第一个真正的 full-content production adapter。**
>
> 最终用户只需一次命令/一个 profile，就可以：
>
> ```text
> discover full KR universe
> + crawl all stock news
> + crawl all discussion-board posts
> + crawl all research categories
> + download research PDFs
> + dedup
> + package Parquet
> + upload
> + verify
> + build Catalog/index
> + checkpoint/recovery
> ```
>
> 后续新网站只实现 site-specific adapter/client，即可复用整个 production pipeline。

# Phase I — Naver Full Content Completion & One-click Orchestration

## I0. Fix the two current production stability blockers

Status:

```text
DONE
```

### I0.1 Upload stderr handling

- Known locale warning must not fail a successful command.
- Judge success by:
  - process exit code;
  - remote file verification;
  - size/hash checks where enabled.
- Add focused regression tests.

### I0.2 Concurrent worker exception handling

- Collect all worker exceptions.
- Cancel sibling tasks deterministically.
- Avoid `Task exception was never retrieved`.
- Preserve retryable recovery manifests.
- Add failure-boundary tests.

Acceptance:

```text
focused tests pass
full tests/unit pass
recovery-only clean
```

Validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_uploader.py tests/unit/test_concurrent_production_runtime.py
16 passed

pytest -q -p no:rerunfailures tests/unit
530 passed

recovery-only success=true remaining_pending=0
```

## I1. Migrate the real Naver News Client

Status:

```text
DONE
```

Source reference:

```text
yaodongwen/naver/Knaver_crawler/naver_news_core.py
```

Implement a real production client under the canonical Naver site package.

Required capabilities:

```text
per-stock news list discovery
article_id + office_id
detail fetch
title
content
author/source
published_at
canonical URL
pagination
retry / 429 handling
checkpoint
full / incremental policy
```

Required architecture:

```text
NaverNewsClient
-> site adapter/plugin
-> CanonicalRecord(news_article)
-> generic pipeline
```

Do not duplicate storage/upload/index logic inside Naver code.

Add real parser fixtures + integration-style tests.

Do not mark I1 finished until all are true:

```text
real HTTP client exists
production plugin/adapter wiring uses it
generic pipeline smoke works
real Naver news smoke passes
```

Validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_naver_news.py tests/unit/test_naver_finance_plugin.py tests/unit/test_app_factory.py
54 passed

pytest -q -p no:rerunfailures tests/unit
542 passed

real production CLI smoke:
site=naver_finance
dataset=news_article
instrument=005930
max_pages=1
raw_count=13
normalized_count=13
files_written=1
files_uploaded=1
files_registered=1
errors=0
remaining_pending=0
```

## I2. Complete News Relation Modeling

Status:

```text
DONE
```

Implement/verify:

```text
news_article
news_instrument
```

One logical article body should not be duplicated merely because it appears under multiple stocks.

Need:

- stable article identity;
- relation persistence/index semantics;
- query path for article by instrument/date.

Validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_naver_finance_plugin.py tests/unit/test_naver_finance_adapter.py tests/unit/test_query.py tests/unit/test_parquet_writer.py tests/unit/test_dataset.py
87 passed

pytest -q -p no:rerunfailures tests/unit
547 passed

real production CLI smoke:
site=naver_finance
dataset=news_article
instrument=005930
max_pages=1
raw_count=17
normalized_count=17
errors=0
remaining_pending=0

real production CLI smoke:
site=naver_finance
dataset=news_instrument
instrument=005930
max_pages=1
raw_count=12
normalized_count=12
errors=0
remaining_pending=0
```

## I3. Migrate the Real Naver Research Client

Status:

```text
DONE
```

Source reference:

```text
yaodongwen/naver/naver_research/
```

Must support all reference categories:

```text
market
invest
company
industry
economy
debenture
all
```

Implement:

```text
list page discovery
pagination
incremental/full mode
detail fetch
report_id
title
institution
analyst
published_at
summary/abstract
source_url
pdf_url
related instruments when available
retry/failure tracking
checkpoint
```

The core framework must remain site-agnostic.

Validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_naver_research.py tests/unit/test_naver_finance_plugin.py tests/unit/test_app_factory.py tests/unit/test_cli_main.py tests/unit/test_uploader.py
104 passed

pytest -q -p no:rerunfailures tests/unit
558 passed

real production CLI smoke:
site=naver_finance
dataset=research_report
research_category=market
max_pages=1
raw_count=30
normalized_count=30
errors=0
```

Known caveat:

```text
recovery-only still reports remaining_pending=1 terminal_failed=1
from an interrupted six-category smoke during I3.
```

## I4. Complete Research Relation Modeling

Status:

```text
DONE
```

Implement/verify:

```text
research_report
research_instrument
```

Rules:

- one logical report stored once;
- multi-stock relation stored separately;
- global market/economy/industry reports may have no single `instrument_id`.

Validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_naver_research.py tests/unit/test_naver_finance_plugin.py tests/unit/test_naver_finance_adapter.py tests/unit/test_query.py tests/unit/test_dataset.py
85 passed

pytest -q -p no:rerunfailures tests/unit
563 passed

real production CLI smoke:
site=naver_finance
dataset=research_instrument
research_category=company
max_pages=1
raw_count=30
normalized_count=30
errors=0
```

## I5. Wire Research PDF Attachments End-to-end

Status:

```text
DONE
```

Production path:

```text
research_report
-> discover_attachments()
-> dataset=attachment crawl logic
-> generic attachment pipeline
-> PDF validation
-> SHA256 dedup
-> deterministic local path
-> generic upload
-> remote verification
-> attachment metadata/index
-> cleanup
-> recovery
```

Requirements:

- `attachment_workers` and `pdf_workers` semantics clarified;
- PDF failures do not corrupt report metadata;
- report can be durable even when PDF retry remains pending, if policy explicitly allows it;
- resumable/retryable attachment failures;
- duplicate PDF SHA should not be uploaded repeatedly.

Validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_attachment.py tests/unit/test_naver_finance_plugin.py tests/unit/test_app_factory.py tests/unit/test_cli_main.py tests/unit/test_uploader.py
114 passed

pytest -q -p no:rerunfailures tests/unit
572 passed

real production CLI smoke:
site=naver_finance
dataset=attachment
research_category=market
max_pages=1
attachment_limit=1
raw_count=1
normalized_count=1
errors=0
```

Known caveat:

```text
attachment processing is generic and production-wired,
but it currently runs through dataset=attachment crawl logic.
A more explicit standalone bounded attachment stage and observability
remain follow-up work.
```

## I6. Unify Naver Production Entry Point

Status:

```text
DONE
```

Current production registry uses `NaverFinancePlugin` as the single
authoritative production `SitePlugin`.

The adapter is now a compatibility facade:

- `NaverFinanceAdapter.discover_instruments()` uses the canonical
  Naver market-sum discovery client;
- adapter task discovery/fetch delegates to injected real
  news/forum/research clients;
- adapter normalization for every production Naver dataset delegates to
  `NaverFinancePlugin.normalize()`;
- adapter no longer keeps a second research normalization
  implementation;
- adapter declares the same production content datasets, including
  `attachment`.

```text
instrument
forum_post
news_article
news_instrument
research_report
research_instrument
attachment
```

`builtin.py` remains intentionally wired to `NaverFinancePlugin`,
because the production registry accepts `SitePlugin` factories and the
adapter is a lower-level `SiteAdapter` compatibility API.

Validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_naver_finance_adapter.py tests/unit/test_builtin_sites.py tests/unit/test_naver_finance_plugin.py
60 passed

pytest -q -p no:rerunfailures tests/unit
578 passed
```

## I7. Add Dataset-specific Runtime Options

Status:

```text
DONE
```

Introduce clear options/config instead of overloading one `--max-pages`.

At minimum:

```text
forum_max_pages
news_max_pages
news_mode=incremental|full
research_categories
research_max_pages
research_mode=incremental|full
download_research_pdf
research_detail_workers
pdf_workers
```

Prefer config/profile values with CLI overrides.

Implemented CLI/context options:

```text
--forum-max-pages
--news-max-pages
--news-mode=incremental|full
--research-max-pages
--research-mode=incremental|full
--download-research-pdf
--no-download-research-pdf
--research-detail-workers
--pdf-workers
```

Compatibility:

- `--max-pages` still maps to forum/news/research max pages.
- Dataset-specific max-page flags override `--max-pages`.
- `--pdf-workers` maps to generic `attachment_workers` when
  `--attachment-workers` is not supplied.
- `research_detail_workers` is passed through context for I8
  budget wiring; the current research client does not yet consume a
  separate detail worker pool.

Validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_cli_main.py tests/unit/test_app_factory.py tests/unit/test_naver_finance_plugin.py
101 passed

pytest -q -p no:rerunfailures tests/unit
584 passed
```

## I8. Add Per-dataset Resource Budgets

Status:

```text
DONE
```

For one-click all-content crawl, avoid letting PDFs starve news/forum HTTP.

Support quotas such as:

```text
forum crawl workers
news crawl workers
research list/detail workers
PDF workers
upload workers
catalog workers
HTTP concurrency per site/dataset
```

All queues must stay bounded.

Implemented:

- generic `DatasetResourceBudget`;
- CLI options for per-dataset crawl workers:
  `--forum-crawl-workers`, `--news-crawl-workers`,
  `--research-crawl-workers`;
- CLI options for per-dataset HTTP budgets:
  `--forum-http-concurrency`, `--news-http-concurrency`,
  `--research-http-concurrency`;
- `make_crawl_context()` materializes `dataset_budgets`;
- `ConcurrentProductionRuntime.run_dataset()` consumes
  per-dataset `crawl_workers` budget, so different datasets can run
  with different scope-worker counts while still using bounded queues;
- `--pdf-workers` remains mapped to the generic attachment worker
  budget.

Known boundary:

- per-dataset HTTP/detail budgets are now modeled and passed through;
- current Naver HTTP clients still perform their internal list/detail
  work sequentially, so client-level HTTP semaphore/detail worker
  consumption remains for I9/I10 follow-up work.

Validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_concurrency_config.py tests/unit/test_cli_main.py tests/unit/test_app_factory.py tests/unit/test_concurrent_production_runtime.py
70 passed

pytest -q -p no:rerunfailures tests/unit
587 passed
```

## I9. Add Adaptive Rate Limiting / Proxy Health

Status:

```text
DONE
```

Current proxy rotation should be strengthened for long Naver runs.

Support:

```text
429/403 adaptive backoff
per-endpoint retry policy
proxy health scoring
cooldown
circuit breaker
request jitter
per-dataset concurrency throttling
```

Do not simply increase concurrency until requests fail.

Implemented:

- generic `AdaptiveRateLimiter` with per-endpoint state;
- throttle/failure backoff;
- jitter support;
- simple circuit-open delay after repeated failures;
- success resets endpoint delay/circuit state;
- generic `HttpTransport` can report status/exception outcomes to
  the limiter;
- Naver forum/news/research real HTTP clients accept optional
  `rate_limiter`;
- `NaverFinancePlugin` default production construction wires one
  shared limiter into forum/news/research clients;
- 403/429 are marked throttled, 5xx and request exceptions are marked
  failures, success clears delay.

Known boundary:

- proxy selection/cooldown existed before I9 and remains generic;
- Naver clients now report health/rate-limit signals, but they still
  use `requests.Session` directly rather than the generic
  `HttpTransport` facade;
- per-dataset HTTP semaphores from I8 are modeled but still not
  consumed inside each Naver client.

Validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_transports.py tests/unit/test_naver_news.py tests/unit/test_naver_research.py tests/unit/test_naver_forum_post.py tests/unit/test_naver_finance_plugin.py
89 passed

pytest -q -p no:rerunfailures tests/unit
593 passed
```

## I10. Add Live Progress Logging

Status:

```text
DONE
```

Periodic progress for long runs:

```text
scope completed/total
news records
forum records
research records
PDF success/failure
queue depths
uploads complete
catalog complete
retry counts
throughput
pending recovery
```

JSON end-of-run stats should remain available.

Implemented:

- CLI option `--progress-interval-seconds`;
- live progress is emitted only when explicitly enabled;
- JSON output mode keeps stdout machine-readable and does not install
  the live progress reporter;
- concurrent production runtime emits reusable `ProgressSnapshot`
  objects;
- text reporter prints concise progress lines to stderr;
- progress includes dataset, scope completed/discovered, pending
  scopes, records, files, uploads, catalog jobs, queue depths, and
  crawl/upload/catalog busy time.

Validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_cli_main.py tests/unit/test_app_factory.py tests/unit/test_concurrent_production_runtime.py
70 passed

pytest -q -p no:rerunfailures tests/unit
599 passed
```

## I11. Add a Naver One-click Production Profile

Status:

```text
DONE
```

Example target:

```bash
python -m crawl_framework.cli.main crawl \
  --site naver_finance \
  --profile naver_full
```

or equivalent:

```bash
python -m crawl_framework.cli.main crawl \
  --site naver_finance \
  --all-content \
  --instruments-file config/universes/naver_finance_kr_rollout_universe.txt
```

`naver_full` should mean, explicitly:

```text
all configured stock news
all configured forum history
all research categories
research PDF attachments
generic Parquet packaging
parallel upload
Catalog/index
checkpoint/recovery
```

Provide a safe incremental profile too:

```text
naver_incremental
```

Implemented:

- CLI `--profile naver_full`;
- CLI `--profile naver_incremental`;
- both profiles default to the deterministic Naver rollout universe
  snapshot:
  `config/universes/naver_finance_kr_rollout_universe.txt`;
- both profiles include:
  `forum_post`, `news_article`, `news_instrument`,
  `research_report`, `research_instrument`, `attachment`;
- `naver_full` defaults `news_mode=full`,
  `research_mode=full`, `research_categories=all`,
  `download_research_pdf=true`;
- `naver_full` maps unset forum/news/research page limits to
  unbounded `"all"`, so plugin default one-page safety caps do not
  accidentally make a full profile partial;
- `naver_incremental` defaults `news_mode=incremental`,
  `research_mode=incremental`, `research_categories=all`,
  `download_research_pdf=true`;
- explicit `--dataset`, `--instrument`, `--instruments-file`,
  `--news-mode`, `--research-mode`, page caps, and PDF flags override
  profile defaults.

Validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_cli_main.py tests/unit/test_app_factory.py tests/unit/test_naver_finance_plugin.py
113 passed

pytest -q -p no:rerunfailures tests/unit
605 passed
```

## I12. Add Run Manifest

Status:

```text
DONE
```

Every one-click run should generate a run manifest:

```text
run_id
site
profile
universe snapshot/hash
datasets
options
start/end time
worker counts
records by dataset
files written
PDF success/failure
uploads/catalog jobs
errors
recovery pending
checkpoint completeness
```

This is essential for auditing multi-hour/full-market runs.

Implemented:

- `--run-manifest` writes a JSON run manifest;
- `--run-manifest-path` can choose the output file;
- `--profile naver_full` and `--profile naver_incremental`
  enable run manifest writing by default;
- default manifest path:
  `state/run_manifests/<run_id>.json`;
- manifest includes run_id, site, profile, datasets, options,
  start/end timestamps, universe snapshot path/count/SHA256,
  startup recovery stats, runtime stats, and success flags.

Validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_cli_main.py
46 passed

pytest -q -p no:rerunfailures tests/unit
609 passed
```

## I13. Add Completeness Audit

Status:

```text
DONE
```

Given a run/universe, provide a command that checks:

```text
expected scopes
completed scopes
missing checkpoints
failed scopes
pending recovery
files in Catalog
attachments pending
```

Example target:

```bash
python scripts/audit_run.py --run-id ...
```

or:

```bash
python scripts/check_rollout_completeness.py \
  --site naver_finance \
  --profile naver_full
```

Implemented:

- `scripts/check_rollout_completeness.py`;
- reads run manifest, universe snapshot, checkpoint directory, and
  recovery manifest directory;
- reports expected scopes, completed scopes, missing checkpoints,
  failed runtime scopes, pending recovery, terminal failed recovery,
  catalog-registered file count, and pending attachment recovery;
- returns non-zero when completeness evidence is missing;
- supports JSON output for automation.

Validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_rollout_completeness_audit.py
3 passed

pytest -q -p no:rerunfailures tests/unit
612 passed
```

## I14. Real Production Smoke Matrix

Status:

```text
DONE
```

Before full-market all-content, validate:

### Smoke 1

```text
1 instrument
forum + news
research market 1 page
1 PDF
```

### Smoke 2

```text
3 instruments
forum + news
all research categories with small page cap
PDF
```

### Smoke 3

```text
50 instruments
forum + news
all research categories
PDF
```

Acceptance:

```text
errors=0 or explicitly retryable failures only
remaining_pending=0 after recovery
all expected datasets queryable
PDF remote verification passes
Catalog/index consistent
checkpoint completeness passes
```

Implemented / verified:

- Added deterministic smoke matrix runner:
  `scripts/run_naver_smoke_matrix.py`;
- Smoke 1 uses the first instrument from the rollout universe;
- Smoke 2 uses the first 3 instruments from the same universe;
- Smoke 3 uses the first 50 instruments from the same universe;
- all smoke commands use production CLI, `naver_incremental`
  profile, real Naver HTTP, PostgreSQL, NAS/rsync upload, generic
  concurrent runtime, Catalog/index, checkpoint/recovery, and PDF
  attachment path.

Real production validation:

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
checkpoint completeness audit complete=true
manifest=state/run_manifests/9207d4f1241548c0aada9067523bd654.json
```

Regression fixes discovered by real smoke:

- `research_category=all` now expands correctly when passed from CLI
  as `("all",)`;
- one Naver news article with missing body no longer fails the whole
  production run; the article is skipped and recorded in
  `NaverNewsClient.detail_errors` without writing fake content.

Validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_naver_smoke_matrix.py tests/unit/test_naver_news.py tests/unit/test_naver_research.py tests/unit/test_rollout_completeness_audit.py tests/unit/test_recovery.py tests/unit/test_retry_policy.py
124 passed

pytest -q -p no:rerunfailures tests/unit
621 passed
```

## I15. Finish H20 Forum Full-market Rollout

Status:

```text
DONE
```

After I0 stability fixes, finish:

```text
3924-instrument forum_post rollout
```

Acceptance:

```text
success=true
remaining_pending=0
errors=0
checkpoint completeness verified
```

Current attempt:

```text
command:
python -m crawl_framework.cli.main crawl \
  --site naver_finance \
  --dataset forum_post \
  --instruments-file config/universes/naver_finance_kr_rollout_universe.txt \
  --forum-max-pages 1 \
  --crawl-workers 8 \
  --writer-workers 4 \
  --upload-workers 4 \
  --catalog-workers 2 \
  --target-file-size-mb 16 \
  --run-manifest \
  --json

result:
run manually stopped after about 3 hours
exit_code=143
```

Observed blocker:

```text
the crawler stayed healthy and continued progressing,
but the run spent hours draining many small Parquet files through
rsync mkdir/stat/upload verification.
```

Recovery result after stopping:

```text
python -m crawl_framework.cli.main crawl --site naver_finance --recovery-only --json
success=true
attempted=5
recovered=5
failed=0
remaining_pending=0
terminal_failed=0
```

Added for reproducibility:

```text
scripts/run_naver_forum_full_rollout.py
```

Small-file mitigation completed:

```text
--coalesce-scope-flushes
```

Production runtime now allows completed scopes to wait for size/final
batch flush instead of forcing a Parquet file per scope completion.
Checkpoint commit still waits until each coalesced batch finishes
upload/catalog.

50-instrument production validation:

```text
success=true
records_crawled=991
files_written=44
uploads_completed=44
checkpoint completeness audit passed
```

Full-market retry after coalescing:

```text
python scripts/run_naver_forum_full_rollout.py
coalesce_scope_flushes=true
3924 instruments
```

Result:

```text
run exceeded 3 hours and was manually interrupted
wrapper exit_code=130
no residual crawl/rsync/ssh production process found
```

Recovery-only verification after PostgreSQL was restored:

```text
success=true
attempted=4
recovered=4
remaining_pending=0
terminal_failed=0
```

Additional speed fixes completed:

```text
RsyncUploader caches remote mkdir directories
--trust-rsync-success skips per-file ssh stat after successful rsync
rollout script streams progress instead of capture_output black-box mode
rollout defaults:
  crawl_workers=16
  upload_workers=4
  catalog_workers=4
```

Full-market retry found the upload concurrency ceiling:

```text
upload_workers=8
-> kex_exchange_identification: read: Connection reset by peer
-> Connection reset by 192.168.1.33 port 22
```

Recovery after that failure:

```text
success=true
attempted=9
recovered=9
remaining_pending=0
terminal_failed=0
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

Validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_naver_forum_full_rollout.py tests/unit/test_naver_smoke_matrix.py tests/unit/test_recovery.py tests/unit/test_retry_policy.py
72 passed

pytest -q -p no:rerunfailures tests/unit
623 passed
```

Next required work:

Completed final I15 run:

```text
manifest:
state/run_manifests/naver_forum_full_rollout_20260903T054641Z.json

success=true
records_crawled=74880
files_written=4086
uploads_completed=4086
catalog_jobs_completed=4086
completed_scopes=3924
missing_checkpoints=0
pending_recovery=0
terminal_failed_recovery=0
final recovery-only attempted=0
remaining_pending=0
```

Residual follow-up after I15:

- consider generic compaction for the 4086 forum files;
- do not block I16 on this because I15 acceptance criteria passed.

## I16. Full Naver All-content Production Run

Status:

```text
BLOCKED BY POSTGRESQL CONNECTIVITY AFTER RUNTIME FIXES
```

Only after I1-I15 pass:

```text
3924-stock universe
news
forum
all research categories
research PDFs
```

Run through the unified one-click command/profile.

After completion:

```text
recovery-only
completeness audit
Catalog/storage audit
query smoke
PDF sample integrity audit
full unit tests
```

Work completed before retry:

```text
naver_full default page policy fixed:
  no unbounded forum/news/research history by default

RsyncUploader stability fixed:
  command_timeout_seconds
  command_attempts
  retry_sleep_seconds
  no permanent ssh/rsync worker hang
```

Latest tests:

```text
pytest -q -p no:rerunfailures tests/unit/test_uploader.py tests/unit/test_naver_forum_full_rollout.py tests/unit/test_cli_main.py tests/unit/test_app_factory.py
98 passed

pytest -q -p no:rerunfailures tests/unit
633 passed
```

Current blocker:

```text
python -m crawl_framework.cli.main crawl --site naver_finance --recovery-only --json
bootstrap configuration failed: connection timeout expired
```

Next required work:

- restore PostgreSQL connectivity;
- rerun recovery-only;
- rerun I16 using:
  `naver_full`, full 3924 universe, bounded default pages,
  coalesced flushes, trust-rsync-success, upload_workers=2-4;
- run completeness audit, Catalog/storage audit, query smoke, and
  PDF sample integrity audit.

# Additional Recommended Production Support

These are recommended before treating the crawler as unattended long-running infrastructure:

1. `dry-run/plan` command:
   - show universe count;
   - datasets;
   - expected scopes;
   - worker config;
   - target remote;
   - no writes.

2. Structured failed-job queue:
   - retry by site/dataset/scope;
   - retain error class/status;
   - targeted retry command.

3. Attachment resumable download:
   - useful for larger PDFs;
   - optional Range support.

4. Metrics export:
   - optional Prometheus/OpenTelemetry later;
   - do not block first one-click Naver release.

5. Scheduled incremental mode:
   - once `naver_incremental` is stable, schedule periodic news/forum/research updates.

6. Schema/version audit:
   - verify news/research/forum payload evolution remains backward compatible.

7. Relation query tests:
   - `news_instrument`;
   - `research_instrument`.

8. Remove stale placeholders/backups after migration:
   - empty `news.py`;
   - empty `comments.py`;
   - `plugin.py.before_dedup.bak`.

# Definition of Naver Full-content Complete

Do not mark the Naver adapter complete until all are true:

```text
full instrument discovery works
real news client works
real forum client works
real research client works for all 6 categories
research PDFs work
all are production-wired
all use the generic concurrent pipeline
all use generic upload/Catalog/index/recovery/query
one-click naver_full exists
one-click naver_incremental exists
50-instrument all-content smoke passes
full forum rollout passes
full all-content run passes
completeness audit passes
```

# Mandatory Legacy Code Reference for Every Phase I Item

Before implementing any Phase I item, read the corresponding sections in `legacy_reference.md`. Do not make Codex rediscover Naver behavior from scratch.

| Phase | Old repository reference | Specific functions / logic to reuse | What must NOT be copied |
|---|---|---|---|
| I0 | No direct old equivalent | none | old site runner is not a fix for new runtime/uploader problems |
| I1 News | `Knaver_crawler/naver_news_core.py`, `naver_news_config.py`, `naver_cache.py`, `retry_failed_items.py` | `warmup`, `create_session`, `get_html`, `news_url`, `parse_news_list`, `parse_article`, `crawl_stock_news`; encoding/Referer/429/body selectors | per-record JSON, claim-before-durable-write, embedded storage |
| I2 News relations | `parse_news_list`, `build_dataset.py::build_news_dataset` | article-to-stock association evidence | duplicated body per stock; article_id-only identity |
| I3 Research | `naver_research_config.py`, `naver_research_core.py`, `run_naver_research.py` | category URLs; `ResearchListItem`, `build_research_list_signature`, `parse_research_list`, `ResearchDetail`, `parse_research_detail`, `process_research_item(s)`, `crawl_research_category` | old JSON/cache/ThreadPool orchestration as authoritative architecture |
| I4 Research relations | `ResearchListItem.stock_code/stock_name/classification`, list/detail parsers | real stock relation and classification fields | inferred stock relations from unrelated links |
| I5 PDF | `naver_research_core.py`, `naver_research_cache.py` | `extract_pdf_url`, `configure_pdf_download_limits`, `is_valid_pdf_file`, `download_research_pdf`, PDF saved/unavailable/failure semantics | old final PDF directory layout and separate authoritative cache |
| I6 Unified entry | `run_naver_news.py`, `run_naver_research.py` | business orchestration semantics | two separate new production pipelines |
| I7 Options | `naver_news_config.py`, `run_naver_research.py::build_argument_parser` | old page/mode/category/resume/PDF/detail-worker options as design input | blindly exposing obsolete flags replaced by generic checkpoint/recovery |
| I8 Budgets | old `STOCK_WORKERS`, `NEWS_WORKERS`, `COMMENT_WORKERS`, research `detail_workers`, `pdf_workers`, `list_page_batch_size` | evidence that endpoint budgets differ | unbounded nested executors |
| I9 Rate limits | old news/forum `get_html`, research `Retry` + random sleeps | encoding-aware retries, 429/5xx handling, jitter | simple fixed policy as final design; new design should add proxy health/adaptive limits |
| I10 Progress | old crawler logs; `print_cache_statistics`, `print_run_result` | useful progress dimensions | site-specific logger as sole observability layer |
| I11 Profiles | `run_naver_news.py::main`, `run_naver_research.py::run_crawler` | old full/incremental business behavior | requiring two commands |
| I12 Manifest | old `stock_progress` and research statistics are partial references | status dimensions only | old JSON progress files as authoritative state |
| I13 Completeness | `naver_cache.py::cache_stats`, research page/statistics functions | coverage/stat dimensions | assuming cache count alone proves completeness |
| I14 Smoke | old `MAX_STOCKS`, page caps, research `--max-pages/--category/--no-pdf` | small-scope test strategy | old storage output as acceptance criterion |
| I15 H20 | `naver_comment_core.py::crawl_stock_comments` for historical behavior only | page/history expectations | replacing current `forum_post` implementation |
| I16 All content | both old runners | proves all three content families can be collected | old two-runner architecture |

## I1 exact migration checklist — News

Read these old functions before writing the new client:

```text
Knaver_crawler/naver_news_core.py
  warmup
  create_session
  sleep_random
  get_html
  news_url
  parse_news_list
  parse_article
  crawl_stock_news
  save_failed_news

Knaver_crawler/naver_news_config.py
  STOCK_NEWS_URL
  REQUEST_TIMEOUT
  REQUEST_DELAY_MIN/MAX
  STOP_REPEAT_PAGES
  MIN_BODY_LENGTH
  NEWS_WORKERS
  HTTP_POOL_MAXSIZE

Knaver_crawler/naver_cache.py
  news_exists
  claim_news
  add_news
  add_news_ids

Knaver_crawler/retry_failed_items.py
  retry_news
```

Migration rules:

```text
finance.naver.com HTML encoding knowledge must be preserved.
n.news.naver.com UTF-8 behavior must be preserved.
Referer behavior must be preserved.
The article body selector fallback list must be covered by fixtures.
The old claim_news timing must NOT be restored.
source_id must use office_id + article_id.
```

## I3 exact migration checklist — Research

Read these old functions/classes before writing `NaverResearchClient`:

```text
naver_research/naver_research_config.py
  RESEARCH_CATEGORIES
  get_research_list_url
  retry/sleep/PDF limits

naver_research/naver_research_core.py
  ResearchListItem
  build_research_list_signature
  create_session
  get_thread_session
  decode_naver_html
  request_html
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
  process_research_item_with_thread_session
  process_research_items
  crawl_research_category

naver_research/naver_research_cache.py
  report_exists
  report_pdf_saved
  upsert_discovered_report
  mark_content_saved
  mark_pdf_saved
  mark_pdf_unavailable
  mark_report_failed
  record_page_failure
  update_page_progress
  get_page_progress
  list_failed_reports
  get_research_statistics

naver_research/run_naver_research.py
  resolve_categories
  resolve_resume_start_page
  retry_failed_reports
  run_crawler
```

Critical migration requirements:

```text
all six categories must work;
repeated-last-page signature protection must be retained;
company stock association must use list evidence first;
em.money and em.coment parser knowledge must be fixture-tested;
old SQLite states must be mapped to generic Seen/checkpoint/recovery rather than duplicated.
```

## I5 exact migration checklist — PDF

Read:

```text
naver_research/naver_research_core.py
  extract_pdf_url
  configure_pdf_download_limits
  get_pdf_path
  is_valid_pdf_file
  download_research_pdf
  process_research_item PDF stage
```

Preserve behavior:

```text
%PDF magic header
Content-Length consistency
maximum file-size limit
streamed write
empty-response detection
partial-download detection
.part temporary file
atomic promotion
bounded PDF concurrency
retry/backoff
Referer
```

Map final persistence to the generic attachment/uploader/recovery pipeline.

2026-09-07 status:

- repeated-final-page page-signature protection is implemented in
  `NaverResearchClient.crawl_pages()`;
- unit tests and read-only real HTTP market page smoke pass;
- remaining action: rerun the production CLI smoke below after the
  rsync/NAS remote directory step is healthy:

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

## Required Codex rule

For every Phase I completion report, Codex must state:

```text
1. Which legacy files/functions were reviewed.
2. Which behavior was migrated.
3. Which old behavior was deliberately not copied and why.
4. Which new files/classes/functions implement it.
5. Which real fixture/smoke proves the migration.
```

A Phase I item cannot be marked complete without that migration traceability.

# Todo Update — Append Phase J

> Do not delete existing Phase I / Naver tasks.
>
> Append this Phase J after the current Naver section.
>
> Detailed source mapping:
>
> `TossInvest_Legacy_Migration_Plan.md`

# Phase J — TossInvest Full Production Migration

## J0. Repository audit + legacy traceability

Status:

```text
DONE
```

- classify current Toss capabilities as real / fixture / interface / missing;
- inspect old `crawler.py`, `collect_stocks.py`, `normalizer.py`,
  `cache_manager.py`, `production_runner.py`, `main.py`;
- confirm current `comments.py`, `news.py`, Playwright transport state;
- update status docs before implementation.

Validation:

```text
focused Toss architecture/plugin tests: 9 passed
full tests/unit baseline: 647 passed
real smoke: not applicable to audit-only J0
next: J1 generic Playwright transport/browser worker pool
```

## J1. Generic Playwright transport/browser worker pool

Status:

```text
DONE
```

Legacy references:

```text
main.py
crawler.py::_prepare_page
crawler.py::save_debug_html
production_runner.py::crawl_worker
```

Implement generic browser lifecycle, bounded workers, context/profile isolation,
timeouts, proxy leasing, graceful shutdown and crash cleanup.

Validation:

```text
focused tests: 11 passed
full tests/unit: 651 passed
real Chromium smoke: passed
```

## J2. Real Toss instrument discovery

Status:

```text
DONE
```

Legacy references:

```text
collect_stocks.py::collect
crawler.py::_wait_screener_ready
crawler.py::_switch_screener_to_domestic
crawler.py::_get_real_screener_filter_chips
crawler.py::_remove_one_screener_filter
crawler.py::_remove_all_screener_filters
crawler.py::_prepare_screener
crawler.py::_extract_visible_screener_stocks
crawler.py::collect_stock_links
```

Generate deterministic canonical KR universe snapshot.

Validation: real full Screener discovery `2427`; focused `20 passed`; full
unit suite `655 passed`; real production plugin discovery smoke passed.

## J3. Real Toss forum/community client

Status:

```text
DONE
```

Legacy references:

```text
crawler.py::_open_community_and_sort_latest
crawler.py::_extract_visible_comment_cards
crawler.py::crawl_comments
```

Implement real Playwright forum crawl through generic runtime.

Validation: real durable smoke `success=true`, raw/normalized `7/7`, one file
written/uploaded/verified/registered, pending `0`, checkpoint complete;
focused `47 passed`; full unit `658 passed`.

## J4. Forum full field parity

Status:

```text
DONE
```

Legacy references:

```text
normalizer.py::normalize_comment
normalizer.py::parse_relative_comment_time
```

Preserve follower/shareholder/relative-time/profile/raw-text/engagement fields.

Validation: focused `11 passed`; full unit `659 passed`; real durable
regression smoke raw/normalized `10/10`, pending `0`.

## J5. Real Toss news list discovery

Status:

```text
DONE
```

Legacy references:

```text
crawler.py::parse_news_id
crawler.py::collect_news_links
```

Preserve full-baseline vs incremental semantics.

Validation: real production-plugin list smoke found 16 links; focused
`14 passed`; full unit `663 passed`.

## J6. Real Toss news detail parser

Status:

```text
DONE
```

Legacy reference:

```text
crawler.py::parse_news_article
```

Preserve metadata-v4 extraction and provenance fields.

Validation: real detail plus durable smoke passed; focused `44 passed`; full
unit `666 passed`; pending recovery 0.

## J7. News article/instrument relations

Status:

```text
DONE
```

Implement:

```text
news_article
news_instrument
```

One logical article should not be physically duplicated per stock.

Validation: focused `51 passed`; full unit `667 passed`; real
`news_instrument` durable smoke passed with pending 0.

## J8. Historical baseline + pending/resume semantics

Status:

```text
DONE
```

Legacy references:

```text
crawler.py::collect_news_links
crawler.py::crawl_news
cache_manager.py::PersistentIdSet
cache_manager.py::bootstrap_from_jsonl
production_runner.py::classify_resume
```

Map old semantics to generic SeenStore/checkpoint/recovery.

Validation: focused `72 passed`; full unit `669 passed`; real checkpoint
smoke complete with baseline false, pending detail 0, RecoveryStore pending 0.

## J9. Browser worker pool + ProxyPool integration

Status:

```text
DONE
```

Use generic ProxyPool with bounded browser workers and isolated contexts.

Validation: focused `63 passed`; full unit `670 passed`; real AppFactory
browser lifecycle/durable smoke passed.

## J10. Dataset-specific concurrency/backpressure

Status:

```text
DONE
```

Separate budgets for instrument/forum/news/browser/write/upload/catalog while
keeping queues bounded.

Validation: named-budget backpressure test plus generic staged overlap tests;
focused `59 passed`; full unit `671 passed`.

## J11. 1-stock real production smoke

Status:

```text
DONE
```

Validate instrument + forum + news + storage/upload/Catalog/index/recovery/query.

Validation on 2026-09-08: all three Toss datasets completed a real production
CLI durable run for `XKRX:005930`; Catalog/NAS query smoke passed; final
recovery pending was zero; focused `70 passed`; full unit `671 passed`.

## J12. 4-stock concurrent smoke

Status:

```text
DONE
```

Validate real browser overlap, profile isolation, proxy allocation and staged pipeline.

Validation on 2026-09-08: snapshot prefix 4 completed real forum/news runs,
browser max active 2, crawl/upload/Catalog overlap proven by intervals,
stable relation rerun wrote zero files, compacted active relation rows are
unique, and recovery pending is zero. No real proxies were configured; direct
isolated contexts were used and generic ProxyPool behavior remains unit-tested.

## J13. 20-stock production smoke

Status:

```text
DONE
```

Validate memory/browser/proxy/queue stability.

Validation on 2026-09-08: 20/20 scopes for all datasets, browser max 4,
bounded queue maxima, no lease failure or swap, peak RSS <= 763 MB, all news
pending counts zero, read-only Catalog/NAS query passed, recovery pending zero,
focused `63 passed`, full unit `677 passed`.

## J14. 100-stock rollout

Status:

```text
DONE
```

Use same frozen universe snapshot and run completeness/recovery audit.

Validation on 2026-09-08: all three datasets completed 100/100 scopes from the
same frozen prefix. Forum/article/relation crawled `550/900/900` records and
registered `197/236/82` files. Browser max active was 4 with no lease failure;
bounded upload backpressure was observed; news pending and recovery pending
were zero; PostgreSQL Catalog/NAS query passed. Focused `74 passed`; full unit
`677 passed`.

## J15. Full Toss KR universe rollout

Status:

```text
DONE
```

Do not hard-code the historical 2458 count as current truth.
Use current deterministic snapshot.

Final acceptance on 2026-09-09:

```text
forum_post       2427/2427, missing=0
news_article     2427/2427, missing=0
news_instrument  2427/2427, missing=0
recovery pending=0, failed=0
NAS/Catalog paths=23349/23349, orphan/missing/size mismatch=0/0/0
focused tests=99 passed; full unit tests=684 passed
```

The interrupted news suffixes resumed from offset 100 using existing
SeenStore/checkpoint/recovery state. No durable prefix was rerun from scratch.

## J16. One-click Toss profiles

Status:

```text
DONE
```

Implement:

```text
toss_incremental
toss_full
```

Recommended additionally:

```text
toss_historical_backfill
```

Phase J is complete only after real site smoke/rollout proves the migration.

Accepted on 2026-09-09. Both required profiles passed real Samsung production
smokes through Playwright and the generic durable pipeline. Each processed
forum/article/relation `3/9/9` rows, wrote/uploaded/registered 3 files, and
finished with zero errors and pending recovery. Focused tests: `113 passed`;
full unit suite: `691 passed`. The optional historical profile is implemented
but is not claimed as a completed full-history rollout.

# Phase K — Kabutan Market News Integration

> Append to existing `todo.md`.
>
> Authoritative plan: `Kabutan_MarketNews_Integration_Plan.md`

## K0. Audit framework + get_all.py
Status: `DONE`

Accepted on 2026-09-10. The supplied `get_all.py` was traced function by
function. Kabutan itself is not registered and has no production plugin,
client, scopes, CLI, or smoke. Generic durable storage/recovery/query pieces
are available unchanged. `HttpTransport` exists as an injected facade, but
the normal AppFactory does not yet create a real HTTP transport and the active
environment has no optional `httpx`; K6 must close that generic wiring gap
without adding Kabutan-owned transport infrastructure. Full baseline:
`691 passed`.

## K1. Add Kabutan site package + builtin registration
Status: `DONE`

Accepted on 2026-09-10. Added the disabled-by-default Kabutan HTTP site
package and builtin registration with the exact JP/month-news contract. No
parser, real HTTP, or production-ready claim is included in K1. Focused:
`4 passed`; full unit: `693 passed`.

Create:
```text
src/crawl_framework/sites/kabutan/__init__.py
src/crawl_framework/sites/kabutan/market_news.py
src/crawl_framework/sites/kabutan/plugin.py
src/crawl_framework/sites/kabutan/site.yaml
```

## K2. Migrate list parser + stable identity
Status: `DONE`

Accepted on 2026-09-10. Migrated Kabutan list selectors, text cleanup,
news-ID extraction, URL joining, and duplicate-table de-duplication. Added an
opt-in canonical identity-scope override so month remains the operational
scope while the real `news_id` remains stable across month/page discovery;
existing sites retain their prior identity behavior. Focused: `12 passed`;
full unit: `698 passed`.

Reference:
```text
clean_text
NEWS_ID_RE
extract_news_id
parse_list_page
```

## K3. Migrate article detail parser
Status: `DONE`

Accepted on 2026-09-10. Migrated `article`, `h1`, primary/fallback detail
datetime, category fallback, `div.body`, and unwanted-node cleanup. Missing
article/body is explicit parser state, and list time never fabricates an exact
event time. Focused: `11 passed`; full unit: `703 passed`.

Reference:
```text
parse_article_page
```

## K4. Implement monthly scopes
Status: `DONE`

Accepted on 2026-09-10. Added stable inclusive month iteration, 2013-09 full
history start, Tokyo dynamic current month, previous-month overlap, and exact
month scopes without instruments. Focused: `16 passed`; full unit:
`708 passed`.

```text
scope_type=month
scope_id=YYYY-MM
source_key=YYYYMM
history start=2013-09
dynamic current month
```

## K5. Pagination protection
Status: `DONE`

Accepted on 2026-09-10. The streaming month client stops on empty pages,
repeated ordered signatures, or current IDs wholly contained in prior month
pages before any repeated detail fetch. Safety caps remain partial. Focused:
`20 passed`; full unit: `712 passed`.

```text
empty page
repeated page signature
current IDs subset of prior month IDs
max-pages safety cap
```

## K6. Wire real Kabutan HTTP client
Status: `DONE`

Accepted on 2026-09-10. Added reusable urllib HTTP requesting, configurable
generic status retry, ProxyPool/rate-limiter composition, and AppFactory
`requires_http` wiring. Kabutan owns only endpoint parameters and Japanese
headers. This is production wiring but not yet a real-site smoke claim.
Focused: `65 passed`; full unit: `715 passed`.

Reuse generic retry/rate-limit/proxy/HTTP infrastructure where compatible.

## K7. Checkpoint/recovery semantics
Status: `DONE`

Accepted on 2026-09-10. Month candidate checkpoints carry year/month,
last durable page/news ID, completion, and stop reason. Resume starts at the
next page; HTTP/parser/interruption states remain incomplete. Persistence is
still controlled by the generic durable barrier and RecoveryStore. Focused:
`47 passed`; full unit: `717 passed`.

No JSONL/failed.jsonl production state.

## K8. CLI + profiles
Status: `DONE`

Accepted on 2026-09-10 and amended on 2026-09-11. Added
`kabutan_incremental` and `kabutan_free_full`; `kabutan_full` remains a
free-full compatibility alias. All
four month/page CLI controls, profile/argument validation, manifest defaults,
and AppFactory context propagation. Kabutan requires no instrument input.
Focused: `114 passed`; full unit: `721 passed`.

```text
kabutan_incremental
kabutan_free_full
kabutan_full
--kabutan-start-month
--kabutan-end-month
--kabutan-overlap-months
--kabutan-max-pages-per-month
```

## K9. Manifest + completeness audit
Status: `DONE`

Accepted on 2026-09-10. Kabutan run manifests freeze their expected ordered
month scopes. Completeness checks only those scopes and only counts checkpoint
state with `month_complete=true`; safety-capped checkpoints remain missing.
Focused: `85 passed`; full unit: `723 passed`.

Expected unit of completeness = month scope.

## K10. Real read-only HTTP smoke
Status: `DONE`

Accepted on 2026-09-10. Real current-month page 1 returned 15 unique items;
the first real article (`n202609100904`) produced article/body, reliable detail
datetime, and 4,748 content characters. A real DOM difference (direct table
rows without `tbody`) was fixed and regression-tested. No storage writes were
made. Focused: `22 passed`; full unit: `724 passed`.

Current month page 1 + one real article detail.

## K11. Free-access durable production acceptance
Status: `DONE`

Accepted on 2026-09-11. Current free HTTP produced and normalized 30 real
articles. The generic pipeline wrote one coalesced Parquet batch, uploaded and
registered it once, committed Seen/checkpoint state, and left recovery/errors
at zero. Query Layer returned 30 Tokyo-local-date rows with real content.

## K12. Free-access resume/dedup acceptance
Status: `DONE`

Accepted on 2026-09-11. A real incremental run was interrupted during HTTP
crawl; recovery-only found zero pending/failed manifests. The repeated run
completed with zero files/uploads/Catalog jobs and a valid
`FREE_ACCESS_COMPLETE` checkpoint. Existing durable files and logical records
were not duplicated.

## K13. Free-access production rollout
Status: `BLOCKED_BY_REMOTE_WAF`

Run `kabutan_free_full` from the current month toward history and stop at the
first public/Premium boundary. Premium credentials are neither needed nor
supported. The implementation is complete; the append-only production command
was started on 2026-09-11 but list discovery received HTTP 405 AWS Human
Verification. Recovery-only reported zero pending/failed work and no durable
state was reset. Resume the same command after anonymous HTTP access recovers.
Manual probe on 2026-09-11 returned `WAF_BLOCKED` (HTTP 405). Do not retry
automatically; rerun `python scripts/probe_kabutan_access.py` manually and only
start the same append-only rollout after it reports `AVAILABLE`.
WAF handling focused tests: `48 passed`; full unit: `747 passed`.

## K14. One-click profile acceptance
Status: `BLOCKED_BY_K13`

Accept:
```text
kabutan_incremental
kabutan_free_full
```

`kabutan_incremental` is production accepted. `kabutan_free_full` and its
`kabutan_full` compatibility alias are wired and unit-tested; final acceptance
waits only for the non-destructive K13 production command, not Premium access.


# Phase L — Financial Reports / Filings Integration

> Append to existing todo.md.
> Do not delete or reset Phase K.

## L0. Repository audit + legacy traceability
Status: DONE (2026-09-11)

Trace `get_all_finance_report.py` function-by-function against current generic
dataset/attachment/runtime/query APIs.

Accepted after inspecting the real legacy script and current framework. The
source-only migration map is frozen as follows:

```text
build_session                  -> generic HttpTransport/retry/ProxyPool
normalize_stock_code           -> HKEX instrument helper (L3)
load_stocks                    -> canonical instruments file/importer (L10+)
parse_jsonp                    -> HKEX source parser (L3)
get_stock_info                 -> HKEX exact stockId resolution (L3)
search_reports_by_type         -> HKEX search client (L4)
search_financial_reports       -> HKEX report-type aggregation (L4/L5)
parse_reports                  -> deterministic result parser (L5)
get_release_date               -> aware release-time parser (L6)
process_stock                  -> SitePlugin crawl/normalize/attachments (L7-L9)
main                           -> generic CLI/profiles (L10)
```

The legacy local path builder, CSV metadata/failure logs, PDF downloader,
completed-URL cache, and progress JSON are explicitly superseded by the
generic attachment, SeenStore, checkpoint, RecoveryStore, Parquet, uploader,
Catalog/index, manifest, completeness, and query layers. Baseline and focused
architecture tests both pass; no HKEX production capability is claimed by L0.

## L1. Generic financial_report dataset/schema
Status: DONE (2026-09-11)

Add/verify:
```text
financial_report
financial_report_instrument
attachment
```

Define stable cross-site report metadata contract.

Added reusable `financial_report` and `financial_report_instrument`
DatasetSpecs and their canonical Arrow registry entries. Logical report
metadata remains separate from the existing generic `attachment` dataset;
unknown event time/content remains nullable and no fiscal metadata is inferred.
Focused: `67 passed`; full unit: `753 passed`.

## L2. HKEX site package + builtin registration
Status: DONE (2026-09-11)

Suggested:
```text
src/crawl_framework/sites/hkexnews/__init__.py
src/crawl_framework/sites/hkexnews/filings.py
src/crawl_framework/sites/hkexnews/plugin.py
src/crawl_framework/sites/hkexnews/site.yaml
```

Added a disabled-by-default HTTP site skeleton with the exact
`hkexnews`/HK/Asia-Hong_Kong contract and builtin registration. It declares
only logical report and report-instrument datasets; generic attachment remains
framework-owned. Unsupported and not-yet-implemented paths fail explicitly.
Focused: `30 passed`; full unit: `757 passed`.

## L3. HKEX stockId exact-resolution client
Status: DONE (2026-09-11)

Preserve exact 5-digit match against `prefix.do`.

Implemented strict code normalization, canonical `XHKG:` IDs, JSONP parsing,
legacy request parameters, explicit HTTP/parser errors, and exact-match-only
`HKEXFilingsClient.resolve_stock()`. Prefix result order cannot select a
different security. Focused: `28 passed`; full unit: `770 passed`. Real HTTP
acceptance remains L12.

## L4. HKEX report-search client
Status: DONE (2026-09-11)

Support 40100/40200/40300 and configurable date range.

Added generic HTTP form-body support and exact HKEX POST form construction,
validated configurable dates, normalized report-type codes, and one request
per unique annual/interim/quarterly category. Focused: `34 passed`; full unit:
`778 passed`. Real endpoint acceptance remains L12.

## L5. HKEX result parser + report taxonomy
Status: DONE (2026-09-11)

Normalize:
```text
annual
interim
quarterly
```

Dedup canonical PDF URLs.

Added row-scoped PDF parsing, canonical HKEX URL validation, complete final-cell
title extraction, code/name/release-time extraction, normalized taxonomy, and
deduplication within/across report categories. Focused: `39 passed`; full unit:
`783 passed`. Release time remains raw until L6; real DOM acceptance is L12.

## L6. Release-time normalization
Status: DONE (2026-09-11)

Use Asia/Hong_Kong.
Never fabricate missing time.

Implemented conservative parsing for source-observed Chinese, slash-date, and
compact date-time forms. Values with a reliable minute are interpreted in
Asia/Hong_Kong and converted to aware UTC; missing/date-only/invalid values
remain null. Focused: `41 passed`; full unit: `793 passed`.

## L7. Canonical financial_report normalization
Status: DONE (2026-09-11)

Stable source_id, XHKG instrument_id, payload metadata.

Implemented global logical identity as SHA-256 of canonical PDF URL while
retaining instrument operational scope, canonical report/relation records,
strict source/scope instrument consistency, aware release time, and the full
minimum payload with unknown language/fiscal fields null. Focused: `74 passed`;
full unit: `798 passed`.

## L8. financial_report -> generic attachment integration
Status: DONE (2026-09-11)

No HKEX-specific upload/PDF storage pipeline.

HKEX report metadata now creates a parent-linked generic `AttachmentRequest`,
uses the injected `AttachmentPipeline`, and normalizes the verified result as
canonical `attachment` metadata. Generic PDF validation now requires `%PDF-`
magic even when MIME claims PDF, supports a 1024-byte HKEX minimum, and uses
atomic local finalization. Focused: `89 passed`; full unit: `801 passed`.

## L9. Checkpoint/recovery semantics
Status: DONE (2026-09-11)

Replace CSV/progress files with generic durable checkpoint/recovery.

Implemented instrument-scope request signatures, complete-only checkpoint
candidates, full-mode durable resume, changed-range re-evaluation, and
attachment-complete state. Search/PDF failures generate no advanced candidate;
generic runtime and RecoveryStore remain authoritative. Focused: `98 passed`;
full unit: `805 passed`.

## L10. CLI/options/profiles
Status: DONE (2026-09-11)

Implement:
```text
hkex_reports_incremental
hkex_reports_full
```

Suggested options:
```text
--report-type
--report-date-from
--report-date-to
--download-report-pdf
--no-download-report-pdf
```

Implemented both profiles, all report/date/PDF controls, canonical instrument
discovery, generic HTTP/AppFactory/concurrent-runtime wiring, and a reproducible
2,798-instrument HK snapshot generated from the supplied legacy market CSV
(stable order, source SHA recorded). Focused: `156 passed`; full unit:
`814 passed`. Real HTTP/profile acceptance remains L12-L18.

## L11. Deterministic parser/unit matrix
Status: DONE (2026-09-11)

Cover annual/interim/quarterly, prefix collisions, datetime and PDF validation.

The deterministic matrix now covers all three taxonomy mappings, exact-prefix
collision/invalid stockId behavior, verified and missing release times,
canonical URL/source identity, cross-category identity, HTML-as-PDF rejection,
minimum size, attachment linkage, profiles, and production composition.
Focused: `200 passed`; full unit: `820 passed`.

## L12. One-instrument real HTTP smoke
Status: DONE (2026-09-11)

Metadata only.

Real read-only `XHKG:00005` smoke resolved exact `stockId=5`, returned three
annual reports since 2024, parsed the real HKEX labeled-cell DOM, normalized a
canonical record and aware event time, and performed no PDF download or durable
write. A real-DOM label bug was fixed with regression coverage. Focused:
`58 passed`; full unit: `821 passed`.

## L13. One-instrument durable metadata+PDF smoke
Status: DONE (2026-09-11)

Require Parquet/NAS/Catalog/index/checkpoint/recovery/query.

Real `XHKG:00005`, annual, March 2026 production smoke completed all three
datasets. Each crawled/normalized/new/wrote/uploaded/verified/cataloged one
record/file with zero errors; all checkpoints completed and recovery pending
was zero. Query returned the report and parent-linked attachment. The real PDF
was 12,588,090 bytes and NAS size matched metadata. Focused: `144 passed`; full
unit: `821 passed`.

## L14. 10-instrument mixed report-type smoke
Status: DONE (2026-09-11; audited 2026-09-15)

Must include annual/interim/quarterly evidence.

The stable snapshot GEM slice at offset 2359 completed 10/10 metadata and
10/10 relation scopes with 425+425 normalized records, zero duplicates/errors,
zero pending recovery, and clean checkpoints. Query proved annual=123,
interim=119, quarterly=183. Focused: `140 passed`; full unit: `821 passed`.

The run also exposed 425 event-day Parquet files for 425 logical reports;
remote materialization took 696.34s versus 1.12s scan time. Future financial
report packaging must be coarsened before L16; existing durable files remain
untouched.

## L15. Interruption/resume/dedup smoke
Status: DONE (2026-09-15)

Future financial-report partitions now use yearly time granularity and one
stable bucket while other datasets retain their existing daily/256 defaults.
No existing durable file was rewritten. A real append-only run for snapshot
offset 2369, limit 3 was interrupted after all three metadata scopes became
durable but before the three relation scopes completed. Recovery-only returned
zero pending work; the exact command resumed by skipping metadata and writing
120 relations into one Parquet file. A second identical run crawled zero
records and wrote/uploaded/cataloged zero files. The metadata run wrote 13
year-partition files for 120 reports instead of one file per report. Focused:
`183 passed`; full unit before real acceptance: `823 passed`.

## L16. 100-instrument rollout
Status: DONE (2026-09-15)

The authoritative snapshot prefix through `XHKG:00116` completed 100 metadata
and 100 relation scopes through the production CLI. It discovered 3,663 rows
per dataset; existing durable state deduplicated 479 metadata and one relation,
while this run wrote 3,184 metadata rows into 20 yearly files and 3,662
relations into one file. All 21 files uploaded and cataloged. Recovery-only was
clean, completeness found no missing/failed/partial scopes, and a read-only
21-file NAS audit found no missing path, size, SHA256, storage, or lifecycle
mismatch. Real query smokes passed for new yearly data and old daily/hash data.

Two real HKEX variants were fixed during rollout: exact stock lookup returning
no current match now completes an auditable empty scope, while resolver/parser
errors still fail; dual-counter fields such as `00016 80016` are accepted only
when the requested scope code is explicitly present. Query pruning now shares
the DatasetSpec bucket policy and includes declared legacy buckets. Focused:
`110 passed`; full unit: `831 passed`.

## L17. Full configured HK universe rollout
Status: DONE (2026-09-16)

The append-only 2,798-instrument run durably completed 2,505 matching metadata
checkpoints before real dual-counter alias `XHKG:80016` exposed a validation
gap. Exact resolver evidence is now carried only inside the adapter and permits
a secondary-counter scope when the title-search row displays the primary code;
unproven aliases still fail. Focused: `114 passed`; full unit: `835 passed`.

The exact command resumed from existing checkpoints without clearing state.
Both datasets completed all 2,798 instruments (5,596 scopes total), with zero
failed/partial/missing scopes and zero pending recovery. The relation resume
crawled 53,657 rows, retained 53,079 new rows after SeenStore dedup, and
wrote/uploaded/cataloged five coalesced files. All five NAS size/SHA256 values
match Catalog. A generic synchronous limiter sleep in the async transport was
fixed with regression coverage. No durable data was reset or overwritten.

## L18. Profiles/query/completeness final acceptance
Status: DONE (2026-09-16)

Accept:
```text
hkex_reports_incremental
hkex_reports_full
```

Both profiles ran through the production CLI. Incremental performed real HTTP
discovery and durable dedup/checkpoint work; full then verified the complete
snapshot through matching checkpoints with zero recrawl or new files.
Completeness reports 5,596 expected/completed scopes and zero problems. Query
Layer read real metadata and relation rows from Catalog/NAS. Focused:
`232 passed`; full unit: `837 passed`.

Phase L status: COMPLETE.

# Phase M — Production Orchestration, Resume & Progress UX

> Preserve the existing crawler, durable storage, and site semantics. Phase M
> composes the current SeenStore, checkpoints, RecoveryStore, staged runtime,
> uploader, Catalog, and manifests into a resumable production UX.

## M0. Current-state audit

Status: DONE (2026-09-16)

Audited the real production path, all nine single-site profiles, recovery
stages, checkpoint ownership, progress snapshots, CLI/AppFactory wiring, and
run manifests. The durable barrier and startup recovery are real; there is no
generic pre-crawl ResumePlanner, global orchestrator, shared cross-site
resource budget, interrupt-safe manifest, or restart-aware dashboard yet.
No production crawl or state mutation was performed.
Focused architecture/recovery tests: `178 passed`; full unit suite:
`837 passed`.

## M1. Generic ResumePlanner / ResumePlan

Status: DONE (2026-09-16)

Classify every planned site/dataset/scope as `DURABLE_COMPLETE`,
`RECOVERY_PENDING`, `INCOMPLETE`, `BLOCKED`, `FAILED_RETRYABLE`, or
`FAILED_TERMINAL`, with stable aggregate counts and no durable-state mutation.

Implemented a deterministic, read-only planner with explicit site completion,
recovery, and blocked-state evidence adapters. It preserves input order,
rejects duplicate scope tokens, exposes crawlable scopes and stable manifest
serialization, and never mutates checkpoint/recovery state. Focused:
`71 passed`; full unit: `847 passed`.

## M2. Recovery-first startup integration

Status: DONE (2026-09-16)

Run safe pending recovery before planning/crawl, then rebuild the plan from
post-recovery state. Keep `--recovery-only` for diagnostics.

AppFactory now injects ResumePlanner into the production concurrent runtime.
CrawlBootstrap completes recovery first; runtime discovery then builds a plan
from the updated checkpoints and skips proven durable scopes before
`plugin.crawl()`. HKEX matching full checkpoints and old completed Kabutan
free-full months expose their existing completion semantics through a generic
read-only plugin hook. Naver/Toss remain conservatively incomplete unless they
provide whole-scope proof. Focused: `100 passed`; full unit: `851 passed`.

## M3. Resumable durable-stage classification

Status: DONE (2026-09-16)

Map local/uploaded/verified/cataloged/Seen/checkpoint states to the minimum
remaining work. Do not recrawl, rewrite, reupload, or re-register stages with
sufficient durable evidence.

RecoveryManifest now persists the exact contributing `scope_tokens` copied
from FlushBatch. A read-only RecoveryStore inspector maps only explicit scope
membership into pending/retryable/terminal plan evidence and exposes the
minimum next action for every durable stage. `uploaded` recovery now verifies
the recorded remote object through a dedicated uploader API and never
retransfers it. Legacy unscoped manifests remain globally recoverable but are
not guessed into a scope. Focused: `149 passed`; full unit: `864 passed`.

## M4. ProgressAggregator

Status: DONE (2026-09-16)

Aggregate platform/site/dataset/scope, record, storage, queue, recovery, and
performance counters. Seed completion from ResumePlan so restarts do not show
zero progress.

Added a thread-safe renderer-independent aggregator with
platform/site/dataset hierarchy, ResumePlan-seeded scope counts, record and
storage metrics, queue current/peak depths, recovery totals, busy times,
throughput, conservative ETA, and JSON-safe immutable snapshots. Production
runtime/AppFactory/Bootstrap now feed it without using progress state for
correctness. Focused: `69 passed`; full unit: `872 passed`.

## M5. Rich/Text progress dashboard

Status: DONE (2026-09-16)

Add `--progress`, `--no-progress`, and `--progress-style auto|rich|text` while
retaining periodic non-interactive text and clean JSON stdout.

Implemented optional Rich Live and dependency-free text renderers. CLI supports
`--progress`, `--no-progress`, `--progress-style auto|rich|text`, and the
existing positive interval option. Auto selects Rich only on a TTY when
installed, otherwise text; explicit Rich also safely falls back to text.
`--json` and `--no-progress` create no renderer. Final snapshots render and
renderer lifecycle always closes. Focused: `130 passed`; full unit:
`881 passed`.

## M6. Graceful Ctrl-C resume

Status: DONE (2026-09-16)

First interrupt stops new scope intake and drains safe durable work; a second
interrupt aborts faster without deleting state. Always await worker tasks and
flush manifest/progress state.

Implemented a generic two-stage ShutdownController and temporary CLI
SIGINT/SIGTERM handlers. The first request prevents discovery/worker intake of
new scopes and drains records already accepted by bounded queues. Interrupted
scopes never execute `checkpoint_after_scope`; safe record-level cursor state
is committed only behind the existing durable batch barrier. A second request
cancels and gathers all stage tasks, skips optional end flush, closes progress,
and preserves replay/recovery state. Interrupted Bootstrap results are
manifest-serializable and CLI exits 130. Focused: `144 passed`; full unit:
`888 passed`.

## M7. Global profiles/orchestrator

Status: DONE (2026-09-17)

Add `run-platform --profile global_full|global_incremental`, mapping the four
accepted single-site profiles. Kabutan WAF is a BLOCKED site outcome and must
not stop the other sites.

Implemented a generic in-process PlatformOrchestrator with stable official
profile mapping, per-site AppFactory/Bootstrap reuse, startup recovery,
failure isolation, shared graceful shutdown, global recovery-only mode, text
and clean JSON summaries. Kabutan performs the existing one-request access
probe before crawl; `WAF_BLOCKED` skips only Kabutan and does not fail the
platform run. Named production profiles are forced onto the bounded concurrent
runtime even with one worker per stage. M7 remains sequential by design;
cross-site concurrency and shared stage budgets are M8. Focused: `148 passed`;
full unit: `899 passed`.

## M8. Global resource budgets

Status: DONE (2026-09-17)

Add `--site-workers` and shared writer/upload/Catalog budgets. NAS upload
concurrency is global, defaults conservatively, and does not bypass site rate
limits.

Implemented concurrent in-process site scheduling and an observable shared
GlobalStageBudget. CLI defaults to two site, writer, upload, and Catalog
workers, with global upload capped at four. Runtime Parquet/end flush,
upload/verify, Catalog/index, startup recovery, attachment uploads, and
compaction all acquire the same cross-site permits. Site-specific crawl,
Playwright, HTTP and adaptive rate limits remain unchanged. Focused:
`173 passed`; full unit: `907 passed`.

## M9. Global run manifest

Status: DONE (2026-09-17)

Write an atomic platform manifest containing site profiles, universes, resume
plan, counters, recovery, blocked sites, errors, and final/interrupted state.

`run-platform` now always writes the aggregate manifest before returning. The
default path is
`state/run_manifests/platform_<profile>_<timestamp>.json`; operators and tests
may override it with `--run-manifest-path`. The same parsed per-site options
used by the orchestrator supply profile, dataset, and universe metadata. JSON
is written to a same-directory temporary file and atomically replaced. Normal,
WAF-blocked, failed, and controlled-interrupt states are covered. Focused:
`193 passed`; full unit: `914 passed`.

## M10. Deterministic resume/progress matrix

Status: DONE (2026-09-17)

Cover cold/partial/complete starts, recovery stages, replay dedup, blocked and
failed scopes, zero-row scopes, attachment/compaction, TTY/text/JSON, and
interrupt behavior.

Added one concentrated runtime matrix over the production ResumePlanner,
FileCheckpointStore, ConcurrentProductionRuntime, ProgressAggregator, bounded
durable stages, and ShutdownController. It proves cold/50%/100% restart
seeding, all six resume classifications, checkpoint-complete zero-work rerun,
zero-row completion, unchanged replay with no file/upload/Catalog work,
attachment/compaction metrics, and interrupted-scope incompleteness. The
focused matrix also includes existing real RecoveryManager stage, SeenStore,
pipeline, renderer, CLI JSON, attachment, and compaction tests. Focused:
`249 passed`; full unit: `923 passed`.

## M11. Interrupted small global smoke

Status: DONE (2026-09-17)

Run only the prescribed 2-instrument-per-supported-site smoke, interrupt it,
and preserve all durable state. Kabutan remains probe-gated.

Executed the real `global_incremental` production entry with deterministic
two-instrument prefixes for Naver, TossInvest, and HKEX, four bounded site
workers, shared `2/2/2` writer/upload/Catalog budgets, coalesced scope flushes,
and the existing Kabutan probe. The first run exposed Playwright driver
teardown being misclassified as a Toss failure after SIGINT. Generic runtime
and platform shutdown classification were fixed and regression-tested without
weakening storage-stage failures. The retry ended with Naver/Toss/HKEX
`INTERRUPTED`, Kabutan `BLOCKED` by WAF, no failed sites, no pending recovery,
and an atomic interrupted platform manifest. Focused: `113 passed`; full unit:
`926 passed`.

## M12. Exact-rerun dedup acceptance

Status: TODO

Rerun the exact M11 command and prove completed scopes and durable stages are
skipped, pending recovery is completed, no duplicate physical/logical data is
created, and progress starts from existing completion.

## M13. global_incremental acceptance

Status: TODO

Accept the one-command four-site incremental UX, shared budgets, dashboard,
platform manifest, and resumability. Do not run destructive global full
operations as part of this acceptance.
