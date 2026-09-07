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
