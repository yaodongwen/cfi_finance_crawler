# Crawl Framework V2 — PROJECT STATUS

> This file records actual current progress.
>
> The repository code + tests are the factual source of truth.
> `Crawl_Framework_V2_Codex_Master_Plan.md` defines the execution roadmap.
>
> Codex must update this file after every completed roadmap item.

---

# 1. Current Baseline

Environment:

```text
Python:
/opt/anaconda3/envs/pac/bin/python

Project import:
/Users/yaodongdong/WorkPlace/intern/crawler/crawl_framework/src/crawl_framework/__init__.py
```

Git state at last confirmation:

```text
modified roadmap/status plus Phase B and Phase C implementation files
```

Current unit-test baseline:

```text
527 passed
```

Focused PostgreSQL tests:

```text
28 passed
```

Focused query tests:

```text
20 passed
```

Focused query-data script tests:

```text
4 passed
```

Focused compaction tests:

```text
4 passed
```

Focused audit tests:

```text
7 passed
```

Focused repair tests:

```text
4 passed
```

Focused retention/cleaner tests:

```text
pytest -q tests/unit/test_retention.py
5 passed

pytest -q tests/unit/test_cleaner.py
14 passed
```

Focused recovery robustness tests:

```text
pytest -q tests/unit/test_recovery_faults.py
7 passed

pytest -q tests/unit/test_recovery.py
32 passed

pytest -q tests/unit/test_recovery_orchestrator.py
10 passed
```

Focused scale validation tests:

```text
pytest -q tests/unit/test_query_multi_instrument.py
7 passed

pytest -q tests/unit/test_scale.py
6 passed
```

Focused architecture proof tests:

```text
pytest -q tests/unit/test_tossinvest_plugin.py tests/unit/test_tossinvest_architecture.py tests/unit/test_builtin_sites.py
8 passed

pytest -q tests/unit/test_runtime.py tests/unit/test_pipeline.py tests/unit/test_query.py
51 passed
```

---

# 2. Completed Roadmap Items

## PHASE A — Core correctness

- [x] A1. Fix PostgreSQL UPSERT physical-state regression
- [x] A2. Add regression tests
- [x] A3. Run full unit suite

A1 result:

`build_insert_data_file_sql()` conflict update now updates metadata only and does not overwrite:

```text
storage_status
remote_path
lifecycle_status
```

Regression tests exist in `tests/unit/test_postgres.py`.

Phase A accepted baseline:

```text
396 passed
```

---

# 3. Current Roadmap Item

```text
PHASE H
H20. Naver full-market production run
```

Status:

```text
PAUSED: user requested H21 before completing H20.
H21 multi-site reuse proof is complete.
H20 full-market production run remains the next incomplete production rollout item.
```

H18.5 actual production call graph before fix:

```text
crawl_framework.cli.main
-> build_default_bootstrap()
-> AppFactory.build()
-> CrawlBootstrap.run()
-> CrawlRuntime.run()
-> CrawlRuntime.run_dataset()
-> serial await CrawlRuntime.run_scope(scope)
-> plugin.crawl()
-> plugin.normalize_and_validate()
-> StoragePipeline.submit()
-> RecordBuffer.add()
-> StoragePipeline.flush_scope()/process_batch()
-> ParquetWriter.write_batch()
-> RecordIndexStore.write_for_parquet()
-> RecoveryStore.save()
-> BaseUploader.upload()
-> remote verify
-> PostgresCatalog.register_parquet_file()/mark_uploaded()
-> SeenStore.commit_many()
-> optional CheckpointStore.save()
-> RecoveryStore advance cleanable/deleted
-> Cleaner.clean()
-> Runtime checkpoint save after barrier
```

Observed gap:

```text
scope 1: crawl -> write -> upload -> catalog -> checkpoint
scope 2: crawl -> write -> upload -> catalog -> checkpoint
```

Root cause:

```text
Phase H components existed, but production AppFactory always instantiated the old CrawlRuntime.
StoragePipeline.submit()/process_batch() also collapsed write/upload/catalog into one synchronous durable path.
```

H9-H16 audit after H18.5:

```text
H9 configurable concurrency        A: implemented and production-wired
H10 target file size               A: implemented and production-wired through RecordBuffer config
H11 bounded pipeline               A: implemented and production-wired for multi-worker production CLI
H12 parallel upload                A: implemented and production-wired through upload workers
H13 parallel Catalog/index         A: implemented and production-wired through catalog workers
H14 checkpoint/watermark           C: scope-level checkpoint waits for all batches containing the scope; ordered watermark remains unit-level
H15 recovery                       C: existing recovery remains startup safe; interrupted concurrent production smoke still required
H16 CLI wiring                     A: AppFactory selects ConcurrentProductionRuntime when stage workers > 1
```

H18.5 implementation:

```text
StoragePipeline split durable lifecycle into reusable stages:
  prepare_batch()
  upload_prepared_batch()
  catalog_uploaded_batch()

Added submit_for_staged_runtime() so concurrent runtime can inspect/buffer without synchronously uploading/cataloging.

Added ConcurrentProductionRuntime:
  crawl workers -> bounded record queue
  writer workers -> bounded upload queue
  upload workers -> bounded catalog queue
  catalog workers -> SeenStore/checkpoint completion

Production observability records:
  crawl_workers / writer_workers / upload_workers / catalog_workers
  records_crawled
  files_written
  uploads_started / uploads_completed
  catalog_jobs_started / catalog_jobs_completed
  max record/upload/catalog queue depth
  crawl/writer/upload/catalog busy time
  queue put waits
  crawl-upload / upload-catalog / crawl-catalog overlap booleans
```

H18.5 validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_concurrent_production_runtime.py tests/unit/test_app_factory.py tests/unit/test_pipeline.py tests/unit/test_cli_main.py
68 passed

pytest -q -p no:rerunfailures tests/unit
524 passed

3-instrument production smoke:
python -m crawl_framework.cli.main crawl --site naver_finance --dataset forum_post --instrument XKRX:490490 --instrument XKRX:377740 --instrument XKRX:004690 --max-pages 1 --crawl-workers 2 --writer-workers 1 --upload-workers 2 --catalog-workers 2 --target-file-size-mb 128 --json
success=true
remaining_pending=0
records_crawled=59
files_written=41
uploads_started=41
uploads_completed=41
catalog_jobs_started=41
catalog_jobs_completed=41
max_record_queue_depth=2
max_upload_queue_depth=39
max_catalog_queue_depth=2
crawl/upload/catalog intervals overlapped

50-instrument same-snapshot resume validation:
python -m crawl_framework.cli.main crawl --site naver_finance --dataset forum_post --instruments-file config/universes/naver_finance_kr_rollout_universe.txt --instrument-limit 50 --max-pages 1 --crawl-workers 4 --writer-workers 1 --upload-workers 2 --catalog-workers 2 --target-file-size-mb 128 --json
success=true
remaining_pending=0
records_crawled=987
files_written=47
uploads_started=47
uploads_completed=47
catalog_jobs_started=47
catalog_jobs_completed=47
max_record_queue_depth=3
max_upload_queue_depth=11
max_catalog_queue_depth=2
crawl/upload/catalog intervals overlapped

Post-validation recovery-only:
success=true
remaining_pending=0

H19 first concurrent 500-instrument attempt:
failed due transient Naver detail 404:
  https://m.stock.naver.com/front-api/discussion/detail?id=428617177

Post-failure recovery-only:
attempted=4
recovered=4
remaining_pending=0

Fix:
NaverForumClient.fetch_detail() now treats detail 404 as a missing/deleted post.
NaverForumClient.fetch_post()/crawl_pages() skips that one raw item without aborting the whole production run.

404 fix validation:
pytest -q -p no:rerunfailures tests/unit/test_naver_forum_post.py tests/unit/test_naver_finance_plugin.py tests/unit/test_concurrent_production_runtime.py
50 passed

pytest -q -p no:rerunfailures tests/unit
526 passed

H19 same-snapshot first 500 resume after 404 fix:
python -m crawl_framework.cli.main crawl --site naver_finance --dataset forum_post --instruments-file config/universes/naver_finance_kr_rollout_universe.txt --instrument-limit 500 --max-pages 1 --crawl-workers 8 --writer-workers 1 --upload-workers 4 --catalog-workers 4 --target-file-size-mb 128 --json
success=true
remaining_pending=0
call_graph=cli.main -> app_factory -> CrawlBootstrap -> ConcurrentProductionRuntime -> crawl_queue -> record_queue -> writer workers -> upload_queue -> upload workers -> catalog_queue -> catalog workers -> SeenStore/checkpoint
production stats included crawl/upload/catalog intervals, worker counts, queue depths, busy times, upload/catalog started/completed counts
tool output was truncated because JSON included all 500 scope results and full interval arrays

H19 post-run recovery-only:
success=true
attempted=0
failed=0
remaining_pending=0

H19 post-run unit validation:
pytest -q -p no:rerunfailures tests/unit
526 passed

H20 pause/interruption note:
H20 full-market run was started from the same 3924-instrument snapshot, then paused by user request to complete H21 first.
Interrupt exposed upload worker exception reporting and a remote mkdir locale-warning issue that should be addressed before resuming H20.

Post-H20-interrupt recovery-only:
success=true
attempted=5
recovered=5
failed=0
remaining_pending=0

H21 multi-site reuse proof:
TossInvest fixture-backed second site now has a concurrent production runtime smoke using the same generic stack:
  ConcurrentProductionRuntime
  StoragePipeline
  SeenStore
  RecordBuffer
  ParquetWriter
  LocalUploader
  PostgresCatalog-compatible fake connection
  RecordIndexStore
  FileCheckpointStore
  RecoveryStore
  CatalogParquetReader query

H21 validation:
pytest -q -p no:rerunfailures tests/unit/test_tossinvest_plugin.py tests/unit/test_tossinvest_architecture.py tests/unit/test_builtin_sites.py tests/unit/test_concurrent_production_runtime.py
12 passed

pytest -q -p no:rerunfailures tests/unit
527 passed
```

## PHASE B — Query operational maturity

- [x] B1. Streaming/query observability
- [x] B2. Large universe input
- [x] B3. Production query smoke tests

B1 result:

`CatalogParquetReader.iter_batches()` remains backward compatible and now accepts an optional `StreamingQueryStats` collector.

Streaming stats include:

```text
catalog_sql_calls
catalog_files
parquet_files_materialized
parquet_files_read
candidate_physical_rows
rows_yielded
bytes_materialized
batches_yielded
query_duration_seconds
catalog_duration_seconds
materialization_duration_seconds
scan_duration_seconds
```

`scripts/query_data.py` stream output now reports the richer stats while preserving existing stream row behavior.

Modified files:

```text
src/crawl_framework/storage/query.py
scripts/query_data.py
tests/unit/test_query.py
project_status.md
```

B1 validation:

```text
pytest -q tests/unit/test_query.py
20 passed

pytest -q tests/unit
398 passed
```

B2 result:

`scripts/query_data.py` now supports `--instruments-file` for UTF-8 files with one instrument per line.

Merge order is explicit:

```text
--instrument
--instruments
--instruments-file
```

Blank lines are ignored, whitespace is stripped, and duplicates are removed while preserving first-seen order. Supplying an instruments file that produces an empty effective instrument set raises an error.

B2 modified files:

```text
scripts/query_data.py
tests/unit/test_query_data_script.py
project_status.md
```

B2 validation:

```text
pytest -q tests/unit/test_query_data_script.py
4 passed

pytest -q tests/unit/test_query.py
20 passed

pytest -q tests/unit
402 passed
```

B3 result:

Read-only production query smoke tests passed against Naver `forum_post`.

Validated:

```text
single instrument query
3-instrument query
--instruments-file query
Asia/Seoul local natural date filter
streaming query
max_rows hard execution limit
JSONL streaming export
Parquet streaming export
streaming observability metrics
```

Production smoke observations:

```text
single XKRX:042700, 2026-08-26 Asia/Seoul:
  catalog_files=1
  parquet_files_read=1
  physical_rows_in_files=63
  rows_returned=53

3 instruments XKRX:000660/XKRX:005930/XKRX:042700:
  catalog_files=4
  parquet_files_read=4
  physical_rows_in_files=123
  rows_returned=113

streaming max_rows=5:
  catalog_sql_calls=1
  catalog_files=4
  parquet_files_materialized=1
  parquet_files_read=1
  candidate_physical_rows=123
  rows_yielded=5
  batches_yielded=3

--instruments-file streaming max_rows=7:
  catalog_sql_calls=1
  catalog_files=4
  parquet_files_materialized=1
  parquet_files_read=1
  candidate_physical_rows=123
  rows_yielded=7

JSONL export:
  /tmp/crawl_framework_b3_export.jsonl
  5 rows

Parquet export:
  /tmp/crawl_framework_b3_export.parquet
  5 rows
```

B3 modified files:

```text
project_status.md
```

B3 validation:

```text
pytest -q tests/unit
402 passed
```

Phase B accepted baseline:

```text
402 passed
```

## PHASE C — Storage maintenance

- [x] C1. Generic compaction
- [x] C2. Generic storage audit
- [x] C3. Safe repair workflows
- [x] C4. Retention/cleanup policy

C1 result:

Added a generic storage compaction subsystem that can:

```text
group active/uploaded Catalog files by site/country/dataset/partition_date/bucket
run dry-run compaction without writing replacement files
deduplicate records by record_uid
write same-partition part-compacted replacement Parquet files
publish replacements in safe order:
  upload replacement
  register replacement
  mark replacement uploaded
  mark old active files superseded
```

C1 modified files:

```text
src/crawl_framework/storage/compaction.py
tests/unit/test_compaction.py
project_status.md
```

C1 validation:

```text
pytest -q tests/unit/test_compaction.py
4 passed

pytest -q tests/unit
406 passed
```

C2 result:

Added a read-only generic storage audit subsystem with structured reports.

Audit detects:

```text
active uploaded files with missing remote_path
uploaded Catalog rows whose remote file is missing
remote file_size mismatch
remote checksum mismatch
duplicate conflicting Catalog metadata
active files that are not uploaded
orphan remote Parquet files
```

C2 modified files:

```text
src/crawl_framework/storage/audit.py
tests/unit/test_audit.py
project_status.md
```

C2 validation:

```text
pytest -q tests/unit/test_audit.py
7 passed

pytest -q tests/unit
413 passed
```

C3 result:

Added explicit safe repair workflows for deterministic Catalog state transitions.

Supported repair-safe actions:

```text
mark_uploaded
mark_superseded
mark_archived
```

Repairs are dry-run by default, never delete files, preserve action order, and require `remote_path` for `mark_uploaded`.

C3 modified files:

```text
src/crawl_framework/storage/repair.py
tests/unit/test_repair.py
project_status.md
```

C3 validation:

```text
pytest -q tests/unit/test_repair.py
4 passed

pytest -q tests/unit
417 passed
```

C4 result:

Added an explicit retention policy layer.

Policy covers:

```text
no deletion before upload/verify/Catalog/SeenStore/checkpoint durability barrier
keep local failures by default
manifest/sidecar retention window
superseded rollback window
archive-before-delete decision
dry-run deletion candidates that are not allowed to mutate state
```

Existing `Cleaner` barrier behavior remains intact.

C4 modified files:

```text
src/crawl_framework/storage/retention.py
tests/unit/test_retention.py
project_status.md
```

C4 validation:

```text
pytest -q tests/unit/test_retention.py
5 passed

pytest -q tests/unit/test_cleaner.py
14 passed

pytest -q tests/unit
422 passed
```

Phase C accepted baseline:

```text
422 passed
```

Next roadmap phase:

```text
PHASE D — Recovery robustness
D1. Fault-injection tests
D2. Recovery idempotency
D3. Crash-boundary validation
```

## PHASE D — Recovery robustness

- [x] D1. Fault-injection tests
- [x] D2. Recovery idempotency
- [x] D3. Crash-boundary validation

D1/D2/D3 result:

`RecoveryManager` now accepts an optional no-op-by-default fault injector.

Fault tests validate transient crashes after these durable stages:

```text
uploaded
verified
catalog_registered
seen_committed
checkpoint_committed
cleanable
```

Each injected crash records a retryable failed manifest with the correct `failed_from_stage`. A subsequent recovery run resumes from that stage and converges to `deleted`, with SeenStore restored from sidecar where available.

Crash-boundary validation also covers a cleanable manifest where the local Parquet has already disappeared before the manifest advanced; recovery converges to `deleted` instead of failing.

D modified files:

```text
src/crawl_framework/storage/recovery.py
tests/unit/test_recovery_faults.py
project_status.md
```

D validation:

```text
pytest -q tests/unit/test_recovery_faults.py
7 passed

pytest -q tests/unit/test_recovery.py
32 passed

pytest -q tests/unit/test_recovery_orchestrator.py
10 passed

pytest -q tests/unit
429 passed
```

Phase D accepted baseline:

```text
429 passed
```

Next roadmap phase:

```text
PHASE E — Scale validation
E1. 100/1000-instrument query tests
E2. Memory testing
E3. File-size / compaction tuning
E4. Longer production runs
```

## PHASE E — Scale validation

- [x] E1. 100/1000-instrument query tests
- [x] E2. Memory testing
- [x] E3. File-size / compaction tuning
- [x] E4. Longer production runs

E result:

Added repeatable scale-validation guardrails.

Validated:

```text
100-instrument query uses one multi-bucket Catalog query
1000-instrument query uses one multi-bucket Catalog query
unique bucket count remains bounded by bucket space
streaming memory observations require no full-result accumulation
file-size observations record total/min/max bytes and rows for tuning
production rollout plans above 50 instruments require explicit staged authorization unless read-only
```

Read-only production scale smoke:

```text
100 instruments, stream max_rows=1:
  catalog_sql_calls=1
  catalog_files=2
  candidate_physical_rows=83
  rows_yielded=0

1000 instruments, stream max_rows=1:
  catalog_sql_calls=1
  catalog_files=4
  candidate_physical_rows=123
  rows_yielded=1
```

E4 note:

No real long-running production crawler was started. The implemented guardrail records and validates rollout plans, and blocks large production write rollouts without explicit staged authorization.

E modified files:

```text
src/crawl_framework/storage/scale.py
tests/unit/test_scale.py
tests/unit/test_query_multi_instrument.py
project_status.md
```

E validation:

```text
pytest -q tests/unit/test_query_multi_instrument.py
7 passed

pytest -q tests/unit/test_scale.py
6 passed

pytest -q tests/unit
437 passed
```

Phase E accepted baseline:

```text
437 passed
```

Next roadmap phase:

```text
PHASE F — Architecture proof
F1. Second-site integration
F2. Verify core remains unchanged
F3. Production smoke test
```

## PHASE F — Architecture proof

- [x] F1. Second-site integration
- [x] F2. Verify core remains unchanged
- [x] F3. Production smoke test

F result:

Added TossInvest as a second site plugin.

Implemented:

```text
site metadata
dataset declaration
instrument discovery
fixture-backed crawl
forum_post normalization
news_article normalization with instrument relations
checkpoint_after_record
builtin site registration
```

Architecture proof:

```text
TossInvest fixture raw
→ CrawlRuntime
→ StoragePipeline
→ SeenStore dedup across runs
→ local Parquet write
→ LocalUploader verification
→ PostgreSQL Catalog registration calls
→ checkpoint commit
→ CatalogParquetReader query
```

Core verification:

```text
No core/runtime/pipeline/query changes were required for TossInvest.
Existing runtime, pipeline, and query tests pass.
```

F3 smoke note:

TossInvest does not yet have a real production transport wired in this repository. The production smoke for Phase F was performed as a local fixture-backed end-to-end smoke using the production core stack and local uploader/fake Catalog. No external TossInvest crawl was attempted.

F modified files:

```text
src/crawl_framework/sites/tossinvest/plugin.py
src/crawl_framework/sites/tossinvest/__init__.py
src/crawl_framework/sites/builtin.py
tests/unit/test_tossinvest_plugin.py
tests/unit/test_tossinvest_architecture.py
tests/unit/test_builtin_sites.py
project_status.md
```

F validation:

```text
pytest -q tests/unit/test_tossinvest_plugin.py tests/unit/test_tossinvest_architecture.py tests/unit/test_builtin_sites.py
8 passed

pytest -q tests/unit/test_runtime.py tests/unit/test_pipeline.py tests/unit/test_query.py
51 passed

pytest -q tests/unit
443 passed
```

Phase F accepted baseline:

```text
443 passed
```

Next roadmap phase:

```text
PHASE G — Rollout
G1. 50 instruments
G2. 500 instruments
G3. Full market
G4. Monitor and tune
```

---

# 4. Next Items

```text
G1. 50 instruments
G2. 500 instruments
G3. Full market
G4. Monitor and tune
```

## PHASE G — Rollout

- [ ] G1. 50 instruments
- [ ] G2. 500 instruments
- [ ] G3. Full market
- [ ] G4. Monitor and tune

G preflight result:

Crawler CLI now supports `--instruments-file`, matching the query CLI's large-universe workflow. Repeated `--instrument` values are merged before file values, blank lines are ignored, whitespace is stripped, and duplicates are removed while preserving first-seen order.

Added a rollout guard module that records the authoritative rollout order:

```text
50 instruments
500 instruments
full market
```

It validates stage sizes and evaluates expansion readiness from operational observations:

```text
scope failure rate
recovery failures
pending recovery
maximum file size
```

G1 production execution status:

```text
not completed
```

Reason:

```text
The repository does not contain an authoritative 50-instrument rollout universe, and no completed production 50-instrument crawl observation exists yet. Do not mark G1 complete from synthetic or arbitrary instrument lists.
```

G preflight modified files:

```text
src/crawl_framework/cli/main.py
src/crawl_framework/rollout.py
tests/unit/test_cli_main.py
tests/unit/test_rollout.py
project_status.md
```

G preflight focused validation:

```text
pytest -q tests/unit/test_cli_main.py tests/unit/test_rollout.py tests/unit/test_app_factory.py
48 passed
```

G preflight full validation:

```text
pytest -q tests/unit
453 passed
```

## PHASE H — Universal Parallel Production Pipeline

Authoritative execution order:

```text
Naver_Universal_Parallel_Production_Pipeline_Plan.md Section 22
```

- [x] H0. Repair interrupted Codex changes and restore green tests
- [x] H1. Consolidate canonical Naver package path
- [x] H2. Define generic SiteAdapter/capability contract
- [x] H3. Finish Naver instrument discovery interface
- [x] H4. Naver news adapter
- [x] H5. Naver forum adapter integration
- [x] H6. Naver research metadata adapter
- [x] H7. Generic attachment subsystem + Naver PDF
- [x] H8. Generic proxy-pool transport integration
- [x] H9. Configurable stage concurrency
- [x] H10. Configurable target package/file size
- [x] H11. Bounded asynchronous production pipeline
- [x] H12. Parallel upload workers
- [x] H13. Parallel Catalog/index workers
- [x] H14. Parallel-safe checkpoint/watermark
- [x] H15. Failure/retry/recovery tests
- [x] H16. Generic CLI wiring
- [x] H17. Naver 3-instrument smoke
- [x] H18. Naver 50-instrument production run
- [x] H18.5. Production Runtime Concurrency Integration Audit & Fix
- [x] H19. Naver 500-instrument production run
- [ ] H20. Naver full-market production run
- [x] H21. Multi-site reuse proof with second adapter/mock

H0 result:

Inspected the interrupted universe-resolution diff and found duplicate Naver implementations:

```text
web/naver/
src/crawl_framework/web/naver/
```

Consolidated production code to the installable package path:

```text
src/crawl_framework/web/naver/
```

The root-level `web/naver/` independent implementation was removed.

H0 preserved and repaired:

```text
Naver market-sum parser/client
rollout universe snapshot read/write/validation
crawler --instruments-file parsing
Phase G rollout guard helpers
```

H0 modified files:

```text
src/crawl_framework/web/__init__.py
src/crawl_framework/web/naver/__init__.py
src/crawl_framework/web/naver/market_sum.py
src/crawl_framework/rollout_universe.py
src/crawl_framework/rollout.py
src/crawl_framework/cli/main.py
scripts/build_naver_rollout_universe.py
tests/unit/test_naver_market_sum.py
tests/unit/test_rollout_universe.py
tests/unit/test_rollout.py
tests/unit/test_cli_main.py
project_status.md
```

H0 validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_naver_market_sum.py tests/unit/test_rollout_universe.py tests/unit/test_rollout.py tests/unit/test_cli_main.py
46 passed

pytest -q -p no:rerunfailures tests/unit
465 passed
```

Environment note:

Restricted sandbox execution cannot load `pytest_rerunfailures` because it binds a local socket. Earlier real `pac` runs passed with the plugin enabled; for this H0 validation only `rerunfailures` was disabled while keeping `pytest-asyncio` active.

H1 result:

The canonical Naver implementation path is:

```text
src/crawl_framework/web/naver/
```

No production source files remain under root-level `web/naver/`; code imports use:

```text
crawl_framework.web.naver
```

H1 validation:

```text
find web src/crawl_framework/web -maxdepth 4 -type f
only src/crawl_framework/web/naver source files remain; root web contains no source implementation

rg "from web\\.naver|import web\\.naver"
no code imports root-level web.naver
```

H2 result:

Added a generic, reusable site adapter boundary that contains no Naver-specific URL, selector, pagination, response, or identifier rules.

New generic contract:

```text
SiteAdapter
AdapterCapability
CrawlTask
RawFetchResult
AttachmentRequest
```

Boundary ownership:

```text
site adapter:
  instrument discovery
  task discovery
  fetch
  normalize
  attachment discovery

generic framework:
  concurrency
  proxy pool
  buffering
  Parquet packaging
  attachment handling
  upload/verify
  Catalog/index
  SeenStore
  checkpoint
  recovery
  query
```

H2 modified files:

```text
src/crawl_framework/core/adapter.py
tests/unit/test_site_adapter.py
project_status.md
```

H2 validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_site_adapter.py tests/unit/test_plugin.py tests/unit/test_runtime.py
28 passed

pytest -q -p no:rerunfailures tests/unit
471 passed
```

H3 result:

Added the formal Naver instrument discovery interface through:

```text
NaverFinanceAdapter.discover_instruments()
```

Discovery source remains site-owned and Naver-specific:

```text
https://finance.naver.com/sise/sise_market_sum.naver
KOSPI  sosok=0
KOSDAQ sosok=1
```

The rollout universe builder now consumes the adapter interface instead of calling the HTML client directly, so generic rollout code continues to see only canonical instrument IDs.

H3 modified files:

```text
src/crawl_framework/sites/naver_finance/adapter.py
src/crawl_framework/sites/naver_finance/__init__.py
scripts/build_naver_rollout_universe.py
tests/unit/test_naver_finance_adapter.py
project_status.md
```

H3 validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_naver_finance_adapter.py tests/unit/test_naver_market_sum.py tests/unit/test_rollout_universe.py tests/unit/test_naver_finance_plugin.py
37 passed

pytest -q -p no:rerunfailures tests/unit
473 passed
```

H4 result:

Naver news now has an adapter-level task/fetch/normalize path:

```text
NaverFinanceAdapter.discover_tasks("news_article", ...)
NaverFinanceAdapter.fetch("news_article", ...)
NaverFinanceAdapter.normalize("news_article", ...)
```

The adapter accepts a Naver-specific `news_client` for site fetching. Generic core still sees only `CrawlTask`, `RawFetchResult`, `CrawlScope`, and `CanonicalRecord`.

Normalization delegates to the already-tested Naver plugin news semantics, preserving existing source identity:

```text
office_id:article_id
```

H4 modified files:

```text
src/crawl_framework/sites/naver_finance/adapter.py
tests/unit/test_naver_finance_adapter.py
project_status.md
```

H4 validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_naver_finance_adapter.py tests/unit/test_naver_finance_plugin.py tests/unit/test_site_adapter.py
33 passed

pytest -q -p no:rerunfailures tests/unit
475 passed
```

H5 result:

Naver forum posts now have an adapter-level task/fetch/normalize path:

```text
NaverFinanceAdapter.discover_tasks("forum_post", ...)
NaverFinanceAdapter.fetch("forum_post", ...)
NaverFinanceAdapter.normalize("forum_post", ...)
```

The adapter accepts a Naver-specific `forum_client`; generic core still only handles generic task/raw/record objects. Forum normalization delegates to the already-verified plugin path, preserving stable identity by `nid` and keeping page number out of logical identity.

H5 modified files:

```text
src/crawl_framework/sites/naver_finance/adapter.py
tests/unit/test_naver_finance_adapter.py
project_status.md
```

H5 validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_naver_finance_adapter.py tests/unit/test_naver_finance_plugin.py tests/unit/test_naver_forum_post.py
48 passed

pytest -q -p no:rerunfailures tests/unit
477 passed
```

H6 result:

Naver research metadata now has an adapter-level task/fetch/normalize path for:

```text
research_report
research_instrument
```

`research_report` normalization records:

```text
report_id
title
institution
analyst
published_at/event_time
summary/abstract
source_url
pdf_url
relations to instrument_ids
```

PDF download/upload is intentionally not performed in H6; `pdf_url` remains metadata for H7's generic attachment subsystem.

H6 modified files:

```text
src/crawl_framework/sites/naver_finance/adapter.py
tests/unit/test_naver_finance_adapter.py
project_status.md
```

H6 validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_naver_finance_adapter.py tests/unit/test_site_adapter.py tests/unit/test_dataset.py
22 passed

pytest -q -p no:rerunfailures tests/unit
479 passed
```

H7 result:

Added a generic attachment subsystem for downloaded binary artifacts:

```text
AttachmentContent
AttachmentRecord
AttachmentDownloader
LocalAttachmentStore
attachment sha256 / size / MIME / PDF validation
stable attachment remote_relative_path
```

Naver research reports now discover PDF attachments through generic `AttachmentRequest`; Naver owns only `pdf_url` extraction, while content validation and storage remain generic.

H7 modified files:

```text
src/crawl_framework/storage/attachment.py
src/crawl_framework/sites/naver_finance/adapter.py
tests/unit/test_attachment.py
tests/unit/test_naver_finance_adapter.py
project_status.md
```

H7 validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_attachment.py tests/unit/test_naver_finance_adapter.py tests/unit/test_site_adapter.py
19 passed

pytest -q -p no:rerunfailures tests/unit
484 passed
```

H8 result:

Added reusable proxy/HTTP transport foundations:

```text
ProxyPool
ProxyPoolConfig
ProxyEndpoint
HttpTransport
HttpRequest
```

Proxy selection, cooldown, success/failure health reporting, and request proxy injection are generic. Naver adapters do not rotate proxy ports themselves.

H8 modified files:

```text
src/crawl_framework/transports/proxy.py
src/crawl_framework/transports/http.py
tests/unit/test_transports.py
project_status.md
```

H8 validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_transports.py tests/unit/test_naver_finance_adapter.py
15 passed

pytest -q -p no:rerunfailures tests/unit
489 passed
```

H9 result:

Added generic stage concurrency and queue-size configuration:

```text
StageConcurrencyConfig
QueueSizeConfig
FrameworkConfig.runtime
FrameworkConfig.queues
AppConfig.stage_concurrency
AppConfig.queue_sizes
```

Parsed config knobs:

```yaml
runtime:
  crawl_workers
  attachment_workers
  writer_workers
  upload_workers
  catalog_workers

http:
  concurrency

queues:
  records
  durable_files
  uploads
  catalog
  attachments
```

H9 modified files:

```text
src/crawl_framework/core/concurrency.py
src/crawl_framework/config.py
src/crawl_framework/app_factory.py
tests/unit/test_concurrency_config.py
tests/unit/test_app_factory.py
project_status.md
```

H9 validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_concurrency_config.py tests/unit/test_app_factory.py
19 passed

pytest -q -p no:rerunfailures tests/unit
494 passed
```

H10 result:

Configurable package/flush sizing now maps into existing buffer controls:

```yaml
storage:
  target_file_size_mb
  max_rows_per_file
  max_buffer_age_seconds
```

Mapped to:

```text
AppConfig.buffer_target_bytes
AppConfig.buffer_max_rows
AppConfig.buffer_flush_seconds
```

H10 modified files:

```text
src/crawl_framework/config.py
src/crawl_framework/app_factory.py
tests/unit/test_app_factory.py
project_status.md
```

H10 validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_app_factory.py tests/unit/test_buffer.py
31 passed

pytest -q -p no:rerunfailures tests/unit
494 passed
```

H11 result:

Added a reusable bounded async stage pipeline primitive:

```text
PipelineStage
PipelineRunStats
BoundedAsyncPipeline
```

Properties validated:

```text
bounded asyncio.Queue per stage
backpressure through maxsize
multi-worker stage completion without early downstream termination
stage input/output statistics
queue high-water observations
```

H11 modified files:

```text
src/crawl_framework/core/parallel_pipeline.py
tests/unit/test_parallel_pipeline.py
project_status.md
```

H11 validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_parallel_pipeline.py tests/unit/test_concurrency_config.py
8 passed

pytest -q -p no:rerunfailures tests/unit
498 passed
```

H12 result:

Added generic bounded parallel upload workers:

```text
ParallelUploadConfig
ParallelUploadWorkers
ParallelUploadResult
```

The worker consumes `ParquetFileInfo`, calls the existing `BaseUploader.upload()` via `asyncio.to_thread`, tracks queue high-water size, and returns existing `UploadResult` objects. Site adapters still do not call upload/rsync directly.

H12 modified files:

```text
src/crawl_framework/storage/upload_workers.py
tests/unit/test_upload_workers.py
project_status.md
```

H12 validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_upload_workers.py tests/unit/test_uploader.py tests/unit/test_parallel_pipeline.py
16 passed

pytest -q -p no:rerunfailures tests/unit
500 passed
```

H13 result:

Added generic bounded parallel Catalog/index workers:

```text
CatalogIndexJob
ParallelCatalogConfig
ParallelCatalogIndexWorkers
ParallelCatalogResult
```

For each file/job, record-index registration runs before Catalog registration. Parallelism is only across independent files/jobs.

H13 modified files:

```text
src/crawl_framework/storage/catalog_workers.py
tests/unit/test_catalog_workers.py
project_status.md
```

H13 validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_catalog_workers.py tests/unit/test_upload_workers.py tests/unit/test_postgres.py tests/unit/test_pipeline.py
48 passed

pytest -q -p no:rerunfailures tests/unit
502 passed
```

H14 result:

Added generic contiguous watermark tracking for parallel checkpoint safety:

```text
ContiguousWatermark
WatermarkAdvance
```

Out-of-order task completion no longer implies checkpoint advancement past unfinished earlier work. Sites can map page/cursor/task order onto the generic sequence number.

H14 modified files:

```text
src/crawl_framework/core/watermark.py
tests/unit/test_watermark.py
project_status.md
```

H14 validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_watermark.py tests/unit/test_checkpoint.py tests/unit/test_parallel_pipeline.py
16 passed

pytest -q -p no:rerunfailures tests/unit
506 passed
```

H15 result:

Added failure-boundary tests for the new concurrent primitives:

```text
parallel pipeline stage failure propagates
upload worker failure propagates without verified result
catalog worker failure propagates after index hook but before success count
```

Also reran the existing recovery fault/idempotency suites.

H15 modified files:

```text
tests/unit/test_parallel_failure_boundaries.py
project_status.md
```

H15 validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_parallel_failure_boundaries.py tests/unit/test_recovery_faults.py tests/unit/test_recovery.py tests/unit/test_recovery_orchestrator.py
52 passed

pytest -q -p no:rerunfailures tests/unit
509 passed
```

H16 result:

Generic CLI wiring now supports:

```text
crawl subcommand compatibility
--instruments-file
--crawl-workers
--attachment-workers
--writer-workers
--upload-workers
--catalog-workers
--http-concurrency
--target-file-size-mb
```

The old direct form remains compatible:

```text
python -m crawl_framework.cli.main --site ...
```

The documented form is now accepted:

```text
python -m crawl_framework.cli.main crawl --site ...
```

H16 modified files:

```text
src/crawl_framework/cli/main.py
src/crawl_framework/app_factory.py
tests/unit/test_cli_main.py
project_status.md
```

H16 validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_cli_main.py tests/unit/test_app_factory.py tests/unit/test_concurrency_config.py
48 passed

pytest -q -p no:rerunfailures tests/unit
512 passed
```

H17 previous blocked attempt:

Command:

```bash
python -m crawl_framework.cli.main crawl \
  --site naver_finance \
  --dataset forum_post \
  --instrument 005930 \
  --instrument 000660 \
  --instrument 042700 \
  --max-pages 1 \
  --crawl-workers 1 \
  --upload-workers 1 \
  --catalog-workers 1 \
  --target-file-size-mb 128 \
  --json
```

Expected behavior:

```text
append-only Naver forum_post smoke
local warehouse write
rsync upload to /mnt/nas-intern/homes/dwyao/Data/stocklake
PostgreSQL Catalog update in stock_data/marketdata
```

Result:

```text
not completed
```

First sandbox attempt:

```text
connection to 192.168.1.33:5432 failed: Operation not permitted
```

Non-sandbox retry:

```text
bootstrap configuration failed: connection timeout expired
```

Conclusion:

H17 cannot proceed from the current environment because the configured PostgreSQL host is unreachable. No production crawl records were written by this failed startup attempt.

Post-H17-attempt validation:

```text
pytest -q -p no:rerunfailures tests/unit
512 passed
```

H17 completed result:

Command:

```bash
python -m crawl_framework.cli.main crawl \
  --site naver_finance \
  --dataset forum_post \
  --instrument 005930 \
  --instrument 000660 \
  --instrument 042700 \
  --max-pages 1 \
  --crawl-workers 1 \
  --upload-workers 1 \
  --catalog-workers 1 \
  --target-file-size-mb 128 \
  --json
```

Runtime result:

```text
success=true
crawler_started=true
startup recovery scanned=0 attempted=0 failed=0 remaining_pending=0
```

Per-scope result:

```text
XKRX:005930
  raw_count=20
  normalized_count=20
  records_new=20
  files_written=1
  files_uploaded=1
  files_verified=1
  files_registered=1
  files_deleted=1
  pipeline_errors=0
  checkpoint last_nid=428599798 page=1

XKRX:000660
  raw_count=20
  normalized_count=20
  records_new=20
  files_written=1
  files_uploaded=1
  files_verified=1
  files_registered=1
  files_deleted=1
  pipeline_errors=0
  checkpoint last_nid=428599768 page=1

XKRX:042700
  raw_count=20
  normalized_count=20
  records_new=20
  files_written=1
  files_uploaded=1
  files_verified=1
  files_registered=1
  files_deleted=1
  pipeline_errors=0
  checkpoint last_nid=428582904 page=3
```

Production write scope:

```text
append-only forum_post smoke
local warehouse write
rsync upload to /mnt/nas-intern/homes/dwyao/Data/stocklake
PostgreSQL Catalog update in stock_data/marketdata
no destructive delete/overwrite requested
```

H17 modified files:

```text
project_status.md
```

H17 validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_naver_finance_plugin.py tests/unit/test_naver_forum_post.py tests/unit/test_pipeline.py tests/unit/test_cli_main.py
87 passed

pytest -q -p no:rerunfailures tests/unit
512 passed
```

H18 universe:

```text
snapshot: config/universes/naver_finance_kr_rollout_universe.txt
source: NaverFinanceAdapter.discover_instruments via https://finance.naver.com/sise/sise_market_sum.naver
universe_count=3924
H18 instrument_count=50
selection: first 50 canonical instrument IDs from the same snapshot
first3: XKRX:005930, XKRX:000660, XKRX:005935
last_first50: XKRX:003670
```

H18 command:

```bash
python -m crawl_framework.cli.main crawl \
  --site naver_finance \
  --dataset forum_post \
  --instruments-file config/universes/naver_finance_kr_rollout_universe.txt \
  --instrument-limit 50 \
  --max-pages 1 \
  --crawl-workers 1 \
  --upload-workers 1 \
  --catalog-workers 1 \
  --target-file-size-mb 128 \
  --json
```

H18 result:

```text
success=true
crawler_started=true
startup recovery scanned=0 attempted=0 failed=0 remaining_pending=0
first50 checkpoint missing count=0
pipeline errors observed in completed run=0
```

H18 production write scope:

```text
append-only forum_post rollout
local warehouse write
rsync upload to /mnt/nas-intern/homes/dwyao/Data/stocklake
PostgreSQL Catalog update in stock_data/marketdata
no destructive delete/overwrite requested
```

H18 fixes made during rollout:

```text
Naver market-sum discovery now retries transient request errors.
Naver forum client now retries transient request errors/timeouts.
Crawler --instruments-file now ignores snapshot comment metadata lines.
Crawler --instrument-limit derives rollout prefixes from one full snapshot.
Naver plugin accepts canonical XKRX:NNNNNN inputs without double-prefixing.
```

H18 modified files:

```text
config/universes/naver_finance_kr_rollout_universe.txt
src/crawl_framework/cli/main.py
src/crawl_framework/sites/naver_finance/plugin.py
src/crawl_framework/sites/naver_finance/forum_post.py
src/crawl_framework/web/naver/market_sum.py
tests/unit/test_cli_main.py
tests/unit/test_naver_finance_plugin.py
tests/unit/test_naver_forum_post.py
tests/unit/test_naver_market_sum.py
project_status.md
```

H18 validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_naver_finance_plugin.py tests/unit/test_naver_forum_post.py tests/unit/test_cli_main.py tests/unit/test_rollout_universe.py
84 passed

pytest -q -p no:rerunfailures tests/unit
519 passed
```

H19 precondition note:

Before H19/H20 can be honestly treated as Phase H production pipeline rollout, production runtime must consume the configured worker knobs. Current code has generic concurrent primitives, but `CrawlRuntime.run_dataset()` still awaits scopes sequentially.

H19 attempted result:

Command:

```bash
python -m crawl_framework.cli.main crawl \
  --site naver_finance \
  --dataset forum_post \
  --instruments-file config/universes/naver_finance_kr_rollout_universe.txt \
  --instrument-limit 500 \
  --max-pages 1 \
  --crawl-workers 1 \
  --upload-workers 1 \
  --catalog-workers 1 \
  --target-file-size-mb 128 \
  --json
```

Result:

```text
not completed
```

Observed progress before stop:

```text
snapshot universe_count=3924
H19 target_count=500
completed checkpoints among first 500=217
missing checkpoints among first 500=283
```

Reason stopped:

```text
The production runtime was still executing scopes through the conservative sequential durable path. At this pace, H19 was long-running and H20 full-market would be operationally unsafe/unbounded. This exposed an architecture gap: generic concurrent primitives exist, but the production runtime has not yet integrated the H plan's bounded crawl/write/upload/catalog queues with backpressure.
```

Interrupt/recovery result:

```text
manual interrupt produced rsync returncode=20 during cancellation
recovery-only scanned=1 attempted=1 recovered=1 failed=0 remaining_pending=0
```

Post-H19-attempt validation:

```text
pytest -q -p no:rerunfailures tests/unit/test_recovery.py tests/unit/test_recovery_orchestrator.py tests/unit/test_naver_forum_post.py tests/unit/test_cli_main.py
94 passed

pytest -q -p no:rerunfailures tests/unit
519 passed
```

Next required work:

```text
Implement real production runtime integration for configured bounded concurrency/backpressure before retrying H19 and H20.
```

---

# 5. Previously Proven Capabilities

- [x] CanonicalRecord identity/version model
- [x] SeenStore durable semantics
- [x] checkpoint durable barrier
- [x] Parquet storage
- [x] PostgreSQL Catalog
- [x] storage/lifecycle state separation
- [x] active/uploaded filtering
- [x] lifecycle compaction semantics
- [x] single-instrument query
- [x] multi-instrument query
- [x] timezone-aware query
- [x] UTC partition pruning
- [x] bucket pruning
- [x] multi-bucket single-SQL lookup
- [x] Arrow predicate pushdown
- [x] streaming RecordBatch query
- [x] hard max_rows
- [x] JSONL output/export
- [x] streaming Parquet export
- [x] Naver production smoke testing

Known production multi-instrument result:

```text
XKRX:000660
XKRX:005930
XKRX:042700

113 rows
```

Known buckets:

```text
XKRX:005930 -> 5f
XKRX:000660 -> f1
XKRX:042700 -> 20
```

---

# 6. Known Environment Note

A full test run inside a restricted sandbox may fail because `pytest_rerunfailures` attempts to bind a local socket.

If that happens, rerun using the permitted non-sandbox execution environment.

Do not treat that socket restriction as an application regression if the same suite passes in the real `pac` environment.

---

# 7. Update Rules for Codex

After every roadmap item:

1. mark the item complete here;
2. record focused test result;
3. record full unit-test baseline;
4. list modified files;
5. record production smoke result if applicable;
6. move `Current Roadmap Item` to the next incomplete item in Master Plan Section 3.

Never mark an item complete merely because code exists.
It must satisfy its acceptance criteria and tests.
