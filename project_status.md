# Crawl Framework V2 — PROJECT STATUS

> This file records actual current progress.
>
> The repository code + tests are the factual source of truth.
>
> `Crawl_Framework_V2_Codex_Master_Plan.md` defines the core-framework roadmap.
>
> `Naver_Universal_Parallel_Production_Pipeline_Plan.md` / Naver Phase I documents define Naver completion work.
>
> `TossInvest_Legacy_Migration_Plan.md` defines the authoritative TossInvest Phase J migration order.
>
> Codex must update this file after every completed roadmap item.

---

# 0. CURRENT DEVELOPMENT TRACK

Current active development track:

```text
PHASE M — Production Orchestration, Resume & Progress UX
```

Current item:

```text
M6. Ctrl-C graceful resume
```

TossInvest current truth:

```text
architecture compatibility proof                DONE
fixture-backed TossInvestPlugin                  DONE
canonical XKRX symbol mapping                    DONE
forum_post/news_article normalization skeleton   DONE

real Toss Screener discovery                     DONE
generic Playwright production transport          DONE
real Toss forum/community crawler                DONE
real Toss news list crawler                      DONE
real Toss news detail crawler                    DONE
legacy field parity                              DONE
historical baseline/pending-resume integration   DONE
1-stock real production rollout                  DONE
100-stock real production rollout                DONE
full-universe forum rollout                       DONE
full-universe news rollout                        RUNNING/INTERRUPTED
```

Important rule:

```text
A fixture/FakeClient/architecture test does NOT count as real TossInvest site support.
```

Real Toss features may move to `finished.md` only after:

```text
real TossInvest Playwright/HTTP behavior
+ production plugin wiring
+ generic durable pipeline
+ real smoke test
```

Current authoritative Toss migration reference:

```text
TossInvest_Legacy_Migration_Plan.md
```

Legacy source repository:

```text
https://github.com/yaodongwen/toss_nvest_crawl
```

Primary legacy files:

```text
crawler.py
collect_stocks.py
normalizer.py
cache_manager.py
production_runner.py
main.py
```

The generic framework components below MUST be reused and must not be reimplemented
inside the Toss site package:

```text
CanonicalRecord
SeenStore
checkpoint
ConcurrentProductionRuntime
RecordBuffer
ParquetWriter
PostgreSQL Catalog
record index
Uploader
RecoveryStore
Cleaner
Compaction
Query Layer
ProxyPool
```

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

Current unit-test baseline (2026-09-16, M5):

```text
881 passed
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


---

# 8. PHASE J — TossInvest Full Production Migration

Authoritative execution order:

```text
TossInvest_Legacy_Migration_Plan.md
```

Current Phase J checklist:

- [x] J0. Repository audit + legacy traceability
- [x] J1. Generic Playwright transport/browser worker pool
- [x] J2. Real Toss instrument discovery
- [x] J3. Real Toss forum/community client
- [x] J4. Toss forum full field parity
- [x] J5. Real Toss news list discovery
- [x] J6. Real Toss news detail parser
- [x] J7. news_article / news_instrument relation modeling
- [x] J8. Historical baseline + pending/resume semantics
- [x] J9. Browser worker pool + ProxyPool integration
- [x] J10. Dataset-specific concurrency/backpressure
- [x] J11. 1-stock real production smoke
- [x] J12. 4-stock concurrent smoke
- [x] J13. 20-stock production smoke
- [x] J14. 100-stock rollout
- [x] J15. Full Toss KR universe rollout
- [x] J16. toss_incremental / toss_full one-click profiles

## J0 — Repository audit + legacy traceability

Status:

```text
DONE
```

Required first inspection in the target repository:

```text
src/crawl_framework/sites/tossinvest/plugin.py
src/crawl_framework/sites/tossinvest/comments.py
src/crawl_framework/sites/tossinvest/news.py
src/crawl_framework/transports/playwright.py
src/crawl_framework/sites/builtin.py
tests/unit/test_tossinvest_plugin.py
tests/unit/test_tossinvest_architecture.py
```

Required old-source inspection:

```text
toss_nvest_crawl/crawler.py
toss_nvest_crawl/collect_stocks.py
toss_nvest_crawl/normalizer.py
toss_nvest_crawl/cache_manager.py
toss_nvest_crawl/production_runner.py
toss_nvest_crawl/main.py
```

J0 must classify each current Toss capability as one of:

```text
A. real production-wired
B. fixture-backed only
C. interface/normalization only
D. missing
```

Audited classification on 2026-09-07:

```text
A  builtin registration + generic durable pipeline/runtime/query
B  TossInvestPlugin.crawl() fixture source + architecture smokes
C  XKRX mapping + forum/news normalization/relation interfaces
D  Playwright transport + real discovery/forum/news + real rollout
```

Legacy sources reviewed:

```text
crawler.py: page preparation, Screener, forum, news list/detail, crawl_news
collect_stocks.py: collection/proxy/snapshot behavior
normalizer.py: forum relative time and forum/news field parity
cache_manager.py: persistent IDs, JSONL bootstrap, legacy state migration
production_runner.py: browser profile isolation, worker lifecycle, resume states
main.py and README.md: browser launch and verified operational workflow
```

Deliberately excluded from migration: JSONL primary storage, PersistentIdSet
as production SeenStore, Toss-specific Parquet/Catalog/upload, subprocess task
runner, and the old single storage worker.

## J1 — Generic Playwright transport/browser worker pool

Status: `DONE` (2026-09-07).

Implemented generic bounded leases, isolated contexts/persistent worker
profiles, configurable browser/page behavior, recycling and cleanup, and
ProxyPool lifecycle reporting. Focused tests: `11 passed`; full unit suite:
`651 passed`; real Chromium data-page smoke passed.

Legacy references:

```text
main.py
crawler.py::_prepare_page
crawler.py::save_debug_html
production_runner.py::crawl_worker
```

Target:

```text
src/crawl_framework/transports/playwright.py
```

Must own:

```text
Playwright lifecycle
Chromium lifecycle
bounded browser workers
browser/context lease
profile isolation
headless configuration
locale/timezone/viewport
navigation/page timeout
proxy assignment
context recycling
browser crash cleanup
graceful shutdown
```

Toss-specific DOM knowledge must NOT be placed here.

## J2 — Real Toss instrument discovery

Status: `DONE` (2026-09-07).

Real Toss Screener discovery generated the deterministic
`config/universes/tossinvest_kr_rollout_universe.txt` snapshot with 2427
canonical KR instruments. Plugin-interface real smoke returned the stable
first-five prefix. Focused tests: `20 passed`; full unit suite: `655 passed`.

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

Required behavior:

```text
open https://www.tossinvest.com/screener/3
switch to domestic market
protect baseline filters
remove additional filters safely
scroll virtual list
deduplicate by stock_key
A005930 -> XKRX:005930
freeze deterministic rollout universe
```

Expected snapshot:

```text
config/universes/tossinvest_kr_rollout_universe.txt
```

Do NOT hard-code the old observed ~2458 count as current truth.

## J3 — Real Toss forum/community client

Status: `DONE` (2026-09-07). Real AppFactory durable smoke: success, 7 raw
and normalized records, one file written/uploaded/verified/registered, zero
pipeline errors and pending recovery, complete checkpoint. Worker counts
`2/1/1/1`; max queue depths `8/1/1`; configured queue defaults
`1000/128/128`. Focused tests `47 passed`; full unit `658 passed`.

The three previously omitted helper arguments were `record_queue_size`,
`upload_queue_size`, and `catalog_queue_size`. This was classification A:
only an ad-hoc direct runtime constructor omitted them. Production AppFactory
already propagates `AppConfig.queue_sizes`, so generic runtime/config code was
not changed.

Legacy references:

```text
crawler.py::_open_community_and_sort_latest
crawler.py::_extract_visible_comment_cards
crawler.py::crawl_comments
```

Target:

```text
src/crawl_framework/sites/tossinvest/comments.py
```

Required:

```text
open stock page
open community
sort latest
parse virtual cards
full history mode
incremental mode
history-only stop
stable-bottom stop
```

## J4 — Toss forum field parity

Status: `DONE` (2026-09-07). All required legacy forum fields and relative
time quality are mapped. Real post ID controls stable logical identity;
engagement changes retain `record_uid` and change `version_hash` under the
generic `latest` policy. Focused `11 passed`; full unit `659 passed`; real
durable regression smoke succeeded with 10 records and pending recovery 0.

Legacy references:

```text
normalizer.py::normalize_comment
normalizer.py::parse_relative_comment_time
crawler.py::_extract_visible_comment_cards
```

Must preserve at least:

```text
post_id
author
profile image
created_at/event_time
created_at_quality
created_at_text
created_at_raw
follower_count
is_shareholder
content
raw_text
like_count
comment_count
stock_name
stock_url
toss stock_key
```

## J5 — Real Toss news list discovery

Status: `DONE` (2026-09-07). Real production-plugin Samsung smoke found 16
Toss news links in two rounds with source IDs, URLs, list text and stock
evidence. Baseline-incomplete traversal remains distinct from incremental
history-only stopping. Focused `14 passed`; full unit `663 passed`. Whole
news dataset acceptance remains pending J6-J8.

Legacy references:

```text
crawler.py::parse_news_id
crawler.py::collect_news_links
```

Required behavior:

```text
stock /news page
NewsContentResolved list
NewsDetailModalLink/contentParams
news_id
news_url
list_text
virtual-list scrolling
stable bottom
full historical baseline
incremental history-only stop
```

Critical rule:

```text
historical baseline incomplete
!=
safe incremental stop
```

Known IDs must NOT stop historical discovery before baseline completion.

## J6 — Real Toss news detail parser

Status: `DONE` (2026-09-07). Required extraction priority and provenance are
implemented. Real durable smoke persisted one real article with all durable
stages, pending 0 and complete checkpoint. Focused `44 passed`; full unit
`666 passed`.

Legacy reference:

```text
crawler.py::parse_news_article
```

Preserve extraction priority:

```text
page JSON state
-> metadata
-> article DOM/nearby heading
-> list_text
-> auditable fallback
```

Preserve fields:

```text
news_id
title
title_available
title_source
publisher
publisher_source
published_at
published_date
published_at_source
author
author_source
content
raw_text
list_text
news_url
```

Principle:

```text
prefer blank/unknown metadata over fabricated wrong metadata
```

## J7 — News article/instrument relations

Status: `DONE` (2026-09-07). Global article identity plus materialized
per-instrument relation is implemented and real durable smoke passed. Focused
`51 passed`; full unit `667 passed`; pending recovery 0.

Required canonical modeling:

```text
news_article
news_instrument
```

If one article is reachable from multiple stocks:

```text
1 article body
N instrument relations
```

## J8 — Historical baseline + pending/resume semantics

Status: `DONE` (2026-09-07). Baseline state and retryable detail links are
stored per scope; generic durable failures remain in RecoveryStore. The new
generic scope-completion checkpoint hook still commits only after the durable
barrier. Focused `72 passed`; full unit `669 passed`; real bounded checkpoint
smoke and pending-recovery checks passed.

Legacy references:

```text
crawler.py::collect_news_links
crawler.py::crawl_news
cache_manager.py::PersistentIdSet
cache_manager.py::bootstrap_from_jsonl
production_runner.py::classify_resume
```

Map old semantics to:

```text
SeenStore
checkpoint
RecoveryStore
run manifest
```

Do NOT make these old files authoritative again:

```text
_pending_news_links.json
_news_state.json
PersistentIdSet
production_runner state.json
```

## J9 — Browser worker pool + ProxyPool

Status: `DONE` (2026-09-08). Generic config -> AppFactory -> Bootstrap now
owns browser pool and multi-proxy lifecycle. Real production-composition smoke
passed. Focused `63 passed`; full unit `670 passed`.

Reuse generic:

```text
ProxyPool
```

Required:

```text
bounded browser workers
isolated browser contexts/profile directories
per-worker proxy lease
report_success/report_failure
proxy cooldown
context recycle
```

Do NOT reduce the final architecture to a single:

```text
TOSS_PROXY_SERVER
```

## J10 — Dataset-specific concurrency/backpressure

Status: `DONE` (2026-09-08). Generic named browser budgets now independently
bound discovery/forum/news-list/news-detail inside the total pool; staged
record/upload/catalog queues remain bounded. Focused `59 passed`; full unit
`671 passed`.

Support independent bounded budgets for:

```text
instrument discovery
forum browser tasks
news list browser tasks
news detail browser tasks
writer
upload
catalog
```

Production must continue to reuse:

```text
ConcurrentProductionRuntime
bounded queues
crawl/write/upload/catalog overlap
```

Do not recreate the old one-storage-worker architecture.

## J11 — 1-stock real production smoke

Status: `DONE` (2026-09-08).

Real production CLI validation used `XKRX:005930` (`A005930`) for all three
datasets. Results:

```text
forum_post:      records=4, files written/uploaded/registered=1/1/1
news_article:    records=9, files written/uploaded/registered=1/1/1
news_instrument: records=9, files written/uploaded/registered=1/1/1
pipeline errors=0
final recovery remaining_pending=0
```

Read-only query smoke found one active Catalog/Parquet file per dataset and
returned valid real rows from NAS. Checkpoints completed after the durable
barrier. Focused tests: `70 passed`; full unit suite: `671 passed`.

Suggested canonical test instrument:

```text
XKRX:005930
```

Expected Toss source key:

```text
A005930
```

Validate:

```text
real website
forum
news
CanonicalRecord
SeenStore
Parquet
upload
Catalog
record index
checkpoint
recovery
query
```

## J12 — 4-stock concurrent smoke

Status: `DONE` (2026-09-08).

The first four frozen-snapshot instruments completed real forum, article and
relation production runs. Browser concurrency reached 2 without failures;
news crawl/upload/Catalog intervals overlapped; checkpoints and recovery were
clean. Relation discovery is now list-only and bounded article details run in
parallel. Immediate relation rerun wrote zero files. Safe remote compaction
removed legacy duplicate versions and the active four-file view contains 48
unique record UIDs. Focused `63 passed`; full unit `677 passed`.

No proxy endpoints were configured, so this run validated direct isolated
contexts; real proxy health remains unmeasured.

Validate:

```text
browser-worker overlap
profile isolation
proxy allocation
bounded queues
no duplicate logical records
recovery clean
```

## J13 — 20-stock production smoke

Status: `DONE` (2026-09-08).

Frozen-prefix production completed 20/20 scopes for forum, article and
relation with `107/180/180` records. Browser max active was 4, no lease failed,
all queues stayed bounded, maximum RSS was about 763 MB with no swap, all news
pending counts and final recovery pending were zero. Query smoke found 20
active relation files and 192 accumulated unique rows. Focused `63 passed`;
full unit `677 passed`.

Validate:

```text
memory
browser process/context count
CPU
proxy health
queue depth
throughput
failure rate
Catalog/index
query
```

## J14 — 100-stock rollout

Status: `DONE` (2026-09-08).

Use the same frozen Toss universe snapshot.

All three real production datasets completed 100/100 scopes. Forum, article
and relation crawled `550/900/900` records and registered `197/236/82` files.
Browser max active was 4 with no lease failures; bounded queue metrics stayed
healthy and forum upload backpressure was observed. All news pending counts
and final recovery pending were zero. A PostgreSQL Catalog/NAS query returned
25 relation rows for `XKRX:005930`. Focused `74 passed`; full unit `677 passed`.

Acceptance:

```text
missing checkpoints=0
remaining_pending=0
Catalog/storage audit pass
query smoke pass
browser/profile stability
```

## J15 — Full Toss KR universe rollout

Status: `DONE` (accepted 2026-09-09).

Use J2's authoritative current snapshot.

Final acceptance facts:

```text
forum_post checkpoints       2427/2427 (missing 0)
news_article checkpoints     2427/2427 (missing 0)
news_instrument checkpoints  2427/2427 (missing 0)
recovery remaining/failed     0/0
pending news scopes            0
NAS/Catalog full path audit   23349/23349, no missing/orphan/size mismatch
focused/full unit tests       99/684 passed
```

The successful forum suffix manifest is
`state/run_manifests/toss_j15_forum_full_offset_1000.json`: 1427/1427 scopes,
5861 writes/uploads/Catalog jobs, 3 transient browser failures recovered by 3
operation retries. Together with the first 1000 committed checkpoint barrier,
the forum stage is complete. The interrupted article and relation suffixes
resumed from offset 100 in deterministic snapshot order. Every resumed segment
manifest completed successfully; existing durable prefixes were not cleared
or restarted.

The final Catalog reports active uploaded files of forum/article/relation
`9458/12706/1158`. A real Catalog/NAS streaming query returned three Samsung
forum rows in 1.72 seconds. Upload queue saturation at its configured bound
was observed and drained without recovery residue.

Do not assume historical ~2458 count remains exact.

Recommended staged rollout:

```text
forum first
news second
combined profile last
```

Acceptance:

```text
all expected scopes complete
missing checkpoints=0
remaining_pending=0
Catalog/storage audit pass
query smoke pass
run manifest complete
```

## J16 — One-click Toss profiles

Status: `DONE` (accepted 2026-09-09).

Required:

```text
toss_incremental
toss_full
```

Recommended explicit third profile:

```text
toss_historical_backfill
```

Recommended semantics:

```text
toss_incremental:
    forum incremental
    news incremental only after historical baseline complete
    retry pending failures

toss_full:
    full current market with configured safe bounds

toss_historical_backfill:
    full historical forum/news traversal
    resumable baseline completion
```

Target CLI:

```bash
python -m crawl_framework.cli.main crawl \
  --site tossinvest \
  --profile toss_incremental
```

Both required profiles use the frozen Toss universe and production
AppFactory/Bootstrap/ConcurrentProductionRuntime path. Real Samsung smokes for
`toss_incremental` and `toss_full` each crawled forum/article/relation
`3/9/9` rows and completed three write/upload/Catalog barriers with zero
pipeline errors and recovery pending. Focused tests: `113 passed`; full unit
suite: `691 passed`.

The optional `toss_historical_backfill` profile is implemented as an explicit
unbounded, resumable mode. It has not been labeled as a completed full-history
production rollout.

---

# 9. PHASE J Completion Rules

For every completed J item, Codex must record here:

```text
legacy files/functions reviewed
exact behavior migrated
old behavior deliberately not copied
new files/classes/functions
focused tests
full tests/unit baseline
real smoke result where applicable
modified files
```

Do not mark J2/J3/J5/J6 or later production features complete from fixtures alone.

For real site capabilities, completion requires:

```text
real TossInvest browser/HTTP behavior
+ production plugin wiring
+ generic durable pipeline
+ real smoke
```

---

# 10. TossInvest Legacy Architecture — Explicitly Superseded

The following old mechanisms are reference/migration sources only:

```text
output/<stock>/comments/comments.jsonl
output/<stock>/news/news.jsonl
PersistentIdSet as production SeenStore
_pending_news_links.json as production recovery
_news_state.json as production checkpoint
production_runner RunnerState
subprocess-per-stock production runner
one serial storage worker
Toss-specific Parquet writer
Toss-specific PostgreSQL registration
Toss-specific storage_sync pipeline
```

The new framework must keep using:

```text
CanonicalRecord
SeenStore
checkpoint
ConcurrentProductionRuntime
ParquetWriter
Uploader
PostgresCatalog
RecordIndexStore
RecoveryStore
Cleaner
Compaction
Query
ProxyPool
```

---

# 11. PHASE K — Kabutan Market News Integration

Authoritative order: K0 through K14 in
`Kabutan_MarketNews_Integration_Plan.md`.

- [x] K0. Audit framework + get_all.py
- [x] K1. Site package + builtin registration
- [x] K2. List parser + stable news identity
- [x] K3. Article detail parser
- [x] K4. Month discovery/scopes
- [x] K5. Pagination/repeated-page protection
- [x] K6. Real HTTP production wiring
- [x] K7. Checkpoint/recovery
- [x] K8. CLI/profiles
- [x] K9. Manifest/completeness audit
- [x] K10. Real read-only HTTP smoke
- [x] K11. Free-access durable production acceptance
- [x] K12. Free-access interruption/resume/dedup acceptance
- [ ] K13. Free-access production rollout (`BLOCKED_BY_REMOTE_WAF`)
- [ ] K14. Free-access profile acceptance (`BLOCKED_BY_K13`)

## K0 — Audit framework + get_all.py

Status: `DONE` (2026-09-10).

Actual classification:

```text
Kabutan site/plugin/client/CLI         D: missing
generic durable pipeline/query        A: production-proven
HttpTransport abstraction             C: interface/injection only
AppFactory real HTTP composition       D: missing
optional httpx in active environment   missing
```

Legacy behavior reviewed: `create_session`, `get_html`, `clean_text`,
`extract_news_id`, `parse_list_page`, `parse_article_page`, `iter_months`, and
`crawl_month`. JSONL files, local failed files, hard-coded end month, and the
legacy persistence loop are explicitly excluded. Full baseline: `691 passed`.

## K1 — Site package + builtin registration

Status: `DONE` (2026-09-10).

Added `src/crawl_framework/sites/kabutan/`, its disabled HTTP `site.yaml`, and
the builtin factory registration. The plugin contract is exactly
`kabutan` / `JP` / `Asia/Tokyo` / `news_article`; no instrument scope is
introduced. Focused: `4 passed`; full unit: `693 passed`.

## K2 — List parser + stable news identity

Status: `DONE` (2026-09-10).

Added the production-shaped Kabutan list parser and an opt-in canonical
identity-scope override. Kabutan records preserve `scope_type=month` and
`scope_id=YYYY-MM`, while identical `news_id` values discovered on another
page/month retain one record identity. Focused: `12 passed`; full unit:
`698 passed`.

## K3 — Article detail parser

Status: `DONE` (2026-09-10).

Detail parsing now preserves title/category fallbacks, prioritizes a valid
detail-page datetime, cleans the article body, and exposes missing structural
elements without guessing publication time. Focused: `11 passed`; full unit:
`703 passed`.

## K4 — Month discovery/scopes

Status: `DONE` (2026-09-10). Full/incremental month ranges, year boundaries,
Tokyo current month, and exact month scope metadata are covered. Focused:
`16 passed`; full unit: `708 passed`.

## K5 — Pagination/repeated-page protection

Status: `DONE` (2026-09-10). All three natural terminal checks execute before
detail fetch; max-page exhaustion remains partial/safety-capped. Focused:
`20 passed`; full unit: `712 passed`.

## K6 — Real HTTP production wiring

Status: `DONE` (2026-09-10). Generic HTTP sites are now composed by AppFactory
with urllib transport, ProxyPool, AdaptiveRateLimiter, and configurable status
retry. Kabutan uses that context directly. Focused: `65 passed`; full unit:
`715 passed`. Real endpoint acceptance is intentionally deferred to K10.

## K7 — Checkpoint/recovery

Status: `DONE` (2026-09-10). Kabutan month state is merged into a candidate
scope checkpoint, resumed from the next durable page, and saved only through
the unchanged generic durable barrier. Focused: `47 passed`; full unit:
`717 passed`.

## K8 — CLI/profiles

Status: `DONE` (2026-09-10). Both named profiles and all month controls reach
the plugin through the official CLI/AppFactory route, with no instrument
requirement. Focused: `114 passed`; full unit: `721 passed`.

## K9 — Manifest/completeness audit

Status: `DONE` (2026-09-10). Manifests freeze expected month IDs and the
rollout auditor requires a naturally complete checkpoint for every expected
month. Focused: `85 passed`; full unit: `723 passed`.

## K10 — Real read-only HTTP smoke

Status: `DONE` (2026-09-10). Real page 1 yielded 15 items and a real detail
yielded complete article/body/datetime content. The direct-row DOM discrepancy
was fixed with regression coverage. Focused: `22 passed`; full unit:
`724 passed`.

## K11 — Free-access durable production acceptance

Status: `DONE` (2026-09-11). Closed month 2026-08 produced 5,583 normalized
records and 19 active/uploaded files totaling 5,583 Catalog rows. Interruption
and resume preserved seven already-durable batches and added twelve without
duplicates. Query, checkpoint, recovery, and completeness passed with zero
pending/missing/errors. Follow-up inspection proved the terminal response had
locked Premium news rows rather than a natural empty page, so complete-month
acceptance is now defined at the free-access boundary. A fresh current-range
run wrote/uploaded/cataloged one 30-row batch and Query Layer returned 30 rows.

## K12 — Free-access interruption/resume/dedup acceptance

Status: `DONE` (2026-09-11). A real run was interrupted during HTTP crawl;
recovery-only reported zero pending/failed work, and the repeated run wrote,
uploaded, and cataloged zero duplicate files. Existing 2026-08/09 durable data
also proves SeenStore identity stability across resume runs.

2026-09-11 correction: real 2026-07 page 1 contains dated news rows whose
Premium titles have no article links. The production HTTP smoke now fails
safely before detail fetch with `free_access_boundary`; this is accepted free
coverage, not a Premium blocker. Generic Ctrl-C worker cleanup was also fixed.
Focused: `147 passed`; full unit: `742 passed`.

# 12. Phase L — Financial Reports / Filings Integration

Phase K remains unchanged: K13 is `BLOCKED_BY_REMOTE_WAF` and K14 is
`BLOCKED_BY_K13`. Phase L is independent and may proceed without resetting or
modifying Kabutan durable state.

## L0 — Repository audit + legacy traceability

Status: `DONE` (2026-09-11). The real `get_all_finance_report.py` was mapped
function-by-function to current generic dataset/plugin/HTTP/attachment/runtime/
recovery/Catalog/query APIs. Only HKEX source knowledge will migrate. Legacy
CSV metadata/failure logs, local PDF paths/downloader, URL cache, and progress
JSON will not become production infrastructure. Full baseline: `747 passed`;
focused architecture tests: `47 passed`.

## L1 — Generic financial_report dataset/schema

Status: `DONE` (2026-09-11). Added `financial_report` and
`financial_report_instrument` DatasetSpecs and default canonical Arrow schemas;
the existing `attachment` remains the reusable physical PDF contract. Query
relation handling recognizes logical financial reports. No HKEX adapter or
real-site claim is included. Focused: `67 passed`; full unit: `753 passed`.

## L2 — HKEX site package + builtin registration

Status: `DONE` (2026-09-11). Added a disabled HTTP site manifest, explicit
plugin skeleton, and idempotent builtin registration for `hkexnews` with
HK/Asia_Hong_Kong and the logical report datasets. Generic attachment remains
framework-owned. No real source behavior is claimed. Focused: `30 passed`;
full unit: `757 passed`.

## L3 — HKEX stockId exact-resolution client

Status: `DONE` (2026-09-11). Implemented strict five-digit normalization,
canonical `XHKG:` IDs, JSONP validation, exact suggestion filtering, and the
legacy prefix request through an injected generic transport. Prefix collisions
are regression-tested. Focused: `28 passed`; full unit: `770 passed`; real
endpoint acceptance is deferred to L12.

## L4 — HKEX report-search client

Status: `DONE` (2026-09-11). Extended generic `HttpRequest`/requesters with
optional POST form data and implemented the verified HKEX title-search form,
three native report codes, date validation, and unique category aggregation.
Focused: `34 passed`; full unit: `778 passed`; no real HTTP claim yet.

## L5 — HKEX result parser + report taxonomy

Status: `DONE` (2026-09-11). Added row-scoped PDF parsing, canonical HKEX URL
validation/dedup, full final-cell title extraction, source metadata, and the
annual/interim/quarterly taxonomy. Cross-category duplicate URLs collapse in
stable request order. Focused: `39 passed`; full unit: `783 passed`; real DOM
acceptance is deferred to L12.

## L6 — Release-time normalization

Status: `DONE` (2026-09-11). Added conservative Asia/Hong_Kong parsing for
verified source forms and UTC normalization. Missing or date-only timestamps
remain null, preserving raw source text for payload. Focused: `41 passed`;
full unit: `793 passed`.

## L7 — Canonical financial_report normalization

Status: `DONE` (2026-09-11). Added stable canonical-URL SHA-256 report
identity, global identity scope, XHKG instrument enforcement, complete minimum
payload with conservative null fields, primary relations, and immutable
relation records. Focused: `74 passed`; full unit: `798 passed`.

## L8 — financial_report -> attachment integration

Status: `DONE` (2026-09-11). Added parent-linked generic attachment requests,
generic pipeline consumption, and canonical attachment metadata. Strengthened
generic PDF magic/minimum-size validation and atomic local finalization without
adding HKEX storage/upload code. Focused: `89 passed`; full unit: `801 passed`;
real durable PDF acceptance remains L13.

## L9 — Checkpoint/recovery semantics

Status: `DONE` (2026-09-11). Added request-signature-bound instrument scope
state, complete-only candidates, safe full resume, changed-input re-evaluation,
and attachment policy state. Errors leave checkpoints unchanged and generic
Runtime/RecoveryStore remain authoritative. Focused: `98 passed`; full unit:
`805 passed`; real interruption acceptance remains L15.

## L10 — CLI/options/profiles

Status: `DONE` (2026-09-11). Added `hkex_reports_incremental/full`, report
type/date/lookback/PDF controls, canonical scope discovery, generic HTTP and
concurrent-runtime composition, and a 2,798-ID stable XHKG snapshot from the
supplied CSV with source SHA metadata. Focused: `156 passed`; full unit:
`814 passed`; no real HTTP acceptance is claimed.

## L11 — Deterministic parser/unit matrix

Status: `DONE` (2026-09-11). Annual/interim/quarterly taxonomy, exact prefix
matching, release-time nullability, canonical identity, PDF safety, attachment
linkage, profile parsing, and production composition all pass deterministic
coverage. Focused: `200 passed`; full unit: `820 passed`.

## L12 — One-instrument real HTTP metadata smoke

Status: `DONE` (2026-09-11). Real `XHKG:00005` exact lookup returned
`stockId=5`; annual search returned three reports since 2024; a real row
normalized with correct title/code/name cleanup, stable UID, and aware UTC
event time. No PDF or durable state was touched. Focused: `58 passed`; full
unit: `821 passed`.

## L13 — One-instrument durable metadata + PDF smoke

Status: `DONE` (2026-09-11). Real March-2026 annual data for `XHKG:00005`
completed metadata, relation, and attachment Parquet/NAS/Catalog/index paths,
all checkpoints, zero recovery pending/errors, and Query Layer readback. The
verified PDF is 12,588,090 bytes with matching NAS size and parent UID/SHA.
Focused: `144 passed`; full unit: `821 passed`.

## L14 — 10-instrument mixed report-type smoke

Status: `DONE` (run 2026-09-11, audited 2026-09-15). Snapshot offset 2359,
limit 10 completed 425 metadata and 425 relation rows across 20/20 scopes with
zero duplicates/errors/recovery pending. Query proved annual/interim/quarterly
counts `123/119/183`. Focused: `140 passed`; full unit: `821 passed`.

Observed performance gap: 425 report rows became 425 daily Parquet files;
remote materialization took 696.34s versus 1.12s scan. Existing files remain
unchanged; future packaging must be coarsened before L16.

## L15 — Interruption/resume/dedup smoke

Status: `DONE` (2026-09-15). Added reusable DatasetSpec partition policy fields
and configured financial-report metadata/relations for yearly time partitions
with one stable bucket; other datasets preserve their prior defaults. No
existing NAS data was changed. For snapshot offset 2369/limit 3, a real run was
interrupted after 3/3 metadata scopes crossed the durable barrier and before
relation completion. Recovery-only reported zero pending/failed work. The exact
resume skipped metadata, completed 120 relations in one file, and left all six
checkpoints complete. A second exact rerun crawled zero records and wrote,
uploaded, and cataloged zero files. The metadata stage coalesced 120 reports
into 13 yearly files. Full unit before acceptance: `823 passed`; focused/final
focused: `183 passed`.

## L16 — 100-instrument rollout

Status: `DONE` (2026-09-15). The first 100 snapshot instruments
(`XHKG:00001..XHKG:00116`) completed 100 metadata and 100 relation scopes using
the production concurrent CLI, fixed 1999-04-01..2026-09-15 range, metadata
only, and append-only durable storage. The source returned 3,663 records per
dataset. Existing state deduplicated 479 metadata rows and one relation; this
run wrote 3,184 metadata rows in 20 yearly Parquet files and 3,662 relations in
one file. Uploads/catalog jobs were 21/21, pipeline errors and recovery pending
were zero, and every new NAS file passed path, size, SHA256, upload-state, and
lifecycle checks.

The rollout exposed and fixed two source correctness cases (`stock_not_found`
durable empty scopes and explicit dual-counter fields) plus a generic
writer/query partition-policy mismatch. Query now reads current and legacy
bucket counts from DatasetSpec and handles yearly date partitions. Real query
smokes returned new `XHKG:00016` metadata/relations and legacy `XHKG:08003`
relations. Focused: `110 passed`; full unit: `831 passed`.

## L17 — Full configured HK universe rollout

Status: `DONE` (2026-09-16). The exact full command initially reached five
500-scope coalesced metadata batches before `XHKG:80016` exposed that HKEX may
return a primary-counter code (`00016`) for an exactly resolved secondary
counter. The adapter now accepts only exact-resolver-backed aliases and keeps
unproven mismatches strict, without changing persisted payload/version
semantics. Focused: `114 passed`; full unit: `835 passed`.

Local checkpoint audit finds 2,505 matching metadata scopes and 100 matching
relation scopes. Completion is intentionally out of order; the first missing
metadata checkpoint is snapshot index 2498 (`XHKG:08250`), so resume must use
the exact command and per-scope checkpoints rather than an offset. All 612
HKEX recovery manifests are terminal `deleted`, with zero local nonterminal
work. After PostgreSQL returned, recovery-only was clean and the exact command
resumed without resetting durable state. Final checkpoints are 2,798 metadata
plus 2,798 relation scopes. The resume crawled 53,657 relation rows, retained
53,079 new rows, and wrote/uploaded/cataloged five coalesced files. All five
remote sizes and SHA256 values match Catalog; failures, missing scopes, and
pending recovery are zero.

The resume also exposed and fixed a generic event-loop blocker: async
`HttpTransport` had called the limiter's synchronous sleep. The transport now
awaits the computed adaptive delay and retains compatibility for synchronous
and fake limiters.

## L18 — Profiles/query/completeness final acceptance

Status: `DONE` (2026-09-16). Completeness recognizes both financial-report
datasets as instrument-scoped and reports 5,596 expected/completed scopes with
zero problems. Real production CLI runs accepted `hkex_reports_incremental`
and `hkex_reports_full`; the final full snapshot rerun skipped all durable
scopes and wrote no files. Query Layer returned real report metadata and
instrument relations from PostgreSQL Catalog/NAS. Focused tests: `232 passed`;
full unit suite: `837 passed`.

Phase L status: `COMPLETE`.

# 13. Phase M — Production Orchestration, Resume & Progress UX

## M0 — Current-state audit

Status: `DONE` (2026-09-16)

No production crawl, recovery mutation, NAS write, Catalog write, or state
cleanup was performed. Baseline: `837 passed`.
Focused architecture/recovery regression: `178 passed`; full unit suite:
`837 passed`.

### Production call graph

```text
cli.main
-> parse single-site profile/options
-> AppFactory.create_bootstrap
-> CrawlBootstrap.run
-> RecoveryOrchestrator.run
-> ConcurrentProductionRuntime.run
-> plugin.discover
-> crawl workers
-> bounded record queue
-> writer workers / SeenStore inspect / RecordBuffer / Parquet
-> bounded upload queue
-> upload workers / upload + verify
-> bounded catalog queue
-> Catalog/index -> SeenStore commit
-> scope checkpoint after all associated batches are durable
```

### Component audit

| Component | Current fact | Phase M gap |
| --- | --- | --- |
| ConcurrentProductionRuntime | Production-wired bounded record/upload/catalog queues and worker metrics | No pre-crawl scope plan, multi-site orchestration, or graceful two-stage interrupt |
| StoragePipeline | Preserves write -> upload -> verify -> Catalog -> Seen -> checkpoint order | Must expose resumable stage intent without changing the barrier |
| SeenStore | New/unchanged/updated inspection; unchanged records create no batch | Cannot alone prove a whole scope is complete |
| CheckpointStore | Atomic per site/dataset/scope checkpoint persistence | Completion keys/states are plugin-specific; no generic classifier yet |
| RecoveryStore/Manager | Crash-safe manifests and stage-based recovery before crawl | No generic scope association/ResumePlan; uploaded stage currently re-enters uploader for verification |
| Uploader | Generic verified/idempotent upload semantics | Need evidence-based skip for already durable uploads, never filename guessing |
| PostgreSQL Catalog | `file_path` uniqueness and idempotent UPSERT preserve physical state | Platform resume reporting not present |
| Run manifest | Single-site normal-completion JSON with runtime/recovery stats | Exceptions/interrupts can return before manifest write; no platform manifest |
| ProgressSnapshot/reporter | Periodic stderr text with scope, queue, file, busy counters | No resume seed, known total, site aggregation, full metrics, ETA, Rich/TTY policy |
| AppFactory/CrawlBootstrap | Correctly chooses concurrent runtime and runs recovery first | No build-plan step between recovery and crawl; no shared global resources |
| CLI | Nine production single-site profiles and `--recovery-only` | Requires `--site`; no `run-platform`, global profiles, or progress mode flags |

### Profile resume truth

```text
naver_full / naver_incremental
  page/history checkpoints and Seen replay exist; no generic whole-scope
  pre-crawl classifier.

toss_full / toss_incremental / toss_historical_backfill
  historical baseline and pending-link state exist; no generic durable-complete
  scope skip before plugin.crawl.

kabutan_free_full / kabutan_incremental
  month completion/free-boundary states exist; free_full skips accepted old
  months inside plugin.crawl. WAF remains an explicit remote BLOCKED state.

hkex_reports_full / hkex_reports_incremental
  matching full request signatures skip inside plugin.crawl; runtime still
  discovers and invokes the plugin before that decision.
```

### M0 decision

The accepted durable architecture is retained. M1 added a read-only generic
ResumePlanner over existing checkpoint/recovery/site evidence; later items
wire its decisions into startup, progress, and platform orchestration.
No Phase M production-ready claim is made by M0.

## M1 — Generic ResumePlanner / ResumePlan

Status: `DONE` (2026-09-16)

Added `core/resume.py` with immutable scope/evidence/plan items and the six
required classifications. Classification precedence is deterministic:

```text
BLOCKED
-> FAILED_TERMINAL
-> FAILED_RETRYABLE
-> RECOVERY_PENDING
-> DURABLE_COMPLETE
-> INCOMPLETE
```

The planner is read-only and conservative. Opaque plugin checkpoints are never
interpreted by core code; a provider supplies the site-specific completion
predicate plus optional recovery/block evidence. It only invokes checkpoint
`exists()` and `load()`, rejects duplicate scope tokens, preserves discovery
order, exposes crawlable scopes, and serializes stable aggregate counts.

M1 does not alter production startup. Recovery-first plan rebuilding and
runtime filtering remain M2. Coalesced recovery manifests currently lack
scope-token membership and remain an explicit M3 integration constraint.

Validation:

```text
tests/unit/test_resume_planner.py                         10 passed
resume/checkpoint/recovery/concurrent focused suite      71 passed
pytest -q -p no:rerunfailures tests/unit                847 passed
```

## M2 — Recovery-first startup integration

Status: `DONE` (2026-09-16)

AppFactory injects the generic ResumePlanner into every production
ConcurrentProductionRuntime. CrawlBootstrap still completes startup recovery
before starting managed resources/runtime; dataset discovery then builds its
plan from post-recovery checkpoints. Plan items with proven
`DURABLE_COMPLETE` evidence are recorded as zero-work completed results and
never reach `plugin.crawl()`. Crawlable items reuse the checkpoint loaded by
planning instead of reading it a second time.

Site checkpoint ownership remains intact:

```text
HKEX full:
  scope_complete + matching request_signature -> durable complete

Kabutan free_full:
  accepted complete/free-boundary checkpoint for a non-current month
  -> durable complete

Naver/Toss:
  no generic whole-scope assertion yet -> conservative existing crawl/resume
```

The runtime result now includes a serializable per-dataset `resume_plans`
section for later manifest/progress aggregation. Deterministic integration
proved that recovery can create six checkpoints during the same startup and
the following plan skips all six with `scopes_started=0`.

M2 does not infer scope ownership from recovery filenames or batches. Exact
pending/uploaded/cataloged stage classification remains M3.

Validation:

```text
focused M1/M2/runtime/bootstrap/AppFactory/site tests   100 passed
pytest -q -p no:rerunfailures tests/unit                851 passed
```

## M3 — Resumable durable-stage classification

Status: `DONE` (2026-09-16)

New RecoveryManifest files persist the exact sorted scope-token set carried by
their FlushBatch. RecoveryStore can query only explicit scope membership, and
the production ResumePlanner maps those manifests to pending, retryable, or
terminal scope evidence after startup recovery.

Minimum next actions are explicit:

```text
local                 UPLOAD_AND_VERIFY
uploaded              VERIFY_REMOTE
verified              REGISTER_CATALOG
catalog_registered    COMMIT_SEEN
seen_committed         ADVANCE_CHECKPOINT_MARKER
checkpoint_committed  CLEAN_LOCAL
cleanable             CLEAN_LOCAL
deleted               NONE
failed                RETRY or STOP_TERMINAL
```

Both production uploaders now support `verify_existing()`. Recovery of an
`uploaded` manifest requires its recorded remote path and configured size or
SHA evidence, performs no transfer, and then resumes from `verified`.
Verified manifests do not upload; cataloged manifests do not register again;
Seen/checkpoint/cleanup stages continue forward only.

Backward compatibility is conservative: old manifests deserialize with an
empty scope-token tuple and remain handled by global startup recovery, but are
never assigned to a scope from filenames or directory names.

Validation:

```text
focused resume/recovery/pipeline/uploader/runtime suite  149 passed
pytest -q -p no:rerunfailures tests/unit                 864 passed
```

## M4 — ProgressAggregator

Status: `DONE` (2026-09-16)

Added `observability/progress.py` as a thread-safe, renderer-independent
aggregation layer. AppFactory gives each production concurrent runtime an
aggregator; CrawlBootstrap supplies startup recovery totals; runtime supplies
ResumePlan seeds and live scope, record, storage, queue, compaction, and
stage-busy-time events.

Snapshots provide:

```text
platform/profile elapsed, completed/total, percent, remaining, ETA
site status and aggregate scopes
dataset complete/running/skipped/blocked/failed/recovery-pending scopes
records crawled/normalized/new/unchanged/updated/failed
storage buffered/prepared/written/uploaded/verified/cataloged/compacted
queue current and maximum observed depth
startup recovery counters
records/sec, files/min, upload MB/sec, stage busy times
```

ResumePlan durable scopes initialize completion and
`skipped_due_to_checkpoint`; they are not added as session work. A
deterministic 5-of-10 restart begins at 50%, completion cannot exceed 100%,
queue final current depths return to zero while peaks remain, and ETA is hidden
until the current session has a measurable completion rate. Snapshot output is
JSON serializable and included in runtime results.

M4 does not render terminal output. TTY/Rich/text/JSON mode selection remains
M5.

Validation:

```text
focused aggregator/runtime/bootstrap/AppFactory tests    69 passed
pytest -q -p no:rerunfailures tests/unit                872 passed
```

## M5 — Rich/Text progress dashboard

Status: `DONE` (2026-09-16)

Added `observability/renderers.py` with dependency-free periodic text output
and optional Rich Live rendering. CLI controls are:

```text
--progress
--no-progress
--progress-style auto|rich|text
--progress-interval-seconds N
```

Default `auto` chooses Rich on a TTY when installed and text for non-TTY/log
streams. Explicit Rich safely falls back to text when unavailable. JSON output
and `--no-progress` instantiate no renderer, preserving clean machine stdout.
The existing raw ProgressSnapshot formatter remains available as a compatibility
API.

Both renderers consume only M4 snapshots and display resume-seeded scopes,
site/dataset status, records, storage stages, queue current/peak depth,
recovery, throughput, and busy times. Runtime emits a final aggregate snapshot
and closes renderer lifecycle from an outer finally block. A real Rich Live
smoke passed in the current environment; its compact table preserves
site/dataset identity at an 80-column console width.

Validation:

```text
focused renderer/aggregator/CLI/AppFactory/runtime tests  130 passed
pytest -q -p no:rerunfailures tests/unit                 881 passed
```

Phase M roadmap status:

```text
M0  audit                                  DONE
M1  ResumePlanner                          DONE
M2  recovery-first startup integration     DONE
M3  resumable stage classification         DONE
M4  ProgressAggregator                     DONE
M5  Rich/Text dashboard                    DONE
M6  Ctrl-C graceful resume                 DONE
M7  global profiles/orchestrator            DONE
M8  global resource budgets                DONE
M9  global run manifest                    DONE
M10 deterministic resume/progress matrix   DONE
M11 interrupted small global smoke         DONE
M12 exact rerun dedup acceptance            TODO (NEXT)
M13 global_incremental acceptance           TODO
```

# 14. Current Next Action

Execute M12 by rerunning the same M11 profile, universe prefixes, and resource
settings. Prove existing durable prefixes are skipped/recovered and no
duplicate Parquet, upload, Catalog, or logical records are created. Kabutan Phase K
remains independently blocked by remote WAF; no automatic K13 retry should run
until its access probe is available.

```text
Preserve all accepted site checkpoints, SeenStore, RecoveryStore, Catalog, and
NAS objects. M12 must not clear or restart any accepted durable prefix.
```

## M6 — Ctrl-C graceful resume

Status: `DONE` (2026-09-16)

The CLI and concurrent runtime share a generic ShutdownController. First
SIGINT/SIGTERM stops discovery and new scope intake while accepted records
drain through writer, upload/verify, Catalog/index, SeenStore, and checkpoint.
An interrupted scope deliberately skips `checkpoint_after_scope`; only its
already durable record-level cursor may be saved. A second signal cancels and
awaits every worker, leaves recovery/checkpoint/storage state intact, closes
the progress renderer, and returns an interrupted result. Bootstrap reports
the run as resumable rather than successful, configured run manifests include
the interrupt state, and CLI exits `130`.

Validation:

```text
focused shutdown/runtime/bootstrap/CLI/AppFactory tests  144 passed
pytest -q -p no:rerunfailures tests/unit                 888 passed
```

## M7 — Global profiles/orchestrator

Status: `DONE` (2026-09-17)

Added `run-platform` and deterministic mappings from `global_full` and
`global_incremental` to the four accepted site profiles. The in-process
orchestrator reuses the real AppFactory, startup recovery, ResumePlanner,
ConcurrentProductionRuntime, and durable storage pipeline for each site. It
isolates failures, shares graceful shutdown state, supports recovery-only, and
returns aggregate text or JSON status. Kabutan is probed once before crawl;
verified WAF blocking records `BLOCKED` and does not prevent HKEX from running.

Named profiles now always select ConcurrentProductionRuntime, including a
conservative one-worker-per-stage configuration. M7 site execution is
sequential; M8 owns parallel site workers and globally shared storage-stage
budgets.

Validation:

```text
focused platform/CLI/AppFactory/runtime/shutdown tests  148 passed
pytest -q -p no:rerunfailures tests/unit                899 passed
```

## M8 — Global resource budgets

Status: `DONE` (2026-09-17)

PlatformOrchestrator now schedules sites concurrently with a bounded
`--site-workers` semaphore. All real production runtimes receive one shared
GlobalStageBudget for Parquet preparation, upload/verification, and
Catalog/index/Seen/checkpoint. The same budget covers startup recovery,
generic attachments, final flush, and post-dataset compaction, so auxiliary
paths cannot exceed the NAS/Catalog limits. Site crawl/browser/http limits are
still independently enforced by existing transports and plugins.

CLI defaults:

```text
site_workers=2
global_writer_workers=2
global_upload_workers=2   (accepted range 1..4)
global_catalog_workers=2
```

Budget results expose `limit`, `active`, `max_active`, and `waits` per stage.

Validation:

```text
focused budget/platform/runtime/attachment/CLI tests  173 passed
pytest -q -p no:rerunfailures tests/unit              907 passed
```

## M9 — Global run manifest

Status: `DONE` (2026-09-17)

`run-platform` now always writes an aggregate manifest for returned normal,
WAF-blocked, failed, and controlled-interrupt outcomes. It records the exact
site options used by the orchestrator, universe path/hash/count metadata,
ResumePlans, completed/skipped/incomplete scopes, records, durable files,
uploads, Catalog jobs, recovery, shared budget observations, blocked sites,
errors, and final state. Default files use
`state/run_manifests/platform_<profile>_<timestamp>.json`; an explicit
`--run-manifest-path` is supported. Both platform and existing single-site
manifest writers use same-directory temporary files with atomic replacement.

Validation:

```text
focused Phase M manifest/orchestrator/runtime/CLI tests  193 passed
pytest -q -p no:rerunfailures tests/unit                 914 passed
```

## M10 — Deterministic resume/progress matrix

Status: `DONE` (2026-09-17)

The focused acceptance set now covers cold, 50%-partial, and 100%-complete
starts; every ResumeStatus and persisted recovery-stage action; complete-scope
pre-crawl skip; startup recovery; unchanged SeenStore replay; blocked and
failed scopes; zero-row scope completion; attachment and compaction progress;
TTY Rich, non-TTY text, JSON-disabled rendering; and first/second interrupt
behavior. The concentrated runtime matrix uses the production ResumePlanner,
FileCheckpointStore, ConcurrentProductionRuntime, ProgressAggregator, bounded
durable stages, and ShutdownController rather than a result-only fixture.

The 100%-complete rerun proves zero crawler calls, record submissions, Parquet
writes, uploads, and Catalog jobs. Zero-row scopes still commit their valid
site-owned completion checkpoint, while interrupted scopes never receive that
completion marker.

Validation:

```text
focused deterministic resume/progress/recovery matrix  249 passed
pytest -q -p no:rerunfailures tests/unit                923 passed
```

## M11 — Interrupted small global smoke

Status: `DONE` (2026-09-17)

Ran the real `global_incremental` CLI with deterministic two-instrument
prefixes for Naver, TossInvest, and HKEX, four bounded site workers, shared
`2/2/2` durable-stage budgets, coalesced scope flushes, and the existing
Kabutan access probe. One Ctrl-C stopped new work and drained the pipeline.

The first attempt found and fixed a generic shutdown integration bug:
Playwright driver connection closure after SIGINT was classified as a Toss
failure. Discovery/crawl transport teardown is now interruption-only when the
shared ShutdownController is already stopping. Pre-shutdown transport errors
and writer/upload/Catalog failures still fail normally.

Final retry evidence:

```text
platform                  INTERRUPTED
naver/toss/hkex           INTERRUPTED
kabutan                   BLOCKED (WAF)
failed sites              0
pending recovery          0
pipeline errors           0
new files/uploads/catalog 0/0/0
PostgreSQL rows since M11 0
```

Both manifests and all checkpoint/SeenStore/RecoveryStore/Catalog/NAS state
were preserved. Focused shutdown/platform/runtime tests: `113 passed`; full
unit suite: `926 passed`.

Codex startup sequence:

```text
1. git status --short
2. run full current unit baseline
3. read the Phase M section in todo.md and project_status.md
4. inspect runtime/checkpoint/recovery/progress/profile wiring
5. preserve all accepted site semantics and production state
6. reconcile project_status.md / finished.md / problem.md / todo.md
7. execute the first incomplete Phase M item
8. continue in M0-M13 order
```

Normal development tasks do not require user confirmation.

Stop only for:

```text
destructive production-data operation
irreversible database migration
real NAS deletion/overwrite
missing credentials/infrastructure
major unresolved architecture conflict
```
