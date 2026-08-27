# Crawl Framework V2 — Codex Continuation Plan

> Purpose: This document is the authoritative handoff for continuing development of the universal financial web crawler / storage / query framework.
>
> Codex should follow this document in order, preserve all existing working behavior, and never replace proven semantics with speculative redesigns.
>
> Current validated baseline: **392 unit tests passed**.
>
> Envirnmnet: 
```bash
conda activate pac
```
---

## 0. Executive Summary

This project is a reusable, multi-site financial data crawling and storage framework.

The intended long-term workflow is:

```text
site-specific crawler/plugin
        ↓
normalize into CanonicalRecord
        ↓
SeenStore inspect
        ↓
buffer
        ↓
atomic local Parquet write
        ↓
sidecar / manifest
        ↓
upload
        ↓
remote verification
        ↓
PostgreSQL Catalog registration
        ↓
SeenStore commit
        ↓
checkpoint commit
        ↓
local cleanup
        ↓
query through Catalog + Parquet
```

Target data sources include:

- Naver Finance
- TossInvest
- HotCopper
- Stockhouse
- future country-specific finance/news/community/research sources

Target datasets include:

- `news_article`
- `news_instrument`
- `forum_post`
- `comment`
- `research_report`
- `research_instrument`
- `author_profile`
- `author_post`
- `holding_snapshot`
- `holding_position`
- `instrument`
- `instrument_relation`
- `attachment`

The framework is designed for very large datasets, potentially billions of comments.

Core storage strategy:

- PostgreSQL for Catalog / metadata / control state
- Parquet for large durable analytical data
- remote NAS / server for long-term storage
- small local disk footprint
- stream/query by partition + bucket
- no SQLite as main database
- SQLite is acceptable only for SeenStore/cache-like local state

---

# 1. Current Project Structure

```text
crawl_framework/
  config/
    config.yaml

  src/crawl_framework/
    config.py

    core/
      models.py
      dataset.py
      plugin.py
      registry.py
      pipeline.py
      runtime.py
      barrier.py
      bootstrap.py

    storage/
      buffer.py
      seen_store.py
      checkpoint.py
      partition.py
      parquet_writer.py
      postgres.py
      postgres_connection.py
      uploader.py
      cleaner.py
      recovery.py
      record_index.py
      retry_policy.py
      recovery_orchestrator.py
      query.py

    transports/
      http.py
      playwright.py
      proxy.py
      rate_limit.py

    sites/
      builtin.py
      naver_finance/
        __init__.py
        plugin.py
        forum_post.py

    cli/
      __init__.py
      main.py

    app_factory.py

  scripts/
    repair_remote_catalog.py
    migrate_seen_forum_post_hashes.py
    compact_naver_forum_post_history.py
    migrate_seen_forum_post_local_history.py
    publish_naver_forum_post_compaction.py
    retire_naver_forum_post_history.py
    migrate_data_files_lifecycle_status.py
    query_data.py

  tests/
    unit/
      test_query.py
      test_query_multi_instrument.py
      test_postgres.py
      ...

  state/
  spool/
  warehouse/
  remote/
  tmp/
  logs/
```

---

# 2. Non-Negotiable Architecture Rules

These rules must not be violated without explicit redesign and new tests.

## 2.1 Canonical identity

`record_uid` is the logical record identity.

Current intended formula:

```text
stable_sha256(
    site_id,
    dataset,
    scope_type,
    scope_id,
    source_id
)
```

`version_hash` represents record content/version.

It excludes crawl timestamp and includes canonical business fields such as:

```text
site_id
dataset
source_id
scope_type
scope_id
instrument_id
event_time
updated_at
title
content
author_id
author_name
source_url
relations
payload
```

Never include unstable page-position or crawl-time-only values in identity hashes.

---

## 2.2 SeenStore semantics

SeenStore is not a crawler checkpoint.

- SeenStore = durable record/version history
- Checkpoint = crawler progress

Required behavior:

```text
inspect()
    new / unchanged / updated

commit()
    only after durable storage success
```

Never commit SeenStore merely because a record was crawled.

---

## 2.3 Durable commit barrier

Correct order:

```text
crawl
→ normalize
→ SeenStore inspect
→ buffer
→ local Parquet atomic write
→ sidecar
→ manifest
→ upload
→ verify
→ PostgreSQL Catalog
→ SeenStore commit
→ checkpoint
→ cleanup
```

Desired semantics:

```text
at-least-once crawling
+
effectively-once durable storage
```

Do not move checkpoint or SeenStore commits before durable storage verification.

---

## 2.4 Lifecycle vs physical storage state

`marketdata.data_files` separates:

### Physical storage state

```text
storage_status:
    local
    uploaded
```

### Logical lifecycle state

```text
lifecycle_status:
    active
    superseded
    archived
```

Normal queries must only use:

```sql
storage_status = 'uploaded'
AND lifecycle_status = 'active'
```

Never treat lifecycle and storage state as the same thing.

---

# 3. Current Proven Baseline

Current unit-test baseline:

```text
392 passed
```

Do not accept future changes that reduce this without a clearly justified test update.

The following production behavior has been validated.

## 3.1 Naver forum_post

Real instruments tested:

```text
XKRX:005930
XKRX:000660
XKRX:042700
```

Current known bucket mapping:

```text
XKRX:005930 -> 5f
XKRX:042700 -> 20
XKRX:000660 -> f1
```

Bucket function uses stable SHA-1 based partition logic from `storage/partition.py`.

---

## 3.2 Current compacted active history

Production compacted history was previously validated at 123 logical rows.

Lifecycle semantics were established:

```text
old files -> superseded
replacement compacted files -> active
```

Normal Catalog queries must never re-read superseded files.

---

## 3.3 Query Layer currently supports

- single instrument query
- multi-instrument query
- local-natural-day timezone semantics
- UTC partition pruning
- single-bucket pruning
- multi-bucket pruning
- date-range Catalog queries
- Arrow predicate pushdown
- streaming Arrow `RecordBatch`
- hard `max_rows` early termination
- JSON Lines streaming
- JSONL export
- Parquet streaming export
- normal small query returning full `pa.Table`
- stable sort in full-table query
- no guaranteed global sort in streaming mode

---

# 4. Query Semantics — Must Preserve

## 4.1 QuerySpec

Current query layer supports both:

```text
instrument_id
instrument_ids
```

Rules:

- single-instrument behavior must remain backward compatible
- multi-instrument input must be normalized and deduplicated
- requested instrument rows must still be filtered exactly at Arrow level
- bucket pruning is only a coarse file-level optimization

---

## 4.2 Timezone semantics

User-facing date inputs are local natural dates.

Example:

```text
timezone = Asia/Seoul
date = 2026-08-26
```

must map to:

```text
>= 2026-08-25 15:00:00 UTC
<  2026-08-26 15:00:00 UTC
```

Partition pruning must derive UTC physical partition dates from this interval.

Do not interpret local dates as UTC dates.

---

## 4.3 Full query vs streaming query

### `query()`

Use for small/interactive results.

Behavior:

```text
returns full pa.Table
may concatenate files
global stable sort:
    event_time ascending
    then record_uid ascending when available
```

### `iter_batches()`

Use for large result sets.

Behavior:

```text
returns RecordBatch stream
constant-bounded memory
no cross-file global sorting guarantee
supports max_rows early termination
```

Do not force global sorting into streaming mode unless implementing an explicit external-sort system.

---

## 4.4 `--limit` vs `--max-rows`

These semantics must remain distinct.

```text
--limit
    output/display limit only

--max-rows
    hard execution limit
    stops reading additional rows/files once satisfied
```

Example:

```text
--stream --limit 5
```

may scan all matching data but print only 5 rows.

Example:

```text
--stream --max-rows 1000 --limit 20
```

must stop execution after at most 1000 yielded rows and print at most 20.

---

# 5. Multi-Instrument / Multi-Bucket Status

Current multi-instrument production validation:

```text
3 instruments
113 rows
```

Verified instruments:

```text
XKRX:000660
XKRX:005930
XKRX:042700
```

The multi-bucket Catalog optimization has now been introduced.

Target SQL behavior:

```sql
bucket = ANY(%(buckets)s)
```

instead of:

```text
bucket 20 -> SQL #1
bucket 5f -> SQL #2
bucket f1 -> SQL #3
```

Desired:

```text
(20, 5f, f1) -> one SQL call
```

The dedicated multi-instrument test suite has been updated to reflect new semantics.

Current baseline after update:

```text
392 passed
```

---

# 6. Immediate Next Task — FIX POSTGRES UPSERT PHYSICAL-STATE REGRESSION

This is the highest-priority remaining correctness issue.

Current `build_insert_data_file_sql()` conflict update still contains behavior equivalent to:

```sql
storage_status = EXCLUDED.storage_status,
remote_path = EXCLUDED.remote_path
```

This is unsafe.

## 6.1 Failure scenario

Existing row:

```text
storage_status = uploaded
remote_path = /nas/.../file.parquet
```

A recovery / retry / duplicate registration reconstructs:

```text
DataFileRecord(
    storage_status="local",
    remote_path=None
)
```

Generic UPSERT can regress the Catalog row to:

```text
storage_status = local
remote_path = NULL
```

That is wrong.

---

## 6.2 Required design

Generic registration should update file metadata only.

Physical state transitions must be explicit.

Correct conceptual ownership:

```text
register_data_file()
    metadata registration

mark_uploaded()
    local -> uploaded
    set remote_path
```

Conflict UPSERT must preserve existing:

```text
storage_status
remote_path
lifecycle_status
```

unless a dedicated explicit state-transition method changes them.

---

## 6.3 Required implementation

Modify `build_insert_data_file_sql()`.

On conflict, update metadata such as:

```text
sha256
row_count
file_size
min_event_time
max_event_time
schema_version
updated_at
```

Do NOT overwrite:

```text
storage_status
remote_path
lifecycle_status
```

The INSERT path may still accept initial values.

---

## 6.4 Required regression tests

Add tests covering at minimum:

### A. SQL safety

Generated conflict-update SQL must not contain:

```text
storage_status = EXCLUDED.storage_status
remote_path = EXCLUDED.remote_path
lifecycle_status = EXCLUDED.lifecycle_status
```

### B. uploaded state preservation

Conceptually verify:

```text
existing uploaded row
+
duplicate generic registration
=
still uploaded
```

### C. remote_path preservation

Existing remote path must not be erased by a default `None`.

### D. explicit mark_uploaded still works

`mark_uploaded()` remains the official physical-state transition method.

### E. lifecycle remains protected

Superseded/archived must not be reactivated by generic registration.

Acceptance:

```text
all existing tests pass
+
new regression tests pass
```

---

# 7. Next Task — QUERY OBSERVABILITY / STREAMING STATS

Current full-query stats:

```text
catalog_files
parquet_files_read
rows_read
rows_returned
```

Streaming CLI currently mainly exposes:

```text
total_batches
rows_returned
```

For large-scale use, add observability.

---

## 7.1 Desired metrics

At minimum:

```text
catalog_sql_calls
catalog_files
parquet_files_materialized
parquet_files_read
physical_rows_in_candidate_files
rows_yielded
bytes_materialized
batches_yielded
```

Optional but recommended:

```text
query_duration_seconds
catalog_duration_seconds
materialization_duration_seconds
scan_duration_seconds
```

---

## 7.2 Important semantics

Do not incorrectly label candidate-file physical row counts as exact Arrow-scanned rows.

If the Catalog provides:

```text
item.row_count
```

that represents physical rows in candidate files.

Prefer naming like:

```text
candidate_physical_rows
```

rather than implying precise rows actually read from storage.

---

## 7.3 Design suggestion

Avoid breaking current `iter_batches()` generator API.

Possible designs:

### Option A

Add a mutable stats collector:

```python
StreamingQueryStats
```

and pass it into `iter_batches()`.

### Option B

Add:

```python
iter_batches_with_stats()
```

that exposes both batches and a final stats object.

### Option C

Add a streaming session object.

Choose the simplest design that:

- preserves existing API compatibility
- does not accumulate result data in memory
- gives reliable metrics

---

# 8. Next Task — QUERY LARGE UNIVERSE INPUT

Current CLI supports:

```text
--instrument
--instruments ...
```

Add support for large universes.

Recommended:

```text
--instruments-file path/to/universe.txt
```

Format:

```text
XKRX:005930
XKRX:000660
XKRX:042700
...
```

Requirements:

- UTF-8
- blank lines ignored
- whitespace stripped
- duplicates removed
- preserve stable input order
- merge behavior with `--instrument` / `--instruments` must be explicit
- reject completely empty effective instrument set when the user supplied a file expecting instruments

Do not require passing thousands of instruments as shell arguments.

---

# 9. Next Task — UNIVERSAL COMPACTION

Current compaction work was primarily Naver/forum-post specific.

This must become a generic storage subsystem.

---

## 9.1 Goal

Convert many small active Parquet files into fewer larger files while preserving logical records and lifecycle history.

Desired workflow:

```text
select active files
        ↓
read records
        ↓
deduplicate by logical identity/version policy
        ↓
write compacted replacement files
        ↓
verify replacement
        ↓
register new files
        ↓
mark new files uploaded/active
        ↓
mark old files superseded
```

Never delete originals before replacement verification.

---

## 9.2 Generic selection dimensions

Compaction should support grouping by:

```text
site_id
country
dataset
partition_date
bucket
```

Potential policy inputs:

```text
minimum file count
minimum/maximum target size
maximum rows per compacted file
age threshold
```

---

## 9.3 Safety

Compaction must be idempotent.

Must not:

- reactivate superseded files
- double-count old + new files
- mark source files superseded before replacement is durable
- erase audit history

---

## 9.4 Generic CLI / script

Create a generic command, e.g.:

```text
scripts/compact_data.py
```

or proper CLI command.

Support dry-run.

Example conceptual usage:

```text
--site naver_finance
--dataset forum_post
--date 2026-08-26
--bucket 20
--dry-run
```

---

# 10. Next Task — STORAGE AUDIT / REPAIR

For billions of records, maintenance tools are mandatory.

Create a general audit system.

---

## 10.1 Detect

At minimum:

### Catalog problems

```text
active + uploaded + missing remote_path
active + uploaded + remote file missing
file_size mismatch
checksum mismatch
duplicate conflicting file metadata
```

### Storage problems

```text
remote file exists but no Catalog row
orphan sidecars/manifests
stale local spool files
```

### Lifecycle problems

```text
superseded file incorrectly considered active
replacement missing while sources superseded
archived file accidentally queried
```

---

## 10.2 Audit modes

Implement:

```text
dry-run / report
repair-safe
```

Dangerous destructive actions should never be automatic without explicit option.

---

## 10.3 Output

Prefer structured output:

```text
JSON / JSONL
```

with summary:

```text
files_checked
catalog_errors
missing_remote
size_mismatch
checksum_mismatch
orphans
repairs_applied
```

---

# 11. Next Task — UNIVERSAL RETENTION / CLEANUP POLICY

Local disk is small.

The framework already has cleanup concepts, but production-scale policy should be explicit.

Required rules:

```text
never delete local file before:
    upload success
    verification success
    Catalog success
    SeenStore commit
    checkpoint commit
```

Potential retention controls:

```text
keep local failures
keep manifests for N days
keep superseded remote files for rollback period
archive before permanent deletion
```

Do not add aggressive deletion until audit and recovery tools are reliable.

---

# 12. Next Task — RECOVERY HARDENING

Recovery should be able to resume after failure at any durable boundary.

Test failure after:

```text
Parquet write
upload
verify
Catalog register
SeenStore commit
checkpoint
```

Expected behavior:

- no logical data loss
- no duplicate logical active records
- no premature checkpoint advance
- re-run is safe
- explicit state transitions remain monotonic

Add fault-injection tests if practical.

---

# 13. Next Task — PERFORMANCE / SCALE VALIDATION

Before full-market crawling, test with synthetic and/or production-like large data.

---

## 13.1 Query scale tests

Test:

```text
100 instruments
1000 instruments
large date ranges
large multi-bucket sets
```

Measure:

```text
Catalog SQL count
candidate files
remote bytes
memory
execution time
```

---

## 13.2 Streaming memory test

Verify memory does not grow linearly with result size.

Especially test:

```text
iter_batches()
JSONL export
Parquet export
```

Never call:

```python
to_pylist()
```

on entire large query results.

Per-batch conversion is acceptable when needed.

---

## 13.3 File-size strategy

Determine practical target Parquet file sizes.

Avoid:

```text
millions of tiny files
```

Also avoid giant files that are expensive to fetch over SCP for narrow queries.

Benchmark before hard-coding policy.

---

# 14. Next Task — SECOND SITE INTEGRATION

This is the final architectural proof.

Choose a structurally different source from Naver.

Recommended candidates:

- TossInvest
- HotCopper
- Stockhouse

The goal is NOT merely “crawl another site”.

The goal is to prove:

```text
core/
storage/
query/
SeenStore/
checkpoint/
Catalog/
```

require little or no change.

Ideally only:

```text
sites/<new_site>/
```

plus registry/config additions.

---

## 14.1 Required second-site proof

Implement at least:

```text
discover()
crawl()
normalize()
checkpoint progression
dedup
Parquet storage
Catalog
query
```

Run production smoke test.

---

# 15. Next Task — FULL MARKET / LONG-RUN VALIDATION

After the framework core is stable:

Run controlled full-market crawling.

Suggested progression:

```text
3 instruments
→ 50 instruments
→ 500 instruments
→ full market
```

Observe:

```text
crawler error rate
retry rate
duplicate rate
records/sec
files/day
storage growth
PostgreSQL growth
NAS bandwidth
checkpoint behavior
recovery behavior
```

Do not jump directly to uncontrolled full-market crawling.

---

# 16. PostgreSQL Catalog Design Rules

Current important table:

```text
marketdata.data_files
```

Core fields:

```text
id
site_id
country
dataset
partition_date
bucket
file_path
sha256
row_count
file_size
min_event_time
max_event_time
schema_version
storage_status
lifecycle_status
remote_path
created_at
updated_at
```

Normal query filter:

```sql
storage_status = 'uploaded'
AND lifecycle_status = 'active'
```

Useful lookup ordering:

```text
partition_date
bucket
id
```

Current index includes the important lookup dimensions.

---

# 17. Multi-Bucket Catalog API

The current intended APIs include:

```python
list_active_data_files(...)
list_active_data_files_range(...)
list_active_data_files_multi_bucket(...)
list_active_data_files_range_multi_bucket(...)
get_data_file(...)
```

Behavior:

### Single bucket

Use old APIs for compatibility.

### Multiple buckets

Use one SQL query.

PostgreSQL condition:

```sql
bucket = ANY(%(buckets)s)
```

Do not regress back to one SQL per instrument.

---

# 18. Parquet Schema

Current canonical Parquet schema is effectively:

```text
schema_version: int32 not null
record_uid: string not null
version_hash: string not null
mutation_policy: string not null
site_id: string not null
country: string not null
dataset: string not null
source_id: string not null
scope_type: string not null
scope_id: string
instrument_id: string
event_time: timestamp[us, tz=UTC]
updated_at: timestamp[us, tz=UTC]
crawled_at: timestamp[us, tz=UTC] not null
title: string
content: string
author_id: string
author_name: string
source_url: string
payload_json: large_string not null
relations_json: large_string not null
```

Do not casually change this schema.

Schema evolution should be explicit and versioned.

---

# 19. Partitioning Rules

Current layout:

```text
site=.../
country=.../
dataset=.../
year=YYYY/
month=MM/
day=DD/
bucket=xx/
part-....parquet
```

Remote storage is under the configured stock data lake path.

Partition datetime is based on DatasetSpec partition-time semantics.

Naive times are normalized to UTC.

Bucket key priority during write:

```text
instrument_id
or scope_id
or source_id
```

Important implication:

Query-side bucket pruning is safe only when `instrument_id` is explicitly known.

Do not derive bucket from `record_uid`.

---

# 20. Naver-Specific Important Semantics

Forum post identity must remain stable.

Dynamic values such as:

```text
view count
recommend
dislike
page number
crawl time
```

must not destabilize logical identity/version unless intentionally modeled.

Current forum-post payload should remain minimal/stable.

Canonical URL should not include irrelevant pagination identity.

---

# 21. Coding Rules for Codex

## 21.1 Inspect before modifying

Before changing a method:

```text
open actual file
inspect actual signature
inspect tests
```

Never guess APIs.

---

## 21.2 Prefer complete replacements for focused changes

When modifying a large file:

- replace a full method
- replace a full class
- avoid scattering tiny patches unless necessary

---

## 21.3 Preserve backward compatibility

Especially:

```text
single-instrument QuerySpec
query()
iter_batches()
existing CLI flags
existing Catalog APIs
```

---

## 21.4 Every architectural change requires tests

For each change:

```text
unit test
then full unit suite
then production smoke test when safe
```

---

## 21.5 No destructive production operations without dry-run

For:

```text
compaction
retirement
cleanup
repair
migration
```

support dry-run first.

---

## 21.6 Never silently change data semantics

Examples:

- local date vs UTC date
- lifecycle state
- hash composition
- partition key
- mutation policy
- source identity

If changing semantics, create migration plan and regression tests.

---

# 22. Recommended Work Order

Codex should execute the remaining work in this exact order unless a blocking dependency requires adjustment.

```text
PHASE A — finish core correctness
  A1. Fix UPSERT physical-state regression
  A2. Add regression tests
  A3. run full tests

PHASE B — query operational maturity
  B1. streaming/query observability
  B2. large universe input
  B3. production query smoke tests

PHASE C — storage maintenance
  C1. generic compaction
  C2. generic storage audit
  C3. safe repair workflows
  C4. retention/cleanup policy

PHASE D — recovery robustness
  D1. fault-injection tests
  D2. recovery idempotency
  D3. crash-boundary validation

PHASE E — scale validation
  E1. 100/1000 instrument query tests
  E2. memory testing
  E3. file-size / compaction tuning
  E4. longer production runs

PHASE F — architecture proof
  F1. second site integration
  F2. verify core remains unchanged
  F3. production smoke test

PHASE G — rollout
  G1. 50 instruments
  G2. 500 instruments
  G3. full market
  G4. monitor and tune
```

---

# 23. Acceptance Criteria by Phase

## Phase A complete when

```text
generic duplicate register cannot regress uploaded -> local
remote_path cannot be erased by generic UPSERT
lifecycle cannot be reactivated by generic UPSERT
explicit mark_uploaded still works
all unit tests pass
```

---

## Phase B complete when

```text
streaming exposes useful metrics
large instrument universe can be supplied from file
multi-bucket stays one Catalog SQL
large result export remains memory bounded
```

---

## Phase C complete when

```text
compaction is generic
audit detects Catalog/storage drift
repair is dry-run safe
cleanup cannot delete data before durable barrier
```

---

## Phase D complete when

```text
crash/retry at each major boundary is idempotent
checkpoint never advances prematurely
SeenStore never commits prematurely
active logical data remains consistent
```

---

## Phase E complete when

```text
1000-instrument queries remain efficient
streaming memory remains bounded
Catalog SQL count scales by query, not by instrument
Parquet file strategy is measured, not guessed
```

---

## Phase F complete when

```text
second structurally different site works
without redesigning core storage/query architecture
```

---

## Phase G complete when

```text
full-market long-running crawl is operational
recovery is proven
storage growth is manageable
query remains usable
```

---

# 24. Definition of “Framework Core Complete”

The core framework can be considered complete when all of these are true:

```text
Canonical model stable
SeenStore durable semantics stable
Checkpoint durable barrier stable
Parquet storage stable
Catalog lifecycle stable
UPSERT monotonic state safe
single + multi instrument query stable
timezone semantics stable
bucket pruning stable
streaming stable
hard max_rows stable
generic compaction available
audit/repair available
recovery fault-tested
```

At that point, adding new websites should mainly be a plugin implementation task rather than framework redesign.

---

# 25. Current Estimated Progress

Current state:

```text
Core framework: approximately 85% complete
Query Layer: approximately 95% complete
Storage maintenance/operations: incomplete
Second-site proof: incomplete
Full-market long-run validation: incomplete
```

The remaining work is less about inventing architecture and more about:

```text
correctness hardening
observability
maintenance tooling
recovery testing
scale testing
second-site proof
```

---

# 26. First Command Codex Should Run

Before changing anything:

```bash
pytest -q tests/unit
```

Expected current baseline:

```text
392 passed
```

Then inspect:

```text
src/crawl_framework/storage/postgres.py
tests/unit/test_postgres.py
```

and begin **Phase A1: UPSERT physical-state regression fix**.

---

# 27. Final Instruction to Codex

Work incrementally and preserve the proven system.

For every task:

```text
1. inspect current implementation
2. inspect tests
3. make the smallest architecture-consistent change
4. add regression tests
5. run focused tests
6. run full unit suite
7. run a safe production smoke test when relevant
8. document changed semantics
```

Never trade correctness for convenience.

Never bypass:

```text
uploaded + active
```

Catalog semantics.

Never advance SeenStore/checkpoint before durable storage.

Never make large-query code accumulate entire result sets in memory.

Never reintroduce one SQL call per instrument when multi-bucket batch lookup exists.

The goal is not merely to “make tests pass”.

The goal is a reusable, recoverable, auditable, scalable financial data collection and query platform.
