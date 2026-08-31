# Crawl Framework V2 — Codex Master Execution Plan

> **This file is the authoritative execution plan.**
>
> Codex MUST follow the execution order in **Section 3 — Execution Roadmap**.
>
> All later sections are **technical specifications for the corresponding roadmap item**.
> They are NOT independent "next tasks" and MUST NOT override the roadmap order.
>
> Actual repository code + tests are the factual source of truth.
> `PROJECT_STATUS.md` records current progress.
>
> If this file, `PROJECT_STATUS.md`, and the repository disagree:
> 1. inspect the real code and tests;
> 2. identify whether work was already completed;
> 3. preserve verified newer behavior;
> 4. update `PROJECT_STATUS.md`;
> 5. continue from the first genuinely incomplete roadmap item.

---

# 1. Project Mission

Build a reusable, recoverable, auditable, scalable financial web crawling, storage, and query framework for multiple sites and countries.

Target sites include:

- Naver Finance
- TossInvest
- HotCopper
- Stockhouse
- future financial/news/community/research sources

Target long-term scale may reach billions of comments/posts.

Core architecture:

```text
site-specific plugin
    ↓
crawl / discover
    ↓
normalize CanonicalRecord
    ↓
SeenStore inspect
    ↓
buffer
    ↓
atomic local Parquet
    ↓
sidecar / manifest
    ↓
upload
    ↓
verify
    ↓
PostgreSQL Catalog
    ↓
SeenStore commit
    ↓
checkpoint commit
    ↓
cleanup
    ↓
Catalog + Parquet Query Layer
```

Main durable data:
- Parquet on remote storage/NAS

Control/catalog data:
- PostgreSQL

Local-only cache/history:
- SQLite acceptable only for SeenStore/cache-like purposes

---

# 2. Current Proven Baseline

Current confirmed baseline:

```text
396 unit tests passed
```

Known completed capabilities include:

- CanonicalRecord identity/version semantics
- SeenStore inspect/commit
- checkpoint durable barrier
- Parquet partitioning
- PostgreSQL Catalog
- `storage_status` vs `lifecycle_status`
- active/uploaded filtering
- lifecycle compaction semantics
- single-instrument query
- multi-instrument query
- timezone-aware local-natural-day query
- UTC partition pruning
- bucket pruning
- multi-bucket single-SQL lookup
- Arrow predicate pushdown
- streaming RecordBatch query
- hard `max_rows` stop
- JSONL output/export
- streaming Parquet export
- Naver production smoke tests

Known production multi-instrument query:

```text
XKRX:000660
XKRX:005930
XKRX:042700

113 rows
```

Current Phase A result:

```text
A1 UPSERT physical-state regression fix   DONE
A2 regression tests                       DONE
A3 full unit validation                   DONE

Current baseline: 396 passed
```

---

# 3. EXECUTION ROADMAP — THE ONLY AUTHORITATIVE ORDER

Codex MUST determine the next task from this section plus `PROJECT_STATUS.md`.

Do NOT infer execution order from later section numbers or headings.

```text
PHASE A — Core correctness
  A1. Fix PostgreSQL UPSERT physical-state regression
  A2. Add regression tests
  A3. Run full unit suite

PHASE B — Query operational maturity
  B1. Streaming/query observability
  B2. Large universe input
  B3. Production query smoke tests

PHASE C — Storage maintenance
  C1. Generic compaction
  C2. Generic storage audit
  C3. Safe repair workflows
  C4. Retention/cleanup policy

PHASE D — Recovery robustness
  D1. Fault-injection tests
  D2. Recovery idempotency
  D3. Crash-boundary validation

PHASE E — Scale validation
  E1. 100/1000-instrument query tests
  E2. Memory testing
  E3. File-size / compaction tuning
  E4. Longer production runs

PHASE F — Architecture proof
  F1. Second-site integration
  F2. Verify core remains unchanged
  F3. Production smoke test

PHASE G — Rollout
  G1. 50 instruments
  G2. 500 instruments
  G3. Full market
  G4. Monitor and tune
```

## 3.1 Execution rule

At startup:

1. Read this file.
2. Read `PROJECT_STATUS.md`.
3. Run `git status --short`.
4. Run `pytest -q tests/unit`.
5. Compare repository reality with `PROJECT_STATUS.md`.
6. Find the **first incomplete item in Section 3**.
7. Work on that item only until its acceptance criteria are met.
8. Update `PROJECT_STATUS.md`.
9. Continue automatically to the next roadmap item unless a stop condition is triggered.

## 3.2 Stop conditions

Stop and ask the user only when:

- production data may be deleted or overwritten;
- a destructive/irreversible DB migration is required;
- real NAS data may be modified destructively;
- tests expose a major architecture ambiguity;
- repository state conflicts materially with this plan and no clear backward-compatible path exists;
- credentials/secrets or unavailable external infrastructure are required.

Normal code changes, tests, refactors, CLI additions, query/storage/recovery work do NOT require user confirmation.

---

# 4. Non-Negotiable Architecture Rules

## 4.1 Canonical identity

`record_uid` = logical identity.

Conceptual formula:

```text
stable_sha256(
    site_id,
    dataset,
    scope_type,
    scope_id,
    source_id
)
```

`version_hash` represents meaningful record version/content.

Do not include unstable page number, crawl timestamp, view count, etc. unless intentionally part of version semantics.

## 4.2 SeenStore vs checkpoint

SeenStore:
- durable record/version history

Checkpoint:
- crawler progress

Never commit either before durable storage success.

## 4.3 Durable barrier

Required order:

```text
crawl
→ normalize
→ Seen inspect
→ buffer
→ local Parquet atomic write
→ sidecar
→ manifest
→ upload
→ verify
→ Catalog
→ Seen commit
→ checkpoint
→ cleanup
```

## 4.4 Catalog state

Physical:

```text
storage_status:
  local
  uploaded
```

Logical:

```text
lifecycle_status:
  active
  superseded
  archived
```

Normal query:

```sql
storage_status = 'uploaded'
AND lifecycle_status = 'active'
```

---

# 5. Query Semantics That Must Be Preserved

## 5.1 Instrument query

Support:

```text
instrument_id
instrument_ids
```

Single-instrument behavior must remain backward compatible.

Multi-instrument:
- strip
- deduplicate
- preserve stable input order
- compute stable buckets
- deduplicate buckets
- Catalog pruning by bucket(s)
- exact Arrow filtering by requested instrument IDs

Bucket pruning is coarse file-level pruning only.

## 5.2 Timezone semantics

User dates are local natural dates.

Example:

```text
timezone = Asia/Seoul
date = 2026-08-26
```

means:

```text
>= 2026-08-25 15:00:00 UTC
<  2026-08-26 15:00:00 UTC
```

UTC physical partition pruning must be derived from this interval.

## 5.3 Full vs streaming query

`query()`:
- full `pa.Table`
- suitable for small/interactive query
- stable global sort by `event_time`, then `record_uid` when available

`iter_batches()`:
- streaming `RecordBatch`
- bounded memory
- no global cross-file sort guarantee
- supports `max_rows`

## 5.4 CLI semantics

`--limit`:
- display/output limit only

`--max-rows`:
- hard execution limit
- stop scanning further once reached

Never merge these meanings.

---

# 6. PHASE A SPECIFICATION — Core Correctness

Status expected at current handoff:

```text
A1 DONE
A2 DONE
A3 DONE
396 passed
```

## A1. UPSERT physical-state regression

Generic `register_data_file()` conflict update must NOT overwrite:

```text
storage_status
remote_path
lifecycle_status
```

Explicit state transition ownership:

```text
register_data_file()
    metadata registration only

mark_uploaded()
    local -> uploaded
    set remote_path
```

## A2. Regression tests

Must cover:
- uploaded state preserved
- remote_path preserved
- lifecycle preserved
- `mark_uploaded()` still works

## A3. Full validation

Run:

```bash
pytest -q tests/unit
```

Current expected baseline after Phase A:

```text
396 passed
```

---

# 7. PHASE B1 SPECIFICATION — Streaming / Query Observability

> This section is the technical specification for roadmap item **B1**.
> It is NOT an independent execution-order instruction.

Current full query exposes roughly:

```text
catalog_files
parquet_files_read
rows_read
rows_returned
```

Streaming currently exposes mainly:

```text
total_batches
rows_returned
```

## Required metrics

At minimum:

```text
catalog_sql_calls
catalog_files
parquet_files_materialized
parquet_files_read
candidate_physical_rows
rows_yielded
bytes_materialized
batches_yielded
```

Recommended timing metrics:

```text
query_duration_seconds
catalog_duration_seconds
materialization_duration_seconds
scan_duration_seconds
```

### Semantic requirement

`item.row_count` means physical rows in candidate files.

Do NOT label it exact Arrow-scanned rows.

Prefer:

```text
candidate_physical_rows
```

### Compatibility requirement

Do not break current `iter_batches()` generator API.

Possible implementation patterns:
- mutable `StreamingQueryStats`
- `iter_batches_with_stats()`
- streaming session object

Choose the simplest backward-compatible design.

### Acceptance criteria B1

- existing query APIs remain compatible
- streaming remains bounded-memory
- metrics are well-defined
- CLI/reporting exposes useful stats
- focused tests pass
- full unit suite passes
- `PROJECT_STATUS.md` updated

---

# 8. PHASE B2 SPECIFICATION — Large Universe Input

Add:

```text
--instruments-file path/to/universe.txt
```

Format:

```text
XKRX:005930
XKRX:000660
XKRX:042700
```

Requirements:
- UTF-8
- blank lines ignored
- whitespace stripped
- duplicates removed
- stable order preserved
- explicit merge semantics with `--instrument` / `--instruments`
- empty effective set handled safely

### Acceptance criteria B2

- large instrument list does not require huge shell arg list
- old CLI flags remain compatible
- tests cover duplicate/blank/merge behavior
- full unit suite passes

---

# 9. PHASE B3 SPECIFICATION — Production Query Smoke Tests

Perform safe read-only production query validation.

At minimum validate:
- single instrument
- 3 instruments
- larger universe if available
- timezone date filter
- streaming
- `max_rows`
- JSONL export
- Parquet export
- observability metrics

Do not mutate production data.

### Acceptance criteria B3

- output instruments correct
- no superseded files read
- no unexpected SQL-per-instrument regression
- expected bounded streaming behavior
- status recorded in `PROJECT_STATUS.md`

---

# 10. PHASE C1 SPECIFICATION — Generic Compaction

Convert site-specific compaction into reusable storage maintenance.

Group dimensions:

```text
site_id
country
dataset
partition_date
bucket
```

Workflow:

```text
select active files
→ read
→ deduplicate according to logical/version policy
→ write replacement
→ verify
→ register
→ upload/activate replacement
→ mark old files superseded
```

Never supersede source files before replacement is durable.

Must support dry-run.

Suggested CLI:

```text
scripts/compact_data.py
```

### Acceptance criteria C1

- generic across datasets/sites
- idempotent
- dry-run available
- lifecycle-safe
- tests + full suite pass

---

# 11. PHASE C2 SPECIFICATION — Generic Storage Audit

Detect:

```text
active + uploaded + missing remote_path
remote file missing
size mismatch
checksum mismatch
orphan remote files
stale local files
lifecycle inconsistencies
```

Structured output preferred:

```text
JSON / JSONL
```

Summary metrics:

```text
files_checked
catalog_errors
missing_remote
size_mismatch
checksum_mismatch
orphans
```

### Acceptance criteria C2

- read-only audit mode safe
- useful machine-readable report
- tests cover major inconsistency classes

---

# 12. PHASE C3 SPECIFICATION — Safe Repair

Repair must be explicitly invoked.

Modes:

```text
dry-run
repair-safe
```

Do not auto-delete.

Repair should handle only deterministic safe cases unless explicitly authorized.

### Acceptance criteria C3

- dry-run clearly shows intended changes
- repair is idempotent where possible
- destructive actions separated from safe repair
- tests + full suite pass

---

# 13. PHASE C4 SPECIFICATION — Retention / Cleanup

Never delete local files before:

```text
upload success
verification success
Catalog success
SeenStore commit
checkpoint commit
```

Potential policies:

```text
keep local failures
manifest retention
superseded rollback window
archive before permanent delete
```

### Acceptance criteria C4

- deletion policy explicit
- no premature cleanup
- dry-run for risky cleanup
- tests cover barrier safety

---

# 14. PHASE D1 SPECIFICATION — Fault Injection

Inject failures after:

```text
Parquet write
upload
verify
Catalog register
SeenStore commit
checkpoint
```

Validate:
- no data loss
- no premature checkpoint
- retry safe
- no duplicate logical active data

---

# 15. PHASE D2 SPECIFICATION — Recovery Idempotency

Repeated recovery must converge to a correct durable state.

Validate:
- state transitions monotonic
- no reactivation of superseded/archived
- uploaded does not regress to local
- retry does not duplicate logical active data

---

# 16. PHASE D3 SPECIFICATION — Crash Boundary Validation

Test crash/restart behavior across all durable boundaries.

Acceptance:
- recovery produces same logical end state as uninterrupted run

---

# 17. PHASE E1 SPECIFICATION — 100/1000 Instrument Query Scale

Measure:
- Catalog SQL calls
- candidate files
- buckets
- rows
- time

Important invariant:

```text
SQL count must not scale one-per-instrument
```

---

# 18. PHASE E2 SPECIFICATION — Memory Testing

Verify bounded memory for:
- `iter_batches()`
- JSONL export
- Parquet export

Never accumulate full large result via full-table `to_pylist()`.

Per-batch conversion is acceptable.

---

# 19. PHASE E3 SPECIFICATION — File Size / Compaction Tuning

Benchmark practical target Parquet sizes.

Avoid:
- huge tiny-file counts
- giant files that make narrow SCP queries expensive

Do not hard-code tuning without measurements.

---

# 20. PHASE E4 SPECIFICATION — Longer Production Runs

Progress gradually.

Measure:
- crawl errors
- retries
- duplicate rate
- records/sec
- files/day
- storage growth
- Catalog growth
- NAS traffic
- checkpoint/recovery behavior

---

# 21. PHASE F SPECIFICATION — Second-Site Architecture Proof

Recommended candidates:
- TossInvest
- HotCopper
- Stockhouse

Goal:

```text
core/
storage/
query/
SeenStore/
checkpoint/
Catalog/
```

should require little or no redesign.

Implement primarily:

```text
sites/<new_site>/
```

plus registry/config.

Acceptance:
- discover
- crawl
- normalize
- checkpoint
- dedup
- Parquet
- Catalog
- query
- production smoke

---

# 22. PHASE G SPECIFICATION — Rollout

Progress:

```text
50 instruments
→ 500 instruments
→ full market
```

Monitor and tune before each expansion.

Do not jump directly to full-market crawling.

---

# 23. PostgreSQL / Catalog Rules

Important table:

```text
marketdata.data_files
```

Normal query filter:

```sql
storage_status = 'uploaded'
AND lifecycle_status = 'active'
```

Intended APIs include:

```python
list_active_data_files(...)
list_active_data_files_range(...)
list_active_data_files_multi_bucket(...)
list_active_data_files_range_multi_bucket(...)
get_data_file(...)
```

Multiple buckets should use one SQL such as:

```sql
bucket = ANY(%(buckets)s)
```

Do not regress to one SQL per instrument.

---

# 24. Canonical Parquet Schema

Current canonical schema:

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

Schema evolution must be explicit/versioned.

---

# 25. Partitioning Rules

Layout:

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

Bucket key write priority:

```text
instrument_id
or scope_id
or source_id
```

Query-side bucket pruning is safe only when instrument ID is explicitly known.

---

# 26. Codex Development Rules

Before modifying:
1. inspect actual source
2. inspect signatures
3. inspect tests

Do not guess APIs.

For each roadmap item:
1. inspect
2. implement smallest compatible change
3. add/update regression tests
4. run focused tests
5. run `pytest -q tests/unit`
6. update `PROJECT_STATUS.md`
7. continue to next incomplete roadmap item

Do not:
- redesign working architecture casually
- alter hash semantics silently
- alter timezone semantics silently
- bypass active/uploaded filters
- advance checkpoint/SeenStore early
- make streaming accumulate entire result set
- perform destructive production actions without explicit authorization

---

# 27. Framework Core Complete Definition

Core is complete when:

```text
Canonical model stable
SeenStore semantics stable
checkpoint durable barrier stable
Parquet storage stable
Catalog lifecycle stable
UPSERT monotonic state safe
single + multi instrument query stable
timezone semantics stable
bucket pruning stable
streaming stable
max_rows stable
query observability available
large universe input available
generic compaction available
audit/repair available
retention safe
recovery fault-tested
scale behavior validated
```

Second-site proof then demonstrates generality.

---

# 28. Current Estimated Progress

```text
Core framework: ~85–90%
Query layer: ~95%
Storage maintenance: incomplete
Recovery hardening: incomplete
Scale validation: incomplete
Second-site proof: incomplete
Full-market rollout: incomplete
```

---

# 29. Startup Prompt for Codex

Use this behavior every new Codex session:

```text
Read:
1. Crawl_Framework_V2_Codex_Master_Plan.md
2. PROJECT_STATUS.md

Then:
- inspect git status
- run pytest -q tests/unit
- reconcile actual repository state with PROJECT_STATUS.md
- locate the first incomplete item in Section 3 of the Master Plan
- continue automatically from there

Section 3 is the only authoritative execution order.
Later sections are technical specifications only.

Do not wait for user confirmation between ordinary roadmap tasks.
Update PROJECT_STATUS.md after each completed item.

Stop only for destructive production operations, irreversible migrations,
real NAS data deletion/overwrite, major unresolved architecture conflicts,
or unavailable required credentials/infrastructure.
```
