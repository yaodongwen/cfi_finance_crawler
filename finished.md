# Finished

Last verified baseline:

```text
conda activate pac
pytest -q -p no:rerunfailures tests/unit
527 passed
```

## Core Framework

- Phase A core correctness is complete. PostgreSQL UPSERT no longer regresses physical storage state fields such as `storage_status`, `remote_path`, and `lifecycle_status`.
- Query operational maturity is complete: streaming query stats, large universe input, JSONL/Parquet export, predicate/bucket pruning, multi-instrument query, and production query smoke have been validated.
- Storage maintenance is complete: generic compaction, audit, repair workflow, and retention/cleanup policy exist with regression tests.
- Recovery robustness is complete at unit/integration-test level: fault injection, idempotent recovery, crash boundary handling, and startup recovery checks pass.
- Scale validation and architecture proof are complete at framework-test level, including large-universe query tests and TossInvest as a second site proof from earlier phases.

## Phase G / Rollout Universe

- A deterministic Naver rollout universe snapshot exists at:

```text
config/universes/naver_finance_kr_rollout_universe.txt
```

- Snapshot source is Naver Finance market-sum discovery.
- Snapshot size is 3924 canonical instruments.
- Rollout uses the same snapshot for:

```text
G1/H18: first 50
H19: first 500
H20: full 3924
```

## Phase H Completed Work

- H0-H18 are complete.
- Naver-specific logic has been consolidated into site/client/adapter layers.
- Generic reusable pieces exist for:

```text
SiteAdapter boundary
Naver instrument discovery
Naver forum/news/research adapter surface
generic attachments
generic proxy transport
stage concurrency config
queue-size config
target file size / rows / age flush config
bounded concurrent production runtime
parallel upload/catalog stages
checkpoint/recovery tests
CLI wiring
```

- H18.5 is complete. The real production CLI now wires into `ConcurrentProductionRuntime` when stage worker counts are greater than 1.
- `StoragePipeline` durable lifecycle was split into reusable stages:

```text
prepare_batch
upload_prepared_batch
catalog_uploaded_batch
```

- Production runtime now follows:

```text
CLI -> AppFactory -> CrawlBootstrap -> ConcurrentProductionRuntime
-> crawl workers -> bounded record queue
-> writer workers -> bounded upload queue
-> upload workers -> bounded catalog queue
-> catalog/index workers -> SeenStore/checkpoint
```

- H19 first-500 production rollout completed successfully after fixing Naver detail 404 handling.
- H21 multi-site reuse proof is complete: TossInvest mock/fixture adapter reuses the same generic concurrent runtime, storage pipeline, uploader, catalog, record index, checkpoint, recovery, and query path.

## Production Validations Completed

- H17 3-instrument Naver smoke succeeded.
- H18 first-50 Naver production run succeeded.
- H18.5 production concurrency smoke succeeded with real overlap among crawl/upload/catalog stages.
- H19 first-500 Naver production run succeeded with concurrent production runtime.
- Post-run recovery checks have reported:

```text
remaining_pending=0
```
