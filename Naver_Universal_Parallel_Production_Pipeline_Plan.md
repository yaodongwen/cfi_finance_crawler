# Crawl Framework V2 — Phase H: Universal Parallel Production Pipeline

## 1. Goal

Make Naver the first full production site adapter while keeping the whole pipeline reusable for future sites.

Target architecture:

```text
Site Adapter
  -> instrument/task discovery
  -> parallel crawl workers
  -> normalize CanonicalRecord
  -> SeenStore inspect
  -> bounded record queue
  -> buffer / Parquet packaging
  -> bounded upload queue
  -> parallel upload workers
  -> remote verify
  -> bounded catalog/index queue
  -> parallel Catalog/index workers
  -> SeenStore commit
  -> checkpoint commit
  -> cleanup
```

Required properties:

- multi-site reusable
- multi-dataset reusable
- configurable crawl concurrency
- configurable attachment/PDF concurrency
- configurable upload concurrency
- configurable Catalog/index concurrency
- generic proxy/IP-pool transport
- configurable target package/file size
- bounded queues and backpressure
- streaming operation
- crash/retry safe
- no premature SeenStore/checkpoint commits

---

## 2. First cleanup

The interrupted Codex work created both:

```text
web/naver/
src/crawl_framework/web/naver/
```

Do not maintain duplicate production implementations.

Choose one canonical installable implementation after inspecting project conventions. Preferred:

```text
src/crawl_framework/web/naver/
```

or merge into existing:

```text
src/crawl_framework/sites/naver_finance/
```

The root-level `web/naver/` may only remain as docs/examples/thin compatibility re-export. It must not contain an independent second implementation.

Before further work:

```bash
git diff
pytest -q tests/unit
```

Finish/repair the interrupted universe work and restore a green full test suite.

---

## 3. Generic Site Adapter boundary

The core must not know Naver URLs, selectors, pagination, nid, office_id, PDF URL formats, or site response structure.

Recommended conceptual contract:

```python
class SiteAdapter:
    site_id: str

    def supported_datasets(self) -> tuple[str, ...]:
        ...

    async def discover_instruments(self, context):
        ...

    async def discover_tasks(
        self,
        dataset,
        scope,
        checkpoint,
        context,
    ):
        ...

    async def fetch(
        self,
        dataset,
        task,
        context,
    ):
        ...

    def normalize(
        self,
        dataset,
        raw,
        scope,
    ):
        ...

    async def discover_attachments(
        self,
        dataset,
        raw,
        context,
    ):
        ...
```

Sites may support only a subset of capabilities/datasets.

---

## 4. Naver required capabilities

Required datasets:

```text
instrument
news_article
news_instrument
forum_post
research_report
research_instrument
attachment
```

Optional later:

```text
comment
author_profile
author_post
```

---

## 5. Naver instrument discovery

Authoritative source:

```text
https://finance.naver.com/sise/sise_market_sum.naver
```

Markets:

```text
KOSPI  sosok=0
KOSDAQ sosok=1
```

Requirements:

- paginate to completion
- parse stock code
- canonicalize to `XKRX:NNNNNN`
- stable order
- deduplicate
- persist reproducible rollout snapshot
- metadata records source, generated time, count

The adapter owns Naver HTML logic. Generic rollout code consumes only canonical instrument IDs.

---

## 6. Naver news

Site layer handles:

```text
news list
news detail
article_id
office_id
title
content
published_at
source/author
source_url
related instrument
```

Canonical storage:

```text
news_article
news_instrument
```

One logical article body should be stored once even if related to multiple instruments.

Do not let pagination/crawl time destabilize identity.

---

## 7. Naver forum

Site layer handles:

```text
board pagination
nid
detail fetch
title
content
writer
written_at
source_url
```

Canonical dataset:

```text
forum_post
```

Future true replies/comments should use separate `comment` dataset.

Do not encode page number into logical identity.

---

## 8. Naver research metadata

Implement a dedicated Naver research interface.

Canonical metadata:

```text
research_report
research_instrument
```

Recommended metadata:

```text
source/report id
title
institution
analyst
published_at
summary/abstract
source_url
pdf_url
payload_json
```

A multi-stock report should be one report plus relation records.

---

## 9. Generic PDF / attachment subsystem

Do not embed PDF binary content in Parquet.

Generic attachment metadata:

```text
attachment_id
parent_record_uid
site_id
dataset
source_url
filename
mime_type
sha256
file_size
remote_path
fetched_at
```

Example physical layout:

```text
attachments/
  site=naver_finance/
  country=KR/
  dataset=research_report/
  year=YYYY/
  month=MM/
  day=DD/
  <sha-or-id>.pdf
```

Validation:

- successful response
- non-empty content
- PDF/MIME check where practical
- SHA256
- file size
- remote verification before local cleanup

Attachment download workers must be independent from metadata crawl workers.

---

## 10. Proxy/IP pool ownership

Site adapters must not rotate proxy ports themselves.

Correct ownership:

```text
Site Adapter
  -> CrawlContext
  -> HttpTransport
  -> ProxyPool
```

Generic proxy pool should support where practical:

```text
round_robin
random
failure cooldown
retry
health tracking
per-request proxy selection
```

All future sites reuse the same transport.

---

## 11. Configurable parallelism

Separate stage concurrency.

Required knobs:

```text
crawl_workers
http_concurrency
attachment_workers
writer_workers
upload_workers
catalog_workers
```

Example:

```yaml
runtime:
  crawl_workers: 32
  attachment_workers: 8
  writer_workers: 2
  upload_workers: 4
  catalog_workers: 4

http:
  concurrency: 64
```

CLI overrides should be supported.

---

## 12. Bounded asynchronous pipeline

Do NOT implement:

```text
crawl everything
then upload everything
then index everything
```

Use bounded queues:

```text
crawl workers
  -> record_queue
  -> writer/buffer workers
  -> durable_file_queue
  -> upload workers
  -> verified_file_queue
  -> Catalog/index workers
  -> commit barrier
```

Research attachment sibling path:

```text
research metadata
  -> attachment_queue
  -> attachment download workers
  -> attachment upload
  -> attachment Catalog/index
```

Backpressure:

```text
downstream queue full
-> upstream awaits
-> crawler naturally slows
```

Use bounded `asyncio.Queue` or equivalent.

---

## 13. Configurable package/flush size

Do not flush only by fixed row count.

Support:

```text
max_rows
target_bytes
max_buffer_age_seconds
```

Flush when any trigger fires:

```text
rows >= max_rows
OR estimated_bytes >= target_bytes
OR buffer age >= max_buffer_age_seconds
```

Example:

```yaml
storage:
  target_file_size_mb: 128
  max_rows_per_file: 200000
  max_buffer_age_seconds: 60
```

Actual compressed Parquet size may differ from the target; record estimated and actual sizes.

---

## 14. Generic upload workers

Uploads overlap with crawling/writing.

Generic upload job contains:

```text
local_path
remote_relative_path
size
sha256
site/dataset/partition metadata
retry policy
```

Requirements:

- configurable worker count
- retry
- size verification
- SHA verification when enabled
- Catalog activation only after verified upload

Site adapters never call SCP/rsync directly.

---

## 15. Catalog and record index

Two layers:

### File-level Catalog

PostgreSQL `data_files` stores:

```text
site
country
dataset
partition_date
bucket
file_path
row_count
file_size
sha256
min/max event_time
remote_path
storage_status
lifecycle_status
```

### Record-level index

For very large scale, do not automatically put billions of row-level entries into PostgreSQL.

Use the existing generic record-index subsystem and evaluate scalable side indexes, e.g. partitioned Parquet:

```text
record_uid
instrument_id
event_time
file_path
optional row_group
```

Document which index is authoritative for:

```text
record_uid -> file
instrument/date -> files
```

Index construction must be reusable and parallelizable.

---

## 16. Durable commit barrier under concurrency

Per durable batch/file preserve:

```text
local write
-> side index / manifest
-> upload
-> verify
-> Catalog
-> SeenStore commit
-> checkpoint commit
-> cleanup
```

Concurrency is allowed across independent batches, not by violating required order within one batch.

---

## 17. Parallel checkpoint safety

Parallel crawling may finish out of order.

Do not advance a sequential checkpoint past unfinished earlier work.

Use one of:

```text
ordered commit watermark
task-level independent checkpoints
completion set + contiguous watermark
```

Choose based on existing checkpoint model.

Add out-of-order completion tests.

---

## 18. Failure/retry behavior

Test failures at:

```text
crawl fetch
normalize
Parquet write
attachment download
upload
remote verify
Catalog/index
SeenStore commit
checkpoint
```

Required:

- restart safe
- no premature checkpoint
- no logical active duplication
- uploaded state never regresses
- failed jobs retryable

---

## 19. Configuration shape

Recommended:

```yaml
runtime:
  crawl_workers: 32
  writer_workers: 2
  upload_workers: 4
  catalog_workers: 4
  attachment_workers: 8

queues:
  records: 5000
  durable_files: 256
  uploads: 256
  catalog: 256
  attachments: 512

storage:
  target_file_size_mb: 128
  max_rows_per_file: 200000
  max_buffer_age_seconds: 60

upload:
  verify_size: true
  verify_sha256: true

proxy:
  enabled: true
  strategy: round_robin
```

---

## 20. Generic CLI target

Forum:

```bash
python -m crawl_framework.cli.main crawl   --site naver_finance   --dataset forum_post   --instruments-file config/universes/naver_finance_kr_rollout_universe.txt   --crawl-workers 32   --upload-workers 4   --target-file-size-mb 128
```

News:

```bash
python -m crawl_framework.cli.main crawl   --site naver_finance   --dataset news_article   --instruments-file ...   --crawl-workers 32
```

Research:

```bash
python -m crawl_framework.cli.main crawl   --site naver_finance   --dataset research_report   --instruments-file ...   --crawl-workers 16   --attachment-workers 8
```

Generic CLI must not contain Naver URL logic.

---

## 21. Multi-site reuse proof

After Naver is complete, add a minimal second mock/test adapter.

The second adapter should only implement site capabilities while reusing unchanged:

```text
pipeline
SeenStore
buffering
Parquet writer
uploader
Catalog
record index
checkpoint
recovery
query
```

This proves the architecture before implementing another full real website.

---

## 22. Implementation order

Execute strictly:

```text
H0. Repair interrupted Codex changes and restore green tests
H1. Consolidate canonical Naver package path
H2. Define generic SiteAdapter/capability contract
H3. Finish Naver instrument discovery interface
H4. Naver news adapter
H5. Naver forum adapter integration
H6. Naver research metadata adapter
H7. Generic attachment subsystem + Naver PDF
H8. Generic proxy-pool transport integration
H9. Configurable stage concurrency
H10. Configurable target package/file size
H11. Bounded asynchronous production pipeline
H12. Parallel upload workers
H13. Parallel Catalog/index workers
H14. Parallel-safe checkpoint/watermark
H15. Failure/retry/recovery tests
H16. Generic CLI wiring
H17. Naver 3-instrument smoke
H18. Naver 50-instrument production run
H19. Naver 500-instrument production run
H20. Naver full-market production run
H21. Multi-site reuse proof with second adapter/mock
```

---

## 23. Acceptance criteria

Phase H is complete only when:

```text
Naver full instrument discovery works
Naver news batch crawl works
Naver forum batch crawl works
Naver research metadata works
research PDFs download and upload safely
site-specific code stays in adapter/client layer
core does not know Naver URLs/HTML/API
crawl concurrency configurable
proxy pool reusable
attachment concurrency configurable
upload concurrency configurable
Catalog/index concurrency configurable
target package/file size configurable
bounded queues/backpressure implemented
crawl/write/upload/index overlap in time
durability barrier preserved
parallel checkpoint semantics safe
record/file indexes correct
recovery tests pass
Naver full-market run succeeds
second adapter reuses pipeline without core redesign
```

---

## 24. Immediate next action

In the next coding session:

```text
1. inspect git diff from interrupted universe work
2. consolidate duplicate Naver implementations
3. make universe tests green
4. run full unit suite
5. update project_status.md
6. start H2 generic SiteAdapter/capability contract
```

Do not run production crawl until H0/H1 are clean and tested.
