# Codex Prompt — TossInvest Real Migration

Read these repositories/files first.

Target repository:

```text
https://github.com/yaodongwen/cfi_finance_crawler
```

Legacy reference repository:

```text
https://github.com/yaodongwen/toss_nvest_crawl
```

Authoritative migration plan in target repo:

```text
TossInvest_Legacy_Migration_Plan.md
```

Also read:

```text
finished.md
problem.md
todo.md
legacy_reference.md
project_status.md
```

The target framework core and Naver implementation are already mature.
Do NOT redesign the generic storage/runtime/query architecture merely to migrate TossInvest.

The current TossInvest plugin is only fixture-backed architecture proof.
The goal is to migrate the real site behavior from `toss_nvest_crawl` into the
generic framework.

Execute Phase J strictly in this order:

```text
J0 Repository audit + legacy traceability
J1 Generic Playwright transport/browser worker pool
J2 Real Toss instrument discovery
J3 Real Toss forum/community client
J4 Forum full field parity
J5 Real Toss news list discovery
J6 Real Toss news detail parser
J7 news_article/news_instrument relations
J8 Historical baseline + pending/resume semantics
J9 Browser worker pool + ProxyPool integration
J10 Dataset-specific concurrency/backpressure
J11 1-stock real production smoke
J12 4-stock concurrent smoke
J13 20-stock production smoke
J14 100-stock rollout
J15 Full Toss KR universe rollout
J16 toss_incremental / toss_full profiles
```

Before each J item:

1. inspect actual current target code;
2. inspect the corresponding old functions listed in
   `TossInvest_Legacy_Migration_Plan.md`;
3. inspect current tests;
4. preserve newer generic framework behavior.

Important legacy sources:

```text
crawler.py
collect_stocks.py
normalizer.py
cache_manager.py
production_runner.py
main.py
```

For real site migration, preserve the old project's proven Toss-specific knowledge:

```text
Screener domestic-market switching
safe filter removal
virtual-list scrolling
stock row parsing
community latest sorting
forum card parsing
shareholder/follower fields
relative time
news list discovery
news_id parsing
historical baseline semantics
pending news resume semantics
news detail metadata v4 parsing
title/publisher/date/author provenance
Playwright profile isolation
```

Do NOT copy back the old infrastructure as authoritative architecture:

```text
per-record JSONL primary storage
PersistentIdSet as production SeenStore
_pending_news_links.json as primary recovery
_news_state.json as primary checkpoint
production_runner state.json
Toss-specific Parquet writer
Toss-specific Catalog pipeline
subprocess-per-stock pipeline
single storage worker
site-owned upload/cleanup logic
```

Map those semantics into the existing generic:

```text
SeenStore
checkpoint
RecoveryStore
ConcurrentProductionRuntime
StoragePipeline
ParquetWriter
Uploader
PostgreSQL Catalog
record index
Cleaner
Query Layer
run manifest
completeness audit
```

Critical rule:

A Toss feature cannot be marked complete merely because:

```text
interface exists
fixture test passes
FakeClient works
```

For J2/J3/J5/J6 and production-facing features, require:

```text
real TossInvest browser/HTTP behavior
production plugin wiring
generic durable pipeline
real smoke
```

Field parity is mandatory.

Forum must preserve, at minimum:

```text
post_id
author
profile image
created_at
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

News must preserve, at minimum:

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
related instruments
```

Historical-news rule:

Before historical baseline is complete, encountering known news IDs must NOT
cause early stop. Only after baseline is complete may consecutive history-only
rounds trigger incremental stop.

Playwright rule:

Implement/reuse generic:

```text
src/crawl_framework/transports/playwright.py
```

Do not let Toss site code own an unbounded browser lifecycle.
Browser concurrency, profile isolation and proxy assignment must be generic.

Proxy rule:

Use existing `ProxyPool`.
Do not reduce the final architecture to one `TOSS_PROXY_SERVER`.

Status-document rule:

- do NOT delete `finished.md`, `problem.md`, or `todo.md`;
- preserve all valid Naver/framework history;
- append/update Toss sections;
- move items from todo -> finished only after acceptance;
- remove a problem only when the real code/tests/smoke prove it is resolved.

After each J item:

```text
run focused tests
run pytest -q -p no:rerunfailures tests/unit
update finished.md/problem.md/todo.md
continue automatically
```

For every completion report state:

```text
legacy files/functions reviewed
behavior migrated
behavior intentionally not copied
new implementation location
focused test result
full unit baseline
real smoke result where applicable
```

Do not wait for ordinary user confirmation between J tasks.

Stop only for:

```text
destructive production data operations
irreversible database migration
real NAS deletion/overwrite
missing credentials/infrastructure
major unresolved architecture conflict
```

Start now from J0.
