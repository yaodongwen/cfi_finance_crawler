# Codex Prompt — Integrate Kabutan Market News as Phase K

Target repository:
```text
https://github.com/yaodongwen/cfi_finance_crawler
```

Reference crawler supplied by the user:
```text
get_all.py
```

Authoritative implementation plan:
```text
Kabutan_MarketNews_Integration_Plan.md
```

The goal is to migrate Kabutan Market News into the existing generic framework.

Do NOT copy the standalone crawler architecture wholesale.

Read first:
```text
Kabutan_MarketNews_Integration_Plan.md
get_all.py
finished.md
problem.md
todo.md
project_status.md
```

Then inspect current repository code and tests.

Key architecture:
```text
site_id=kabutan
country=JP
timezone=Asia/Tokyo
dataset=news_article
scope_type=month
scope_id=YYYY-MM
```

Kabutan is a global/month-scoped news source. Do NOT invent instrument scopes.

Execute strictly in order:
```text
K0 audit
K1 site package + builtin registration
K2 list parser + stable news identity
K3 article detail parser
K4 month discovery/scopes
K5 pagination/repeated-page protection
K6 real HTTP production wiring
K7 checkpoint/recovery
K8 CLI/profiles
K9 manifest/completeness audit
K10 real read-only HTTP smoke
K11 one-month durable production smoke
K12 three-month resume/dedup smoke
K13 historical rollout
K14 one-click profile acceptance
```

Preserve from `get_all.py`:
```text
LIST_URL=https://kabutan.jp/news/marketnews/
category=-1
date=YYYYMM00
page=N
NEWS_ID_RE=n\d{12}
table.s_news_list
td.news_time
article link matching news ID
article
h1
time.s_news_date / time[datetime]
div.body
body cleanup
empty-page stop
repeated-page stop
max-pages safety guard
Japanese headers
retry 429/500/502/503/504
```

Stable identity:
```text
source_id = Kabutan news_id
month/page are NOT part of logical identity
instrument_id=None
```

Article fields:
```text
title
content
event_time=detail datetime
source_url
payload.category
payload.list_time
payload.source_section=marketnews
```

Do not fabricate timestamps from list_time when detail datetime is absent.

Pagination protection:
```text
seen_page_signatures
seen_month_ids
```

Stop if:
```text
empty page
exact repeated page signature
current page IDs are subset of IDs already seen in this month
```

Repeat detection MUST happen before detail fetching.

Checkpoint:
```text
last_completed_page
last_news_id
month_complete
stop_reason
```

Only mark month complete on natural terminal condition:
```text
empty_page
repeated_page_signature
repeated_page_ids
```

Do NOT mark complete for:
```text
HTTP failure
parser failure
interrupt
max_pages safety cap
```

Max-page cap means partial/safety-capped.

Do NOT migrate these as production state:
```text
kabutan_data/
marketnews.jsonl
failed.jsonl
load_existing_ids()
save_news()
save_failed()
hard-coded END_YEAR/END_MONTH
```

Use existing generic:
```text
CanonicalRecord
SeenStore
checkpoint
ConcurrentProductionRuntime
ParquetWriter
Uploader
PostgreSQL Catalog
record index
RecoveryStore
Cleaner
Compaction
Query
AdaptiveRateLimiter
ProxyPool/HttpTransport where compatible
run manifest
completeness audit
```

Profiles:
```text
kabutan_incremental
kabutan_full
```

Recommended incremental semantics:
```text
current month + one previous-month overlap
```

Recommended full semantics:
```text
2013-09 -> current month
resumable month scopes
```

Suggested CLI:
```text
--kabutan-start-month YYYY-MM
--kabutan-end-month YYYY-MM
--kabutan-overlap-months N
--kabutan-max-pages-per-month N
```

No `--instrument` should be required.

Testing must include:
```text
list parser
duplicate tables
article parser
datetime
body cleanup
missing body
empty-page stop
repeated signature
subset repeat
repeat detected before detail
max page partial state
2013-09 start
year transition
dynamic current month
incremental overlap
same news ID different page/month same record identity
checkpoint after durable barrier only
```

Real acceptance sequence:
```text
K10 current month page1 + one article detail
K11 one-month durable production smoke
K12 three-month resume/dedup
K13 historical 2013-09 -> current month
K14 profile acceptance
```

Status docs:
Do NOT delete `finished.md`, `problem.md`, `todo.md`, `project_status.md`.

Before coding:
- append Phase K to todo.md;
- append current Kabutan gaps to problem.md;
- do not claim Kabutan crawling in finished.md.

After each accepted K item:
- mark that K item DONE in todo.md;
- move only proven facts to finished.md;
- resolve corresponding problem entries;
- update project_status.md current item.

A site feature is complete only after:
```text
real HTTP
+ production plugin wiring
+ generic durable pipeline where applicable
+ real smoke
```

After each K item:
```text
focused tests
pytest -q -p no:rerunfailures tests/unit
```

For K13 historical rollout, print before running:
```text
start month
end month
expected month scopes
page cap policy
crawl/http worker counts
NAS target
PostgreSQL target
append-only status
```

Resume durable completed months. Do not restart history from scratch.

Do not pause for ordinary development steps.
Stop only for destructive production-data operations, irreversible DB migrations,
NAS delete/overwrite, unavailable infrastructure/credentials, or major unresolved architecture conflict.

Start now at K0.
