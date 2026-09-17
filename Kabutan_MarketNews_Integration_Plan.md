# Kabutan MarketNews Integration Plan

## 0. Free/public scope amendment (authoritative from 2026-09-11)

Phase K covers only news available without authentication. Premium login,
cookies, `storage_state`, hidden-ID guessing, paid archive access, and the
2013-09-to-current completeness claim are explicitly out of scope.

An unlinked timed row such as `span.fin_modal.vtlink` is the normal public
archive boundary. It produces `free_access_complete=true`,
`month_complete=false`, `archive_access_validated=false`, and
`stop_reason=free_access_boundary`. It is not an empty page or failure.

> Source/reference implementation: `get_all.py`
>
> Target repository: `https://github.com/yaodongwen/cfi_finance_crawler`
>
> Target site: `kabutan`
>
> Target dataset: `news_article`

## 1. Current source behavior

The supplied crawler proves these Kabutan-specific behaviors:

```text
BASE_URL=https://kabutan.jp
LIST_URL=https://kabutan.jp/news/marketnews/
history starts at 2013-09
monthly discovery with category=-1, date=YYYYMM00, page=N
list parser: table.s_news_list
news_id regex: n\d{12}
detail parser: article / h1 / time.s_news_date / div.body
pagination stop: empty page, repeated/fully-seen IDs, max pages
HTTP retries: 429/500/502/503/504
```

Only migrate site-specific behavior. Do not migrate JSONL/failed.jsonl as production architecture.

## 2. Target architecture

```text
site=kabutan
country=JP
timezone=Asia/Tokyo
dataset=news_article
scope_type=month
scope_id=YYYY-MM
source_key=YYYYMM
```

Production flow:

```text
KabutanPlugin.discover(news_article)
-> monthly CrawlScope
-> KabutanMarketNewsClient.crawl_month()
-> raw dict
-> KabutanPlugin.normalize()
-> CanonicalRecord(news_article)
-> SeenStore/checkpoint
-> ConcurrentProductionRuntime
-> Parquet
-> upload
-> PostgreSQL Catalog/index
-> recovery
-> query
```

Do not invent fake instrument IDs. Initial Kabutan integration is global/month-scoped.

## 3. Recommended files

```text
src/crawl_framework/sites/kabutan/
├── __init__.py
├── market_news.py
├── plugin.py
└── site.yaml
```

Register in:

```text
src/crawl_framework/sites/builtin.py
```

Constants:

```text
KABUTAN_SITE_ID="kabutan"
KABUTAN_COUNTRY="JP"
KABUTAN_TIMEZONE="Asia/Tokyo"
```

## 4. Legacy function mapping

### create_session / get_html

Preserve:
```text
Japanese headers
Referer
timeout=20
retry 429/500/502/503/504
```

Prefer existing generic HTTP / limiter / proxy abstractions when compatible.

### clean_text

Preserve:
```text
NBSP cleanup
Japanese full-width space cleanup
line breaks
horizontal whitespace collapse
```

### extract_news_id

Legacy:
```python
NEWS_ID_RE = re.compile(r"(n\d{12})")
```

Stable canonical source_id must be the real Kabutan news ID. Month/page must not affect identity.

### parse_list_page

Preserve:
```text
table.s_news_list
tbody > tr
td.news_time
first href matching news ID
urljoin(BASE_URL, href)
title
second td category when plausible
dedup by news_id
```

### parse_article_page

Preserve:
```text
article
article h1
time.s_news_date
fallback time[datetime]
div.body
remove script/style/iframe/.ads_box/.ad/.sns/.related
```

Canonical fields:
```text
source_id=news_id
title
event_time=published_at
source_url=url
content
payload.category
payload.list_time
payload.source_section=marketnews
instrument_id=None
```

Do not fabricate published_at from list_time unless a deterministic source rule is proven.

## 5. Monthly scopes

Legacy URL range (not a free-access completeness target):
```text
2013-09 -> current month
```

The production free-full profile starts at the current month and discovers
older month scopes only until the first public/Premium boundary.

Example:
```python
CrawlScope(
    scope_type="month",
    scope_id="2026-09",
    source_key="202609",
    metadata={"year": 2026, "month": 9},
)
```

Benefits:
```text
safe resume
per-month checkpoint
natural historical completeness audit
parallel months at low concurrency
```

## 6. Pagination safety

Migrate legacy protection and strengthen it:

```python
seen_page_signatures = set()
seen_month_ids = set()

items = parse_list_page(html)

if not items:
    stop_reason = "empty_page"
    break

signature = tuple(item["news_id"] for item in items)

if signature in seen_page_signatures:
    stop_reason = "repeated_page_signature"
    break

current_ids = {item["news_id"] for item in items}

if current_ids and current_ids.issubset(seen_month_ids):
    stop_reason = "repeated_page_ids"
    break

seen_page_signatures.add(signature)
seen_month_ids.update(current_ids)
```

Repeat detection must happen before article detail fetches.

Retain a configurable `max_pages_per_month` safety guard.

## 7. Checkpoint semantics

Recommended month checkpoint:
```text
year
month
last_completed_page
last_news_id
month_complete
stop_reason
```

Checkpoint advances only after the page's records pass the generic durable barrier.

Mark `month_complete=true` only on natural end:
```text
empty_page
repeated_page_signature
repeated_page_ids
```

Do NOT mark complete on:
```text
HTTP failure
parser failure
worker cancellation
max_pages safety cap
```

A safety-capped month should be marked partial, not complete.

## 8. Full vs incremental

Create:
```text
kabutan_incremental
kabutan_free_full
```

Recommended:
```text
kabutan_incremental:
  current month + previous month overlap

kabutan_free_full:
  current month -> older months
  stop normally at the first free-access boundary
```

`kabutan_full` remains a compatibility alias for `kabutan_free_full`; it does
not mean complete Premium history.

Optional explicit:
```text
kabutan_historical_backfill
```

Suggested CLI overrides:
```text
--kabutan-start-month YYYY-MM
--kabutan-end-month YYYY-MM
--kabutan-overlap-months N
--kabutan-max-pages-per-month N
```

## 9. Failure/recovery

Do not migrate:
```text
marketnews.jsonl
failed.jsonl
load_existing_ids()
save_news()
save_failed()
```

Use:
```text
SeenStore
checkpoint
RecoveryStore
run manifest
```

List-page failure must not advance checkpoint past that page.

## 10. Concurrency

Kabutan is HTTP/HTML, not Playwright-heavy.

Start conservatively:
```text
crawl_workers=2-4
http_concurrency=4-8
```

Within each month, keep list pages ordered. Detail concurrency may be added only if bounded and checkpoint-safe.

## 11. Relations

Initial Phase K:
```text
news_article only
instrument_id=None
relations=[]
```

The supplied source does not prove stock relations. Do not infer them from article text.

Future `news_instrument` can be added only after source-derived stock relation evidence is verified.

## 12. Required tests

Create:
```text
tests/unit/test_kabutan_market_news.py
tests/unit/test_kabutan_plugin.py
```

Update:
```text
tests/unit/test_builtin_sites.py
tests/unit/test_cli_main.py
```

Must cover:
```text
list parsing
duplicate table dedup
article title/body/date
body cleaning
missing article/body
empty-page stop
repeated signature stop
seen-ID subset stop
repeat stop before detail fetch
max-page cap => partial
2013-09 first month
year boundary
dynamic current month
incremental overlap
same news ID on different page/month => same identity
checkpoint only after durable success
```

## 13. Real smoke sequence

```text
K10 current month page 1 list + one detail
K11 free-access durable production acceptance
K12 free-access interruption/resume/dedup acceptance
K13 free-access production rollout to the public boundary
K14 `kabutan_incremental` / `kabutan_free_full` profile acceptance
```

## 14. Phase K work order

```text
K0  audit framework + get_all.py
K1  add Kabutan site package + builtin registration
K2  list parser + stable ID
K3  article detail parser
K4  month scopes
K5  pagination/repeated-page protection
K6  real HTTP production client wiring
K7  checkpoint/recovery semantics
K8  CLI/profile options
K9  run manifest/completeness audit
K10 real read-only HTTP smoke
K11 free-access durable production acceptance
K12 free-access interruption/resume/dedup acceptance
K13 free-access production rollout
K14 free-access profile acceptance
```

## 15. Definition of complete

Do not mark Kabutan complete until:
```text
real list HTTP
real detail HTTP
stable news ID identity
monthly scopes
repeat-page protection
checkpoint/recovery
production plugin registration
generic durable pipeline
one-month smoke
three-month resume/dedup
free-access rollout to the public boundary
kabutan_incremental
kabutan_free_full
query smoke
remaining_pending=0
```

Premium archive history is not a completion condition.
