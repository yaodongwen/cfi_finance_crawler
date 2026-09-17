# TossInvest Legacy Migration Plan

> Source repository:
>
> `https://github.com/yaodongwen/toss_nvest_crawl`
>
> Target repository:
>
> `https://github.com/yaodongwen/cfi_finance_crawler`
>
> Purpose:
>
> migrate all proven TossInvest site-specific behavior into the generic
> `cfi_finance_crawler` framework without reintroducing the old project's
> site-owned storage, dedup, upload, checkpoint, or production-runner architecture.

---

# 0. Current Reality

## J0 audit result (2026-09-07)

The current target and local legacy checkout were inspected before Phase J
implementation. Classification at the J0 boundary:

```text
A real production-wired  builtin registration; generic durable runtime/storage/query
B fixture-backed only    TossInvestPlugin.crawl(); runtime architecture smokes
C interface only         symbol mapping; forum/news normalization and relations
D missing                Playwright transport; real discovery/forum/news; rollout
```

The audited legacy traceability source is the local
`crawl_tossinvest/tossinvest_crawler_v2` checkout. The functions listed in
Sections 3-12 were found there and remain the behavior source for J1-J10.
Legacy storage and orchestration remain explicitly superseded by the generic
framework components listed below.

The target framework already has a TossInvest plugin, but it is currently an
architecture proof rather than a real TossInvest production crawler.

Current target files:

```text
src/crawl_framework/sites/tossinvest/
├── __init__.py
├── comments.py      # currently empty
├── news.py          # currently empty
├── plugin.py        # fixture-backed architecture proof
└── site.yaml
```

Current `TossInvestPlugin` already proves:

```text
forum_post canonical normalization
news_article canonical normalization
XKRX instrument mapping
generic runtime compatibility
generic Parquet compatibility
generic Catalog/index compatibility
generic query compatibility
```

But `crawl()` currently reads fixture data from:

```text
ctx.extra["tossinvest_raw"]
```

instead of accessing real TossInvest.

The generic Playwright transport file is also currently empty:

```text
src/crawl_framework/transports/playwright.py
```

Therefore the Toss migration should focus on:

```text
real Playwright transport
real Toss instrument discovery
real Toss community/forum crawler
real Toss news list/detail crawler
legacy field parity
full/incremental semantics
browser worker/proxy lifecycle
production rollout
```

The following framework components MUST remain authoritative and reusable:

```text
CanonicalRecord
RecordRelation
SitePlugin / SiteAdapter contract
SeenStore
checkpoint
ConcurrentProductionRuntime
RecordBuffer
ParquetWriter
PostgreSQL Catalog
record index
Uploader
RecoveryStore / RecoveryManager
Cleaner
Compaction
Query Layer
ProxyPool
run manifest / completeness audit
```

---

# 1. Migration Rule — Migrate Site Knowledge, Not Old Infrastructure

## Migrate from the old repository

The old project contains proven Toss-specific knowledge:

```text
Screener URL
DOM selectors
virtual-list scrolling
domestic-market switching
filter removal
stock-row parsing
stock_key semantics
community-tab navigation
community "latest" sorting
forum card parsing
shareholder badge parsing
follower count parsing
relative-time behavior
news list discovery
news_id parsing
news virtual-list scrolling
news baseline/full-history behavior
news pending/resume behavior
news detail extraction
structured JSON fallback parsing
title/publisher/date/author provenance
browser timeout behavior
browser profile isolation
proxy use
incremental stop heuristics
```

These are valuable and should be migrated.

## Do NOT migrate as authoritative architecture

Do not restore:

```text
output/<stock>/comments/comments.jsonl as primary storage
output/<stock>/news/news.jsonl as primary storage
state/crawler/<stock>/comment_ids/*.txt as primary SeenStore
state/crawler/<stock>/news_ids/*.txt as primary SeenStore
_pending_news_links.json as the primary durable recovery mechanism
_news_state.json as the primary checkpoint system
state/production_runner/state.json as the framework task state
normalizer.py as a separate mandatory spool step
dedup.py as a separate site-owned production layer
parquet_writer.py as a Toss-specific writer
postgres.py Toss-specific registry as the new framework Catalog
storage_sync.py as the authoritative pipeline
production_runner.py subprocess orchestration as the new runtime
one serial storage worker because old dedup/parquet state was shared
```

The new framework already solves these generically.

---

# 2. Old Project Source Map

Primary migration sources:

```text
toss_nvest_crawl/
├── crawler.py
├── collect_stocks.py
├── normalizer.py
├── cache_manager.py
├── production_runner.py
├── main.py
├── config.py
├── stock_worker.py
├── dedup.py
├── parquet_writer.py
├── postgres.py
├── uploader.py
├── cleaner.py
├── storage_sync.py
└── README.md
```

The user specifically wants the following files mapped in detail:

```text
crawler.py
collect_stocks.py
normalizer.py
cache_manager.py
production_runner.py
```

Supporting files may be read for behavioral verification, but those five are
the primary source map.

---

# 3. Old crawler.py — Function-Level Migration Map

## 3.1 Shared helpers

Old functions:

```text
utc_now()
human_sleep()
clean_text()
parse_news_id()
stock_key_from_url()
find_scroll_parent()
scroll_element_once()
```

### New targets

```text
src/crawl_framework/sites/tossinvest/common.py
src/crawl_framework/sites/tossinvest/news.py
src/crawl_framework/transports/playwright.py
```

### Migration notes

`parse_news_id()` is important Toss-specific identity logic:

- parse `contentParams` query parameter;
- URL-decode JSON;
- read `id`;
- fallback to a stable hash of URL.

Preserve the real Toss identifier when available.

`find_scroll_parent()` and `scroll_element_once()` are generic Playwright
virtual-list helpers and should preferably move into generic Playwright transport
utilities if they are site-neutral.

Do not bury them in production CLI code.

---

## 3.2 Browser/page preparation

Old class:

```text
TossInvestCrawler
```

Old methods:

```text
_prepare_page()
save_debug_html()
```

### Behavior to preserve

```text
default page timeout
default navigation timeout
optional debug HTML
```

### New target

Generic:

```text
src/crawl_framework/transports/playwright.py
```

Toss-specific debug naming may remain under:

```text
src/crawl_framework/sites/tossinvest/
```

The generic browser layer should own:

```text
Browser/Context lifecycle
page acquisition
timeouts
profile lifecycle
proxy assignment
browser crash recovery
context recycle
```

---

## 3.3 Screener readiness

Old method:

```text
_wait_screener_ready()
```

### Proven behavior

Do not use a blind fixed sleep.

The old project waits until either:

```text
real stock links appear
or
Skeleton disappears and real filter buttons appear
```

Important selectors/conditions include:

```text
a[href*="/stocks/"][href*="/order"]
[data-tds-wts-skeleton-box]
button[data-tossinvest-log="ModifyFilterChip"]
```

### New target

```text
TossInvestInstrumentClient
```

This is site-specific readiness logic.

---

## 3.4 Switch Screener to domestic market

Old method:

```text
_switch_screener_to_domestic()
```

### Proven behavior

The Screener may open on overseas market.

The old crawler:

```text
find country/market filter
open it
select 국내
click 보기
verify button changed to 국내
fallback verify KOSPI/KOSDAQ marker
```

Important selectors/labels include:

```text
button[data-parent-name="국가_CONDITION"]
[data-content-value="해외"]
[data-content-value="국내"]
ListRow
보기
KOSPI
KOSDAQ
```

### Migration target

```text
TossInvestInstrumentClient._switch_to_domestic()
```

Do not put Korean UI strings into generic Playwright transport.

---

## 3.5 Remove unwanted Screener filters

Old methods:

```text
_get_real_screener_filter_chips()
_remove_one_screener_filter()
_remove_all_screener_filters()
_prepare_screener()
```

### Critical proven behavior

The old code protects baseline filters:

```text
국내 / 해외
시장
산업 / 업종
시가총액
```

and removes other applied:

```text
button[data-parent-name="FilterPopover"]
```

filters by opening a chip and selecting:

```text
필터제거
```

The old project explicitly added safety guards so it would never delete
baseline market filters.

### New target

Keep this entire behavior in:

```text
TossInvestInstrumentClient
```

with parser/UI fixture tests where possible.

---

## 3.6 Extract visible Screener stocks

Old method:

```text
_extract_visible_screener_stocks()
```

### Proven behavior

Do not iterate every stock link because one stock row can contain duplicate links.

Use one row as one stock:

```text
div[role="row"][data-row-id]
```

Important fields:

```text
stock_key
stock_code
stock_name
country_market
market
stock_url
row_text
```

For Korean domestic stock:

```text
stock_key = A005930
stock_code = 005930
canonical = XKRX:005930
```

`stock_name` is taken from:

```text
data-contents-label
```

### New target

Canonical dataset:

```text
instrument
```

Recommended payload retention:

```text
toss_stock_key
stock_name
stock_url
row_text
country_market
market if source actually provides it
```

Do not guess KOSPI/KOSDAQ if Toss does not explicitly provide it.

---

## 3.7 Full Screener universe discovery

Old method:

```text
collect_stock_links()
```

Old standalone wrapper:

```text
collect_stocks.py::collect()
```

### Proven behavior

```text
open /screener/3
prepare Screener
find grid
find virtual scroll parent
scroll
accumulate stocks by stock_key
stop only after stable rounds + actual bottom
```

Old validated universe:

```text
about 2458 domestic stocks
```

### New target

```text
TossInvestPlugin.discover("instrument")
or
TossInvestInstrumentClient.discover_instruments()
```

Persist a deterministic snapshot, for example:

```text
config/universes/tossinvest_kr_rollout_universe.txt
```

with canonical IDs:

```text
XKRX:005930
...
```

Also store metadata:

```text
source URL
generated_at
count
snapshot SHA256
```

---

# 4. Old collect_stocks.py — Migration Map

Old functions:

```text
utc_stamp()
normalize_proxy()
read_stock_count()
backup_existing()
collect()
main()
```

## Proven useful behavior

The old script:

```text
opens persistent Playwright context
passes optional proxy
calls crawler.collect_stock_links(...)
requires limit=None for full discovery
compares newly discovered count with previous count
warns if count below minimum_expected
backs up previous universe
restores prior better universe when current discovery looks suspicious
```

## New framework target

Generic rollout/universe snapshot layer should provide:

```text
snapshot generation
snapshot count
snapshot hash
previous snapshot backup
minimum expected count safety check
dry-run
```

Toss-specific code should only discover instruments.

## Do not copy

Do not use:

```text
output/_stocks.json
```

as the permanent authoritative format.

Prefer canonical snapshot + metadata.

---

# 5. Old Community / "Comments" — Function-Level Migration

## 5.1 Open community and sort newest

Old method:

```text
_open_community_and_sort_latest()
```

### Proven behavior

The crawler tries multiple Korean/Chinese labels for:

```text
커뮤니티 / 社区
```

and sort choices involving:

```text
최신
인기
투자
환영
```

Then selects:

```text
최신
```

### New target

```text
src/crawl_framework/sites/tossinvest/comments.py
```

Suggested class:

```text
TossInvestForumClient
```

---

## 5.2 Parse visible forum cards

Old method:

```text
_extract_visible_comment_cards()
```

### Proven root selector

```text
[data-post-anchor-id]
```

### Proven fields

```text
post_id
stock_key
stock_code
stock_name
country_market
market
stock_url
author
author_profile_image
created_at_text
created_at_raw
follower_count
is_shareholder
content
raw_text
like_count
comment_count
crawl_time
```

### Proven DOM knowledge

Author/time header:

```text
label[for^="header::image::"]
```

Shareholder marker:

```text
주주
股东
股東
```

Like button:

```text
[data-content-value="좋아요 버튼"]
```

Reply-count button:

```text
[data-content-value="댓글 펼치기 버튼"]
```

Profile image:

```text
button[data-content-value="프로필 이미지"] img
```

### Important content cleaning

The old parser removes UI/runtime noise, including Radix-generated CSS and
header lines.

Preserve the cleaning behavior.

---

## 5.3 Crawl forum history/incremental

Old method:

```text
crawl_comments()
```

### Old behavior

Full mode:

```text
no known post IDs
-> scroll until virtual list stops at bottom
```

Incremental mode:

```text
existing IDs exist
-> sort latest
-> scroll newest to history
-> stop after HISTORY_STOP_ROUNDS consecutive rounds with only known posts
```

Additional protections:

```text
seen_this_run
stable_rounds
actual scroll bottom
flush batches
```

### New mapping

Use:

```text
SeenStore.inspect()
checkpoint
ConcurrentProductionRuntime
```

Do NOT use old local `PersistentIdSet` as the authoritative cache.

The incremental stop heuristic itself is worth preserving.

Recommended current dataset:

```text
forum_post
```

---

# 6. Old normalizer.py — Field-Parity Migration Map

## 6.1 Utility functions

Old functions:

```text
clean_text()
as_int()
as_bool()
parse_iso_datetime()
to_iso_z()
normalize_date()
parse_relative_comment_time()
normalize_stock_code()
common_fields()
```

These are useful as semantic references.

Some may belong in site-specific normalization helpers rather than generic core.

---

## 6.2 Forum normalization

Old function:

```text
normalize_comment()
```

### Old normalized fields

```text
normalized_schema_version
source
record_type
source_id
source_url
stock_key
stock_code
stock_name
country_market
market
stock_url
crawl_time

post_id
author
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
profile_image
source_schema_version
```

### Current new-framework parity requirement

The current Toss fixture plugin loses several of these fields.

The migrated `forum_post` must preserve at least in canonical columns or payload:

```text
post_id
author
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
profile_image
stock_name
stock_url
toss stock_key
```

Do not silently drop fields that the legacy crawler already captured.

---

## 6.3 Relative forum timestamps

Old function:

```text
parse_relative_comment_time()
```

Supported examples:

```text
방금
N초
N분
N시간
N일
```

and multilingual fallbacks.

### New rule

If only relative time is available:

```text
event_time = crawl_time - relative delta
payload.created_at_quality = approximate_relative
```

Do not pretend the timestamp is exact.

Preserve:

```text
created_at_text
created_at_raw
created_at_quality
```

for auditability.

---

## 6.4 News normalization

Old function:

```text
normalize_news()
```

### Fields

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

stock_key
stock_code
stock_name
stock_url
source_schema_version
```

### New-framework requirement

Do not reduce this to only:

```text
title
content
publisher
symbols
```

Retain the provenance/quality fields in payload:

```text
title_available
title_source
publisher_source
published_at_source
author_source
list_text
raw_text
```

These were deliberately added by the legacy project for metadata audit.

---

# 7. Old News List — Function-Level Migration

Old helper:

```text
parse_news_id()
```

Old method:

```text
collect_news_links()
```

## Proven news URL construction

The old crawler derives a stock news URL from:

```text
stock_url
-> replace final segment with /news
```

## Proven list root

```text
[data-list-name="NewsContentResolved"]
```

## Proven link selectors

```text
a[data-tossinvest-log="NewsDetailModalLink"]
a[href*="contentType=news"][href*="contentParams"]
```

## Important full-vs-incremental behavior

The old project distinguishes two phases:

### Historical baseline incomplete

```text
baseline_complete=False
```

Behavior:

```text
must scroll to the true end
known news IDs are skipped for body fetch
but encountering known IDs does NOT stop history discovery
```

This prevents the classic bug:

```text
newest 100 already crawled
old 9000 never crawled
crawler sees old IDs near top
incorrectly stops and permanently misses history
```

### Baseline complete

```text
baseline_complete=True
```

Then real incremental behavior is allowed:

```text
scroll newest downward
stop after multiple rounds containing only known news IDs
```

This behavior is critical and MUST be migrated into generic checkpoint semantics.

---

# 8. Old News Detail — Function-Level Migration

Old method:

```text
parse_news_article()
```

The old project calls this:

```text
metadata v4
```

## Extraction priority

```text
1. page JSON state
   __NEXT_DATA__
   application/json
   JSON-LD

2. real article DOM

3. list_text

4. metadata fallbacks

5. publisher/date/author auditable fallback
```

Core principle from old project:

```text
宁可留空，也不制造错误元数据
```

Preserve this principle.

## Important quality/provenance fields

```text
title_source
publisher_source
published_at_source
author_source
```

## Structured JSON keys considered

Identity:

```text
id
newsId
news_id
articleId
article_id
contentId
content_id
```

Title:

```text
title
headline
newsTitle
articleTitle
contentTitle
subject
```

Publisher:

```text
publisher
press
pressName
media
mediaName
provider
providerName
source
sourceName
officeName
```

Date:

```text
publishedAt
published_at
publishDate
publishedDate
datePublished
createdAt
created_at
articleDate
```

Author:

```text
author
authorName
reporter
reporterName
writer
writerName
journalist
```

## Important cleaning

Remove Toss-specific appended sections such as:

```text
관련 주식
관련 종목
相关股票
주요 뉴스
```

Do not include these UI blocks in article body.

---

# 9. Old News Full/Resume State — Migration Map

Relevant old behavior exists across:

```text
crawler.py::collect_news_links()
crawler.py::crawl_news()
cache_manager.py
```

## Old control files

```text
_pending_news_links.json
_news_state.json
_meta.json
```

## Important old states

```text
baseline_complete
pending news links
failed items
known news IDs
```

## Old crawl_news behavior worth preserving

```text
if pending links exist:
    resume pending first

discover links according to baseline state

skip known IDs

fetch detail

periodically persist progress

failed items stay pending

only mark baseline_complete=True when no remaining pending items exist
```

## New-framework mapping

Map old semantics into:

```text
checkpoint state
SeenStore
RecoveryStore
run manifest
```

Recommended checkpoint fields may include:

```text
baseline_complete
discovery_position / scroll state if practical
historical_backfill_complete
```

Pending individual news detail failures should become generic retryable recovery tasks,
not a site-specific JSON file.

---

# 10. Old cache_manager.py — Function-Level Migration Map

Old helpers/classes:

```text
clean_id()
digest_id()
PersistentIdSet
iter_jsonl_ids()
bootstrap_from_jsonl()
stock_state_root()
migrate_stock()
discover_stocks()
show_status()
```

## 10.1 PersistentIdSet

Old behavior:

```text
SHA1 IDs
256 shards by first two hash characters
lazy shard load
append + fsync
```

This was designed to avoid loading millions of IDs into memory.

### New framework mapping

Do not use it as production SeenStore.

The generic SeenStore already owns logical identity/version state.

However this old structure is useful for:

```text
legacy data migration
performance comparison
historical ID import
```

If existing Toss legacy history must be imported, add a one-time migration script:

```text
scripts/migrate_tossinvest_legacy_seen.py
```

that reads old ID shards / JSONL and translates legacy records into new canonical identity.

Do not make the new crawler depend on old state directories.

---

## 10.2 bootstrap_from_jsonl()

Use only for migration of existing old Toss history.

Do not bootstrap normal production runs from JSONL.

---

## 10.3 migrate_stock()

Old code migrates:

```text
comments JSONL -> comment ID cache
news JSONL -> news ID cache
old news control files -> state/crawler
```

### New target

If the user needs to retain old data/history:

create a dedicated migration path:

```text
legacy JSONL
-> old normalizer semantics
-> CanonicalRecord
-> generic durable pipeline / import pipeline

old ID/control state
-> new SeenStore/checkpoint migration
```

This should be an explicit one-time migration tool, not normal crawl behavior.

---

# 11. Old production_runner.py — Function-Level Migration Map

Old functions/classes include:

```text
utc_now()
atomic_write_json()
load_json()
normalize_stock_key()
load_stock_keys()
RunnerState
stream_subprocess()
storage_cmd()
spool_cmd()
sync_one()
crawl_worker()
storage_worker()
classify_resume()
```

## 11.1 What the old runner proves

The old project already recognized:

```text
crawler must run concurrently
worker profile directories must be isolated
Ctrl+C must be recoverable
crawl and storage are separate stages
already-crawled stocks should resume storage without crawling again
failed crawl/storage states must be distinct
```

These semantics should be preserved.

## 11.2 What must NOT be copied

The old architecture used:

```text
crawl workers
-> subprocess stock_worker.py
-> sync queue
-> ONE serial storage worker
-> storage_sync.py subprocess
```

The README explicitly says storage was serial because the old:

```text
state/dedup
state/parquet
dedup/*.jsonl
```

were shared resources.

The new generic framework has already removed that constraint with staged,
bounded parallel production runtime.

Therefore do not reproduce:

```text
subprocess-per-stock
one storage worker
site-specific sync process
```

## 11.3 Resume classification

Old:

```text
classify_resume()
```

separates:

```text
stocks requiring crawl
stocks that already crawled and only need storage resume
```

### New mapping

This behavior should emerge from:

```text
checkpoint
RecoveryStore
durable manifests
SeenStore
```

rather than a separate Toss-only `RunnerState`.

---

# 12. Old main.py — Migration Map

Old CLI:

```text
--start-url
--limit-stocks
--stock-url
--comments-only
--news-only
--headless
```

Old orchestration:

```text
launch persistent Chromium
discover stocks
for each stock:
    crawl comments
    crawl news
```

### New target

Do not create another Toss main program.

Use the generic framework CLI.

Target examples:

```bash
python -m crawl_framework.cli.main crawl \
  --site tossinvest \
  --profile toss_incremental
```

and:

```bash
python -m crawl_framework.cli.main crawl \
  --site tossinvest \
  --profile toss_full
```

---

# 13. Phase J — Exact Work Order

Execute in this exact order.

---

# J0. Repository Audit + Legacy Traceability

## Goal

Establish the real baseline before changing Toss code.

## Inspect current target

```text
src/crawl_framework/sites/tossinvest/plugin.py
src/crawl_framework/sites/tossinvest/comments.py
src/crawl_framework/sites/tossinvest/news.py
src/crawl_framework/transports/playwright.py
src/crawl_framework/sites/builtin.py
tests/unit/test_tossinvest_plugin.py
tests/unit/test_tossinvest_architecture.py
```

## Inspect old sources

```text
crawler.py
collect_stocks.py
normalizer.py
cache_manager.py
production_runner.py
main.py
README.md
```

## Output

Create/update this file in repo:

```text
TossInvest_Legacy_Migration_Plan.md
```

and update:

```text
finished.md
problem.md
todo.md
```

## Acceptance

Codex must explicitly classify each Toss capability:

```text
implemented + real production-wired
fixture only
interface only
missing
```

Do not mark a feature complete because a fixture test exists.

---

# J1. Generic Playwright Transport / Browser Worker Pool

## Old references

```text
main.py
crawler.py::_prepare_page
crawler.py::save_debug_html
production_runner.py::crawl_worker
```

## New target

Implement generic:

```text
src/crawl_framework/transports/playwright.py
```

Suggested abstractions:

```text
PlaywrightTransportConfig
BrowserLease / BrowserContextLease
PlaywrightTransport
BrowserWorkerPool
```

Required responsibilities:

```text
start/stop Playwright
Chromium lifecycle
persistent or isolated context lifecycle
headless
locale ko-KR
timezone Asia/Seoul where requested
viewport
page/navigation timeout
proxy assignment
per-worker profile isolation
context recycling
browser crash cleanup
bounded browser concurrency
graceful shutdown
```

## Proxy integration

Reuse generic:

```text
ProxyPool
```

A browser worker should lease a proxy and report success/failure.

Do not copy old single `TOSS_PROXY_SERVER` limitation as the final architecture.

## Acceptance

Deterministic fake/mocked transport tests plus one small real browser smoke.

---

# J2. Real Toss Instrument Discovery

## Old references

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

## New target

Suggested file:

```text
src/crawl_framework/sites/tossinvest/instruments.py
```

Suggested class:

```text
TossInvestInstrumentClient
```

Required:

```text
https://www.tossinvest.com/screener/3
switch to domestic
remove extra filters safely
virtual-list scroll
stable-bottom detection
stock_key
stock_code
stock_name
stock_url
canonical XKRX ID
```

## Universe snapshot

Generate:

```text
config/universes/tossinvest_kr_rollout_universe.txt
```

Do not assume old 2458 count forever.

Use current discovery, but treat a severe drop from previous good snapshot as suspicious.

## Acceptance

```text
no duplicate canonical IDs
all Korean IDs valid
stable deterministic snapshot
real small/full discovery smoke
```

---

# J3. Real Toss Forum/Community Client

## Old references

```text
crawler.py::_open_community_and_sort_latest
crawler.py::_extract_visible_comment_cards
crawler.py::crawl_comments
```

## New target

```text
src/crawl_framework/sites/tossinvest/comments.py
```

Suggested:

```text
TossInvestForumClient
```

Required:

```text
open stock page
open community
sort latest
parse virtual cards
scroll
full history mode
incremental mode
stable bottom
history-only stop
```

Do not write JSONL.

Yield raw dicts to plugin/runtime.

---

# J4. Toss Forum Full Field Parity

## Old references

```text
normalizer.py::normalize_comment
normalizer.py::parse_relative_comment_time
crawler.py::_extract_visible_comment_cards
```

## Required canonical/payload fields

At minimum:

```text
source_id/post_id
instrument_id
author_name
event_time
content
source_url

created_at_quality
created_at_text
created_at_raw
follower_count
is_shareholder
raw_text
like_count
comment_count
profile_image
stock_name
stock_url
toss_stock_key
```

## Identity

Use real Toss `post_id` when present.

Hash fallback only if source ID is genuinely unavailable.

## Version policy

Decide whether engagement counters should change version hash.

Recommended:

```text
content/author/time = logical content
like_count/comment_count = dynamic metrics in payload
```

Do not make page/scroll position part of identity.

---

# J5. Real Toss News List Discovery

## Old references

```text
crawler.py::parse_news_id
crawler.py::collect_news_links
```

## New target

```text
src/crawl_framework/sites/tossinvest/news.py
```

Suggested:

```text
TossInvestNewsClient
```

Required:

```text
stock /news page
NewsContentResolved list
NewsDetailModalLink/contentParams discovery
news_id
news_url
list_text
virtual-list scroll
stable bottom
full baseline semantics
incremental history-only stop
```

## Critical semantic requirement

Do not confuse:

```text
"some known news seen"
```

with:

```text
"historical baseline complete"
```

Until baseline is complete, encountering old IDs must NOT stop deeper historical discovery.

---

# J6. Real Toss News Detail Parser

## Old reference

```text
crawler.py::parse_news_article
```

## Required extraction order

Preserve old proven priority:

```text
page structured JSON
-> metadata
-> article DOM/nearby heading
-> list_text
-> auditable fallback
```

Preserve principle:

```text
leave field empty rather than fabricate incorrect metadata
```

Required fields:

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
stock relation evidence
```

Add parser fixture tests for multiple Toss layouts.

---

# J7. News Article / Instrument Relation Modeling

## Goal

Avoid physical article duplication across stocks.

Use:

```text
news_article
news_instrument
```

If the same Toss news item is reachable from multiple stocks:

```text
1 article
N instrument relations
```

Do not store N full copies.

The current Toss fixture plugin's `RecordRelation` behavior is a useful starting point,
but production dataset wiring must be explicit and queryable.

---

# J8. Toss Full-History + Pending/Resume Semantics

## Old references

```text
crawler.py::collect_news_links
crawler.py::crawl_news
cache_manager.py::PersistentIdSet
cache_manager.py::bootstrap_from_jsonl
production_runner.py::classify_resume
```

## New mapping

Use:

```text
SeenStore
checkpoint
RecoveryStore
run manifest
```

Need to represent:

```text
historical baseline complete
incremental mode
pending detail failures
retryable news detail
crawl interruption
```

## Critical acceptance

Scenario:

```text
history baseline incomplete
newest 700 known
older 9000 still missing
```

Crawler must continue historical discovery and not stop because it encounters known IDs.

After baseline complete, incremental mode may stop after consecutive history-only rounds.

---

# J9. Browser Worker Pool + Proxy Pool Integration

## Old references

```text
production_runner.py::crawl_worker
main.py
collect_stocks.py::normalize_proxy
```

## New target

Different browser workers must use isolated browser contexts/profile dirs.

Configurable:

```text
browser_workers
headless
profile_root
context_recycle_after_scopes
page_timeout
navigation_timeout
proxy strategy
```

Reuse:

```text
ProxyPool
```

Recommended mapping:

```text
BrowserWorker 1 -> proxy endpoint 1
BrowserWorker 2 -> proxy endpoint 2
...
failure -> proxy cooldown
```

Do not launch an unbounded Chromium instance per record.

---

# J10. Dataset-Specific Concurrency / Backpressure

Toss is browser-heavy.

Support budgets such as:

```text
instrument discovery browser workers
forum browser workers
news list browser workers
news detail browser workers
writer workers
upload workers
catalog workers
```

All queues bounded.

Avoid nested unbounded executors.

The old runner proved crawler parallelism is useful, but the new runtime should keep:

```text
crawl
write
upload
catalog
```

overlapping through the generic staged pipeline.

---

# J11. Real 1-Stock Production Smoke

Use a stable liquid example such as canonical:

```text
XKRX:005930
```

but derive Toss source key through adapter:

```text
A005930
```

Validate:

```text
instrument discovery
forum
news
Parquet
upload
Catalog
record index
checkpoint
recovery
query
```

Compare sample fields against old crawler expectations.

No FakeClient acceptance.

---

# J12. 4-Stock Concurrent Smoke

Legacy README used a 4-stock concurrency test.

Run 4 real stocks through new framework.

Validate:

```text
browser workers overlap
proxy allocation
bounded queues
crawl/write/upload/catalog overlap
no browser-profile collisions
no duplicate logical records
recovery clean
```

---

# J13. 20-Stock Production Smoke

Run:

```text
forum
news
```

with safe resource budgets.

Validate:

```text
memory
browser count
CPU
proxy health
queue depths
throughput
failure rate
Catalog/index
query
```

---

# J14. 100-Stock Rollout

Use the same frozen Toss universe snapshot.

Validate:

```text
checkpoint completeness
recovery remaining_pending=0
news baseline behavior
forum incremental behavior
compaction needs
browser stability
```

---

# J15. Full Toss KR Universe Rollout

Use the full deterministic Toss snapshot generated by J2.

The old project previously observed around:

```text
2458 stocks
```

but do not hard-code that as current truth.

Run in stages:

```text
forum first
news after forum is stable
then combined profile
```

Acceptance:

```text
all scopes completed
missing checkpoints=0
pending recovery=0
Catalog/storage audit passes
query smoke passes
run manifest complete
```

---

# J16. One-Click Toss Profiles

Create:

```text
toss_incremental
toss_full
```

Optional explicit historical backfill profile:

```text
toss_historical_backfill
```

Recommended semantics:

## toss_incremental

```text
current deterministic universe
forum incremental
news incremental only where historical baseline complete
retry pending failures
generic upload/Catalog/index/recovery
```

## toss_full

Define clearly.

Recommended meaning:

```text
full market using safe configured crawl bounds
```

Do NOT overload it to mean all historical pages forever.

## toss_historical_backfill

```text
full historical forum/news traversal
baseline_complete tracking
resume/recovery
```

Target CLI:

```bash
python -m crawl_framework.cli.main crawl \
  --site tossinvest \
  --profile toss_incremental
```

---

# 14. Required New/Changed Files

Suggested target structure:

```text
src/crawl_framework/
├── transports/
│   └── playwright.py
│
└── sites/
    └── tossinvest/
        ├── __init__.py
        ├── common.py
        ├── instruments.py
        ├── comments.py
        ├── news.py
        ├── plugin.py
        └── site.yaml
```

Tests:

```text
tests/unit/test_playwright_transport.py
tests/unit/test_tossinvest_instruments.py
tests/unit/test_tossinvest_comments.py
tests/unit/test_tossinvest_news.py
tests/unit/test_tossinvest_plugin.py
tests/integration/test_tossinvest_production_runtime.py
```

Scripts/profiles where needed:

```text
scripts/build_tossinvest_rollout_universe.py
scripts/run_tossinvest_smoke_matrix.py
```

Avoid creating Toss-specific replacements for generic:

```text
Parquet
Catalog
Uploader
Recovery
Query
```

---

# 15. Field-Parity Checklist

## Instrument

```text
stock_key
stock_code
stock_name
country_market
market
stock_url
row_text
canonical instrument_id
```

## Forum

```text
post_id
author
author_profile_image/profile_image
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
toss_stock_key
```

## News

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

Do not mark Toss full migration complete while these old proven fields are silently lost.

---

# 16. Legacy-State Migration

If old Toss data/state must be preserved, implement explicit migration tools.

Potential tools:

```text
scripts/import_tossinvest_legacy_jsonl.py
scripts/migrate_tossinvest_legacy_seen.py
scripts/migrate_tossinvest_legacy_news_state.py
```

Goals:

```text
old JSONL -> CanonicalRecord -> new durable storage
old known IDs -> SeenStore where safely derivable
old baseline_complete -> checkpoint state
old pending news -> RecoveryStore / retry tasks
```

Do not mix migration code into normal production crawl.

---

# 17. Required Codex Completion Report per J Item

For every J0-J16 completion, Codex must report:

```text
1. legacy files/functions reviewed
2. exact behavior migrated
3. old behavior deliberately not copied
4. new files/classes/functions
5. focused tests
6. full tests/unit baseline
7. real smoke result if applicable
8. finished.md changes
9. problem.md changes
10. todo.md changes
```

A J item is not complete solely because:

```text
class exists
FakeClient test passes
fixture normalizer passes
```

For real site features, acceptance requires:

```text
real TossInvest Playwright/HTTP behavior
production plugin wiring
generic durable pipeline
real smoke
```

---

# 18. Definition of TossInvest Full Migration Complete

Do not mark Phase J complete until all are true:

```text
real Toss Screener discovery works
real current KR universe snapshot exists
real forum/community crawl works
all legacy forum fields preserved
real news list crawl works
real news detail parser works
all important news metadata provenance preserved
news_article/news_instrument relation model works
full-history baseline semantics work
incremental mode works
pending/recovery behavior works
generic Playwright transport is production-wired
proxy pool works with browser workers
browser workers are bounded
generic concurrent pipeline handles Toss
Parquet/upload/Catalog/index/recovery/query are reused
1-stock real smoke passes
4-stock concurrent smoke passes
20-stock smoke passes
100-stock rollout passes
full universe rollout passes
toss_incremental exists
toss_full exists
historical backfill semantics are explicit
```

---

# 19. Non-Negotiable Architecture Boundary

After Phase J, adding future Playwright-heavy sites should reuse:

```text
PlaywrightTransport
BrowserWorkerPool
ProxyPool
ConcurrentProductionRuntime
StoragePipeline
Uploader
Catalog
RecordIndex
Checkpoint
Recovery
Query
```

A future site should primarily implement:

```text
instrument discovery
task discovery
page navigation
DOM/API parsing
normalize
site-specific checkpoint semantics
```

If Toss migration requires copying generic storage/upload/index logic into
`tossinvest/`, stop and redesign before proceeding.
