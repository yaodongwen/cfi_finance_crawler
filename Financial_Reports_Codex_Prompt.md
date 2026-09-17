# Codex Prompt — Phase L Financial Reports / Filings Integration

Target repository:
https://github.com/yaodongwen/cfi_finance_crawler

Reference implementation supplied by user:
get_all_finance_report.py

New roadmap:
Phase L — Financial Reports / Filings Integration

The purpose is NOT merely to copy one HKEX PDF downloader.

Build a reusable financial-report/filing abstraction so future sources
(EDINET/MOPS/SGX/SEC/European exchanges) can reuse the same metadata,
attachment, storage, recovery and query architecture.

First read:

- get_all_finance_report.py
- todo.md
- problem.md
- finished.md
- project_status.md
- current dataset/schema definitions
- current attachment implementation
- current CanonicalRecord / RecordRelation
- current SitePlugin interfaces
- current query layer
- current run-manifest/completeness code

Then inspect the current repository before editing.

Do not alter or erase Kabutan Phase K state.
K13/K14 may remain blocked by remote WAF and are independent from Phase L.

Execute Phase L in order:

L0 repository audit + legacy traceability
L1 generic financial_report dataset/schema
L2 HKEX site package + builtin registration
L3 HKEX stockId exact-resolution client
L4 HKEX report-search client
L5 HKEX result parser + taxonomy
L6 release-time normalization
L7 financial_report canonical normalization
L8 financial_report -> attachment integration
L9 checkpoint/recovery semantics
L10 CLI/options/profiles
L11 deterministic parser/unit matrix
L12 one-instrument real HTTP smoke
L13 one-instrument durable metadata+PDF smoke
L14 10-instrument mixed report-type smoke
L15 interruption/resume/dedup smoke
L16 100-instrument rollout
L17 full configured HK universe rollout
L18 profile/query/completeness final acceptance

Core architecture rule:

Do NOT create HKEX-specific:

- Parquet writer
- PostgreSQL schema/pipeline
- uploader
- recovery system
- SeenStore
- query engine
- PDF storage subsystem
- JSON/CSV progress architecture

Reuse the generic framework.

Create/verify generic datasets:

financial_report
financial_report_instrument
attachment

`financial_report` is the logical filing metadata record.
`attachment` is the physical PDF.

For HKEX:

site_id=hkexnews
country=HK
timezone=Asia/Hong_Kong

canonical instrument IDs:
XHKG:00005
XHKG:00700
etc.

Preserve exact legacy source behavior:

1. Stock lookup:
GET https://www1.hkexnews.hk/search/prefix.do

params:
callback
lang=EN
type=A
name=<5 digit stock code>
market=SEHK

Never accept the first prefix suggestion blindly.
Normalize to 5 digits and require exact code match.

2. Report search:
POST https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=en

form:
lang=EN
category=0
market=SEHK
searchType=1
documentType=-1
t1code=40000
t2Gcode=-2
t2code=<report type code>
stockId=<resolved stockId>
from=<YYYYMMDD>
to=<YYYYMMDD>
MB-Daterange=0
title=

3. HKEX source report codes:

40100 -> annual
40200 -> interim
40300 -> quarterly

Keep original code in payload.

4. Result parser:

- parse real PDF anchors;
- canonicalize URL;
- dedup by canonical PDF URL;
- parse containing row;
- release_time;
- stock code/name;
- use full final-cell document text where anchor text is partial.

5. Stable identity:

Prefer proven stable HKEX document ID if available in real URL/path.

If no proven source document ID exists:

source_id = SHA256(canonical_pdf_url)

Do not put page/order/local path into logical identity.

6. Canonical `financial_report`:

top-level:
site_id
country
dataset
source_id
instrument_id
event_time
title
source_url
crawled_at
updated_at
record_uid
version_hash
relations_json
payload_json

HKEX payload at minimum:

report_type
report_type_code
release_time_raw
hkex_stock_id
hkex_stock_name
input_stock_name
pdf_url
source_section="financial_reports_esg"
document_language=null unless source-proven
fiscal_period_end=null unless source-proven
fiscal_year=null unless source-proven

Do not infer accounting period/fiscal year from title without explicit source evidence.

7. Release time:

support source-observed forms including:

YYYY年M月D日 HH:MM
DD/MM/YYYY HH:MM

Normalize using Asia/Hong_Kong.

If exact time cannot be reliably parsed:
event_time=null
preserve release_time_raw

Do not invent midnight.

8. PDF handling:

Do not copy `download_pdf()` as a new HKEX-specific pipeline.

Use existing generic attachment layer.

Preserve/ensure these safety properties generically:

streaming download
temporary file
atomic finalization
minimum-size sanity
%PDF- magic
reject HTTP-200 HTML/error pages
SHA256
dedup
deterministic storage path
upload
remote verification
attachment metadata/index
recovery

9. Scope/checkpoint:

Use instrument scope:
scope_type=instrument
scope_id=XHKG:NNNNN

Do not migrate:
progress_annual.json
progress_all.json
metadata_financial_reports.csv
failed.csv
load_completed_pdf_urls()

Use:
SeenStore
checkpoint
RecoveryStore
run manifest
universe snapshot/hash

10. Profiles:

hkex_reports_incremental
hkex_reports_full

Incremental:
recent configurable date lookback
annual+interim+quarterly
SeenStore dedup

Full:
configured HK universe
default source search start 1999-04-01 through current date
annual+interim+quarterly
resume from durable state

Recommended CLI overrides:

--report-type annual|interim|quarterly|all
--report-date-from YYYY-MM-DD
--report-date-to YYYY-MM-DD
--download-report-pdf / --no-download-report-pdf
--instruments-file
--instrument

Do not create interactive CLI prompts in production.

11. Universe:

The legacy CSV with Chinese columns is a reference input only.

Create a one-time importer if needed.

Production should use canonical instruments file/snapshot.

Manifest must include universe count + SHA256.

12. Query acceptance:

Must support at least functionally:

reports by instrument
reports by release-date range
reports by report_type
attachments belonging to report
latest annual report
latest interim report

Do not redesign Catalog solely for report_type pushdown unless current query architecture requires it.

13. Tests:

At minimum cover:

stock code normalization
exact stockId match against prefix collisions
JSONP parsing
40100/40200/40300 mapping
duplicate PDF URL dedup
partial anchor vs full row title
release-time formats
invalid release time
stable identity across reruns
HTML returned as PDF rejection
valid PDF acceptance
attachment SHA dedup
checkpoint after durable barrier
interruption/resume
same report rediscovered under multiple searches
no duplicate logical record

14. Real smoke matrix:

L12:
one instrument, metadata only, real HTTP

L13:
one instrument, at least one real PDF through durable pipeline

L14:
10 instruments with annual/interim/quarterly mix

L15:
interrupt + recovery-only + rerun

L16:
100-instrument production rollout

L17:
full configured HK universe

L18:
query + completeness + profile acceptance

Do not jump directly to full universe.

15. Status docs:

Append Phase L to todo.md.
Append current financial-report gaps to problem.md.
Do not delete older Naver/Toss/Kabutan history.
Do not mark features finished until real acceptance.

After each accepted L item:

- update todo.md status
- append proven facts to finished.md
- resolve/update corresponding problem
- update project_status.md current item
- run focused tests
- run:
  pytest -q -p no:rerunfailures tests/unit

Completion report must state:

legacy functions reviewed
new generic schema/API
HKEX-specific implementation
tests
real smoke
records/files/uploads/catalog jobs
checkpoint/recovery
NAS/Catalog audit
query smoke

Start from L0 now.
