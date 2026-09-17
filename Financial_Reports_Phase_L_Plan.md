# Phase L — Financial Reports / Filings Integration Plan

> First real source: HKEXnews
> Reference implementation: get_all_finance_report.py
> Goal: build a reusable financial-report/filing layer, with HKEX as the first production adapter.

# 0. Architecture principle

Do not migrate the standalone HKEX script as a second storage system.

Migrate only HKEX-specific source knowledge:

- stock code -> HKEX stockId exact lookup;
- HKEX title-search request parameters;
- report category codes;
- search-result parsing;
- release-time parsing;
- PDF URL discovery;
- conservative retry/rate-limit behavior.

Reuse the current generic framework for:

- CanonicalRecord
- SeenStore
- checkpoint
- ConcurrentProductionRuntime
- Parquet
- generic attachment/PDF pipeline
- SHA256
- Uploader
- PostgreSQL Catalog
- record index
- RecoveryStore
- Query Layer
- run manifest
- completeness audit

# 1. New canonical datasets

Recommended:

```text
financial_report
financial_report_instrument
attachment
```

Do NOT put every report field only in `attachment`.

`financial_report` is the logical filing/report metadata record.

`attachment` is the physical PDF object.

One report metadata record may point to one or more attachments in future sources.

HKEX v1 may have one PDF attachment per report.

# 2. Canonical financial_report schema

Top-level canonical fields:

```text
site_id
country
dataset=financial_report
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
```

For HKEX:

```text
site_id=hkexnews
country=HK
instrument_id=XHKG:NNNNN
event_time=release_time converted to Asia/Hong_Kong aware UTC
title=HKEX document title
source_url=canonical PDF URL
```

Payload at minimum:

```json
{
  "report_type": "annual|interim|quarterly",
  "report_type_code": "40100|40200|40300",
  "release_time_raw": "...",
  "hkex_stock_id": 12345,
  "hkex_stock_name": "...",
  "input_stock_name": "...",
  "pdf_url": "...",
  "source_section": "financial_reports_esg",
  "document_language": null,
  "fiscal_period_end": null,
  "fiscal_year": null
}
```

Important:

- do not infer fiscal period end from title unless source-derived evidence is implemented;
- do not infer document language merely from the HKEX search UI language;
- unknown fields remain null.

# 3. Stable report identity

Preferred HKEX v1 identity:

```text
source_id = deterministic identity derived from canonical PDF URL
```

If a proven stable HKEX document identifier is present in the URL/path, parse and use it.

Otherwise:

```text
source_id = sha256(canonical_pdf_url)
```

Do not include:

```text
stock list position
crawl page
local path
release-date partition
```

in logical identity.

The same PDF discovered from multiple categories or reruns must remain one logical report.

# 4. Instrument model

HKEX input codes are 5 digits:

```text
00005
00700
01288
```

Canonical:

```text
XHKG:00005
XHKG:00700
XHKG:01288
```

Legacy exact stock lookup behavior must be preserved:

```text
GET /search/prefix.do
market=SEHK
type=A
name=<5 digit code>
```

Prefix results must be filtered by exact normalized code.

Do not choose the first autocomplete result.

# 5. HKEX report taxonomy

Legacy verified codes:

```text
40100 = annual
40200 = interim
40300 = quarterly
```

Target normalized values:

```text
annual
interim
quarterly
```

Keep original code in payload.

Future sources may map their own native categories into the same normalized taxonomy.

Generic taxonomy should be extensible, e.g.:

```text
annual
interim
quarterly
semiannual
earnings_release
financial_statements
amendment
other
```

But do not fabricate categories not present in HKEX.

# 6. HKEX search client

Recommended package:

```text
src/crawl_framework/sites/hkexnews/
├── __init__.py
├── filings.py
├── plugin.py
└── site.yaml
```

Real endpoints from legacy script:

```text
GET  https://www1.hkexnews.hk/search/prefix.do
POST https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=en
```

Search form fields to preserve:

```text
lang=EN
category=0
market=SEHK
searchType=1
documentType=-1
t1code=40000
t2Gcode=-2
t2code=<40100|40200|40300>
stockId=<resolved HKEX stockId>
from=<YYYYMMDD>
to=<YYYYMMDD>
MB-Daterange=0
title=
```

Date range must be configurable.

Default historical start for HKEX v1 may remain:

```text
1999-04-01
```

but real production completeness must be based on actual returned source data, not merely the configured start date.

# 7. Search-result parser

Migrate behavior from `parse_reports()`:

- find PDF anchors;
- canonicalize with urljoin(BASE, href);
- dedup same PDF URL;
- use parent `tr`;
- parse release time;
- source stock code/name;
- use full final-cell document text when anchor text is partial.

Do not treat every PDF link on the page as a report unless it is inside the expected result structure or backed by deterministic fixtures/real smoke.

# 8. Release time

Support verified formats:

```text
YYYY年M月D日 HH:MM
DD/MM/YYYY HH:MM
YYYYMMDD...
```

Use timezone:

```text
Asia/Hong_Kong
```

Canonical `event_time` must be timezone-aware.

Keep raw text in payload.

If time cannot be reliably parsed:

```text
event_time = null
release_time_raw preserved
```

Do not invent midnight.

# 9. PDF attachment model

Reuse current generic attachment system.

Flow:

```text
financial_report metadata
-> AttachmentRequest
-> generic HTTP download
-> PDF magic validation
-> SHA256
-> deterministic attachment path
-> upload
-> remote verify
-> attachment metadata/index
-> recovery
```

Do not create HKEX-specific PDF storage/upload code.

Preserve legacy safety knowledge:

```text
stream download
temporary file
atomic rename
minimum size sanity
%PDF- magic
reject HTML returned with HTTP 200
```

Generic attachment validation should own these where possible.

# 10. Local/remote paths

Do not preserve legacy:

```text
outputs/hkex_annual_reports/<stock>/...
metadata_financial_reports.csv
failed.csv
progress_*.json
```

as production architecture.

Use framework storage layout and Catalog.

Report metadata goes through normal Parquet.

PDF binary goes through generic attachment storage.

# 11. Checkpoint semantics

Recommended scope:

```text
scope_type=instrument
scope_id=XHKG:NNNNN
```

For each instrument, mode/report types and date range belong to crawl options, not identity.

Checkpoint should not advance until:

```text
report metadata durable
and attachment policy has reached its allowed durable state
```

If framework policy allows metadata durable while attachment retry remains pending,
that must be explicit and recovery-tracked.

Do not reproduce `next_index` progress files as authoritative state.

# 12. Incremental/full profiles

Recommended profiles:

```text
hkex_reports_incremental
hkex_reports_full
```

`hkex_reports_incremental`:

```text
current HK universe / supplied universe
recent configurable lookback window
annual + interim + quarterly
SeenStore dedup
```

`hkex_reports_full`:

```text
configured universe
1999-04-01 -> current date
annual + interim + quarterly
resume from generic durable state
```

Optional:

```text
hkex_annual_reports_full
```

only if operationally useful.

# 13. Input universe

Do not make production depend on a Chinese-column CSV forever.

Support:

```text
--instruments-file
--instrument
```

using canonical IDs.

A one-time importer may convert:

```text
outputs/hk_stocks_market_cap.csv
```

into:

```text
config/universes/hkex_...txt
```

Preserve stock list snapshot/hash in run manifest.

# 14. Query use cases

At minimum:

```text
reports by instrument
reports by release date range
reports by report_type
attachments for report
latest annual report for instrument
latest interim report for instrument
```

If generic query cannot push down payload.report_type yet,
first expose correct functional filtering; optimize later.

# 15. Phase L work order

```text
L0  repository audit + legacy traceability
L1  generic financial_report dataset/schema
L2  HKEX site package + builtin registration
L3  HKEX stockId exact-resolution client
L4  HKEX report-search client
L5  HKEX result parser + taxonomy
L6  release-time normalization
L7  financial_report canonical normalization
L8  financial_report -> attachment integration
L9  checkpoint/recovery semantics
L10 CLI/options/profiles
L11 deterministic parser/unit matrix
L12 one-instrument real HTTP smoke
L13 one-instrument durable metadata+PDF smoke
L14 10-instrument mixed report-type smoke
L15 resume/dedup/interruption smoke
L16 100-instrument rollout
L17 full configured HK universe rollout
L18 profile/query/completeness final acceptance
```

# 16. Real acceptance matrix

Use at least:

```text
annual
interim
quarterly
```

Across companies with different filing histories.

Do not accept only one annual-report sample as proof of all formats.

Validate:

```text
source identity
stockId resolution
release time
title
report type
PDF URL
PDF validity
SHA256
Parquet metadata
NAS upload
Catalog/index
SeenStore
checkpoint
recovery
query
```

# 17. Legacy functions mapping

```text
build_session
  -> generic HTTP transport/retry configuration

normalize_stock_code
  -> hkex canonical code helper / instrument mapping

load_stocks
  -> one-time universe importer or generic instruments file

parse_jsonp
  -> hkex helper

get_stock_info
  -> HKEXFilingsClient.resolve_stock()

search_reports_by_type
  -> HKEXFilingsClient.search_report_type()

search_financial_reports
  -> client aggregation over normalized report types

parse_reports
  -> deterministic HKEX result parser

get_release_date
  -> release timestamp parser (expanded to timezone-aware datetime)

build_local_path
  -> superseded by generic attachment path

is_valid_existing_pdf
download_pdf
  -> generic attachment downloader/validator

append_metadata
log_failure
  -> superseded by Parquet/Catalog/run manifest/RecoveryStore

load_completed_pdf_urls
  -> superseded by SeenStore + attachment SHA/index

get_stock_list_signature
  -> generic universe snapshot/hash

save_progress
load_resume_index
  -> generic checkpoint/recovery

process_stock
  -> SitePlugin crawl/normalize + generic attachment discovery

main
  -> generic CLI/profile
```

# 18. Definition of complete

Phase L is not complete until:

```text
financial_report is a real generic dataset
HKEX annual/interim/quarterly discovery works
exact stockId lookup works
stable report identity works
metadata normalization works
PDF attachment pipeline works
invalid HTML-as-PDF is rejected
SeenStore/checkpoint/recovery work
one/10/100/full rollout sequence passes
NAS/Catalog audit is clean
query can retrieve report metadata and attachment references
hkex_reports_incremental works
hkex_reports_full works
```

Do not mark complete from parser fixtures alone.
