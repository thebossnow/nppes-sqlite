# nppes-sqlite

Streaming ingest pipeline + normalized SQLite database for the full CMS NPPES NPI registry (9M+ providers).

One file you can move between your Mac mini M4 and Vultr VPS. Fast local queries. Weekly refresh in seconds.

## Why this exists

The raw CMS data is a beast:

- Monthly full: ~9–10 GB uncompressed CSV (9M rows × 330 columns, most of them empty repeating taxonomy/identifier slots).
- Weekly delta ZIPs: single-digit MB, tens of thousands of changed records.
- Three reference files per bundle: practice locations, endpoints, other names.

Direct load of the wide table wastes enormous space. Normalized properly it shrinks dramatically and queries become fast.

## Storage footprint (engineering estimates)

**Raw, on disk:**

- Compressed monthly ZIP: ~900 MB – 1 GB
- Uncompressed main CSV: 9–10 GB
- Reference files: ~1–2 GB combined (practice locations is the big one at ~850K rows in full)

**Loaded into SQLite:**

- Naive (all 330 cols as-is): ~12–16 GB + 2–4 GB indexes
- **Normalized (recommended):** ~6–9 GB total including indexes

You only store actual taxonomy/identifier rows (most providers have 1–3 taxonomies and a few identifiers).

**During ingest:** plan on ~15 GB working space (CSV + building DB at same time). Steady state 6–9 GB normalized.

On either your M4 or the Vultr box this is trivial. Make sure you have ~20 GB headroom on the VPS before a full ingest.

## Compute and time

- **Initial full load:** 10–30 min on M4 (streaming batched Python). Normalizing is slower to load but *much* faster forever after. Batch in 5k–50k row transactions.
- **Weekly incremental:** seconds to a couple minutes (tiny files).
- **Memory:** a few hundred MB streaming. Do **not** load the whole CSV into pandas.
- **Query perf (indexed):** single NPI or state+taxonomy = sub-ms to low-ms. County-level searches < 1s.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  INGEST (scheduled)                                        │
│                                                            │
│  CMS monthly full ──┐                                      │
│  CMS weekly deltas ─┤──> streaming CSV parser ──> staging  │
│                     │         │                            │
│                     │         ├─> normalize repeating      │
│                     │         │   groups (taxonomy,        │
│                     │         │   identifiers)             │
│                     │         v                            │
│                     │    upsert by NPI                     │
└─────────────────────┼─────────────┬──────────────────────┘
                       │             v
                       │   ┌──────────────────────┐
                       │   │  LOCAL DB             │
                       │   │  providers (1/NPI)    │
                       │   │  taxonomies (N/NPI)   │
                       │   │  other_identifiers    │
                       │   │  practice_locations   │
                       │   │  endpoints            │
                       │   │  other_names          │
                       │   │  + indexes            │
                       │   └───────────┬───────────┘
                       │               │
        ┌──────────────┴───────┐       │
        │ FRESHNESS LAYER       │      │
        │ NPPES live API        │      │
        │ - on-demand revalidate│      │
        │ - check single NPI    │      │
        │ - "is this current?"  │      │
        └──────────┬────────────┘      │
                   │                   │
                   v                   v
        ┌──────────────────────────────────────┐
        │  QUERY / SERVICE LAYER                 │
        │  - search (county, taxonomy, zip)      │
        │  - NPI validate (Luhn + DB lookup)     │
        │  - batch claims scrub                  │
        │  - roster reconcile                    │
        └──────────┬─────────────────────────────┘
                   v
        your apps (provider tools, patient locator)
```

**Division of labor:**

- The **DB** handles 95% of work: searches, aggregates, batch jobs. Fast, offline, unlimited.
- The **NPPES API** (https://npiregistry.cms.hhs.gov/api-page) is used only for targeted freshness checks on a specific NPI right before an action (e.g. claim submission). Never for search/bulk.

Store `last_update_date` (from CMS) + your `db_loaded_at`. Apps do instant DB lookup then optionally one API call to confirm "nothing changed since snapshot".

## Schema (normalized)

```sql
providers
  npi (PK), entity_type, ein, org_name, last_name, first_name,
  middle_name, credential, sex, sole_proprietor,
  status, enumeration_date, last_update_date,
  deactivation_date, deactivation_reason,
  mailing_line1, mailing_line2, mailing_city, mailing_state,
  mailing_postal, mailing_country, mailing_phone, mailing_fax,
  location_line1, location_line2, location_city, location_state,
  location_postal, location_country, location_phone, location_fax,
  authorized_official_* (for orgs)

taxonomies            (npi FK, code, license, license_state, is_primary, taxonomy_group)
other_identifiers     (npi FK, identifier, type_code, state, issuer)
practice_locations    (npi FK, line1, line2, city, state, postal, country, phone, fax)
endpoints             (npi FK, type, endpoint, use, content_type, ...)
other_names           (npi FK, name, type_code, created_date)
```

**Key indexes** (covers the common access patterns your tools need):
- providers(npi)
- providers(location_state, location_postal)
- taxonomies(npi), taxonomies(code)
- other_identifiers(npi)
- practice_locations(npi), practice_locations(state, postal)
- endpoints(npi)

## Recommendation

**SQLite, normalized** is the right engine for solo/self-hosted use at this scale.

- Single portable file.
- Zero server to run/maintain.
- Plenty fast for 9M rows + proper indexes.
- Easy to rsync or move between M4 ↔ Vultr.

Only move to Postgres later if you need concurrent writes from multiple app instances or want to expose as shared service.

Net footprint: ~6-9 GB disk steady, ~15 GB during ingest, 10-30 min first load, seconds for deltas, negligible RAM, one cron job.

## Quick start

```bash
git clone https://github.com/thebossnow/nppes-sqlite.git
cd nppes-sqlite

# 1. (optional) create venv / uv
python -m venv .venv && source .venv/bin/activate
pip install -e .

# 2. Download data (or just the small weekly for testing)
# Full monthly: https://download.cms.gov/nppes/NPI_Files.html
# Weekly are tiny: look for "Weekly Update"

# 3. Ingest a weekly delta (fast to test)
python -m nppes.loader --db data/nppes.db \
  --main npidata_pfile_....csv \
  --pl pl_pfile_....csv \
  --endpoint endpoint_pfile_....csv \
  --othername othername_pfile_....csv

# Or full load (same command, just point at the big files)
```

The loader is streaming, normalizes on the fly, and does batched upserts.

## Weekly automation (cron)

```bash
# ~/.local/bin/nppes-weekly.sh
#!/bin/bash
set -euo pipefail
DATE_RANGE=$(date +%m%d%y -d '7 days ago')_$(date +%m%d%y)
URL="https://download.cms.gov/nppes/NPPES_Data_Dissemination_${DATE_RANGE}_Weekly_V2.zip"
WORKDIR=/tmp/nppes-weekly-$$
mkdir -p $WORKDIR
curl -sL "$URL" -o $WORKDIR/weekly.zip
unzip -o $WORKDIR/weekly.zip -d $WORKDIR
python -m nppes.loader \
  --db /data/nppes/nppes.db \
  --main $WORKDIR/npidata*.csv \
  --pl $WORKDIR/pl*.csv \
  --endpoint $WORKDIR/endpoint*.csv \
  --othername $WORKDIR/othername*.csv \
  --delta
rm -rf $WORKDIR
```

Run daily or after each weekly drop. Idempotent upserts.

## Query usage (from your apps)

```python
from nppes.query import NPIQuery

q = NPIQuery("data/nppes.db")

# Single lookup (instant)
rec = q.get_by_npi("1234567890")

# Search examples
dentists = q.search_by_taxonomy("122300000X", state="CA", limit=100)
in_zip = q.search_by_postal("90210", limit=50)
all_in_county = q.search(state="NY", postal_prefix="100", taxonomy_code="207Q00000X")

# Validate NPI (Luhn + existence)
ok = q.validate_npi("1992817777")
```

See `nppes/query.py` and examples/.

## Freshness

```python
# Pseudocode in your app
provider = db.get_by_npi(npi)
if provider and needs_guaranteed_fresh(provider.last_update_date):
    live = nppes_api.get(npi)  # one call only when it matters
    if live['last_update_date'] > provider.last_update_date:
        db.upsert_live(live)
```

## Development

```bash
python -m pytest tests/ -q
python -m nppes.loader --help
```

## Data sources

- https://download.cms.gov/nppes/NPI_Files.html (monthly full + weekly incremental + deactivation reports, V2)
- NPPES Read API for live checks: https://npiregistry.cms.hhs.gov/api-page

All data is FOIA-disseminable public data.

## License

MIT

---

**One more time on the numbers (environment-independent raw footprint):**

- 9M rows main.
- Weekly deltas: tiny.
- Normalized SQLite: 6–9 GB steady state.
- Ingest 10–30 min full, seconds for deltas.
- Stream everything. Batch transactions. Index the right things.

Build once, query forever locally.
