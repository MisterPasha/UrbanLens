# Published releases and read API

Steps 33–39 provide immutable releases, rollback and the required read API. Local PostGIS and Neon each retain active/previous validated model sets. UI wiring starts in step 40.

## Use

```powershell
python scripts/dev.py api
python scripts/dev.py release status --target neon
python scripts/dev.py release prepare --target neon
python scripts/dev.py release publish --target neon --release-id <validated-UUID>
python scripts/dev.py release rollback --target neon
python scripts/dev.py release cleanup --target neon
python scripts/dev.py release cleanup --target neon --apply
python scripts/dev.py api-schema
python scripts/dev.py api-check
python scripts/dev.py api-acceptance --target neon
```

Use `--target local` for Docker. `prepare` freezes the last verified Gold build into its own schema and checks its contents; it does not fetch new provider data. `publish` atomically switches the active pointer. `rollback` swaps active/previous. Cleanup previews by default, protects active/previous and skips schemas with readers; obsolete/failed models and unfinished candidates older than 24 hours are eligible. A hard cap of four registered model schemas prevents unlimited copies. Failed evidence stays in private S3.

The API runs locally at `http://127.0.0.1:8000`, with interactive documentation at `/docs`. Default local settings use the local API login; hosted settings use `URBANLENS_API_DATABASE_URL`. Provider refreshes remain monthly. Nothing here deploys a public web service.

## Endpoints

| GET path | Returns |
| --- | --- |
| `/health` | Process liveness; no database/provider call |
| `/api/v1/meta` | Release, source health, versions, filters, attribution and coverage |
| `/api/v1/areas` | Supported LSOAs and simplified display geometry |
| `/api/v1/postcodes/BN11AA` | Cached approximate search centre and explicit lookup/scope state |
| `/api/v1/establishments` | Matching total, page, bounded map points and category counts |
| `/api/v1/establishments/{UUID}` | Evidence, hygiene, location quality and reviewed company context |
| `/api/v1/areas/{code}/summary` | Area metrics and weighted city benchmark |
| `/api/v1/compare?area_codes=E01016849&area_codes=E01016850` | Exactly two distinct supported LSOAs, with shared categories/release |
| `/api/v1/companies?cohort=registered_office` | Bounded legal-company cohort; also supports `verified_operator` |
| `/api/v1/companies/{number}` | Supported company, legal dates, source link and verified premises |
| `/api/v1/activity` | Observed changes, before/after context, evidence and baseline state |

All analytical responses carry `release_id`, applied filters, metadata, coverage, null reasons and a request ID. Pin dependent calls with `?release_id=<UUID>`. Cursors also pin their original release and filters. Expired releases return **410 `release_expired`** with a refresh instruction; they are never silently replaced.

Exploration uses exactly one `mode`: `city` (default), `area`, `radius` or `bbox`. Radius needs longitude/latitude and defaults to 500 m; supported radii are 250/500/1,000/2,000 m. Categories are repeated query parameters; `empty_categories=true` selects none. Page size is at most 100, markers at most 1,000, and bbox at most 0.1° per side. `map_truncated` is explicit; marker/page limits never change the matching total. Unknown-location exclusions are labelled city-wide, never assigned to a circle/area.

Postcodes first reuse valid pipeline cache entries. Interactive misses use a separate cache (1,000 entries; positive 30 days, unmatched 1 day), one fixed provider endpoint, 32 KiB/4-second work limits and a 30-miss/minute process budget. Failure/throttle states are not negative-cache results. Coordinates remain approximate. The interactive cache is local SQLite in development and temporary per runtime in production; shared-cache/deployment concurrency configuration remains deployment work. It never grants database write access to the API login.

Area rates reuse dbt's shared calculations; city benchmarks divide summed counts by summed land/population, rather than averaging area rates. Responses identify population dates, land products and retained reference hashes. Missing denominators return null with reasons. Category filters also apply to the benchmark; demographic denominators remain unchanged.

Company lists require a cohort and allow `legal_status`, five-digit `sic_code`, and literal `name` substring filters. Activity allows `entity_type` and `event_type`. Both use `limit` (1–100) and release/filter-bound `cursor`; company numbers remain strings. Company lookup never calls live Companies House. Activity separates legal/source dates from observation times, reports unavailable baselines distinctly from zero changes, and omits historical locations and private evidence paths.

`api-acceptance` checks published data and records a ten-reader benchmark in ignored `data/gold/`. It uses an in-process HTTP client and real database, so deployed HTTP performance still needs verification. [M3 evidence](../../docs/evidence/m3-api.md).

## Files

- `scripts/release.py`, `database/releases.py`: freezing, atomic pointers, rollback and safe cleanup.
- Migration `0013_published_gateway.sql`: release registry and curated database read gateway; API has no direct Silver/candidate access.
- Migrations `0014_context_api.sql` and `0015_area_provenance.sql`: summary/comparison, cohort companies, activity and reference provenance.
- Migration `0016_nonexcluded_match_coverage.sql`: matching coverage excludes already excluded non-hospitality records. Counts derive from the pinned frozen roster; source records and draft reviews are unchanged. Local/Neon report 286 unresolved out of 2,478 non-excluded records (139 excluded unresolved records omitted). The previous all-roster match count remains only in retained audit tables.
- `src/urbanlens/api/`: request/response contracts, routes, read-only queries and postcode cache.
- `reference/api/openapi.json`, `web/src/api/contracts.ts`: generated from Python contracts. CI checks they match; regenerate with `api-schema` after contract changes.
- `web/src/api/client.ts`: typed fetch helper; no handwritten duplicate response types.
- `data/gold/`: ignored live verification/release reports; durable proofs are private S3 objects.

Logs contain request IDs, route templates, status and timing—not raw query values or database errors. Queries have 5-second statement and 2-second lock limits. Both retained release schemas now enter logical backups; unpublished workspace schemas remain rebuildable candidates.

[Validation evidence](../../docs/evidence/published-api.md).
