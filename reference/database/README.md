# Database foundation

Step 19 created the PostGIS tables and permissions for Silver and Gold. [Step 20](../silver/README.md) now loads normalised source working state. Source files and full monthly copies stay in S3.

## Files

| File | Purpose |
| --- | --- |
| `src/urbanlens/database/sql/0001_core.sql` | Tables, keys, indexes and controlled values |
| `0002_api.sql` | Published-data views and indexed radius function |
| `0003_integrity.sql` | Replay, observation-date and registered-office safeguards |
| `0004_processing_privacy.sql` | Processing revisions and withheld-location safeguards |
| `0005_observation_streams.sql` | Independent observation scopes, string FSA identifiers and bounded postcode cache |
| `src/urbanlens/database/migrations.py` | Transactional migration runner and role grants |
| `src/urbanlens/database/verification.py` | Synthetic access, integrity and spatial checks |
| `src/urbanlens/database/dbt.py` | Connect dbt using its restricted login |
| `scripts/database.py` | Setup, migrate and check commands |
| `tests/integration/test_migrations.py` | Fresh-database, checksum and rollback tests |

Applied SQL files are immutable: add the next numbered file for changes. The migration ledger records each file's SHA-256; concurrent migrations are locked and failures roll back.

## Table grain and lifecycle

Grain means what one row represents. Names below omit their schema prefix.

| Schema / tables | One row represents | Lifecycle |
| --- | --- | --- |
| `ul_control.source_scope`, `collection_cycle` | Provider/query scope; one collection window | Monthly source metadata; on-demand postcode requests are separate |
| `ingestion_run`, `source_snapshot`, `source_head` | Attempt; accepted complete collection; current snapshot per scope | Failed attempts remain runs; replay keeps the original cycle and cannot become a fresh snapshot |
| `artifact` | S3 object URI, hash and size | Metadata only; full raw payloads stay out of PostgreSQL |
| `review`, `schema_migration` | Review decision; applied SQL file | Retain decisions and migration history |
| `data_release`, `release_source`, `release_slot` | Release metadata; source pinned to release; retained model slot | Active, previous and at most one building set |
| `ul_silver.area`, `population` | Versioned LSOA; area/population edition/reference date | Keep geography and population needed by retained releases |
| `source_identity`, `identity_scope_state` | Provider key/generation; identity's observation state in a scope | One active generation; retain absence/baseline evidence |
| `observation_version` | Changed interpretation of a source record within a scope | One current version per identity/scope; unchanged monthly sightings update last-seen fields |
| `postcode_cache` | Current dated response per postcode | Original expiry retained; source evidence in S3 |
| `establishment`, `company` | Physical venue; legal company | Separate entities; registered office is not a venue location |
| `establishment_source_link`, `company_establishment_link` | Source-to-venue assignment; reviewed company relationship | Effective dates preserve relationship history; company links require matching approved review |
| `field_provenance` | Selected venue field and its evidence/rule | One current selection per field; retain necessary evidence |
| `change_event`, `event_observation_evidence` | Event; observation supporting an event | Retained events protect their supporting observations from deletion |
| `ul_gold.establishment`, `company`, `company_establishment` | Venue, company or relationship within a release | Deleting an obsolete release slot cascades to its Gold rows |
| `ul_gold.activity`, `area_metric`, `source_health` | Release/event; release/area/category; release/provider/scope | Same bounded release lifecycle |
| `ul_api.*` | Read-only views of active/previous published releases | Failed, unvalidated and building releases remain hidden |

Observed dates describe the source evidence; `processed_at`, `valid_from`, `valid_to` and `revision` describe processing history. Reprocessing one snapshot with a new rule can create a changed version without inventing a new observation date.

Current observations and required evidence remain; changed history/events have the planned 12-month window. The retention worker and backup recovery arrive in step 27. This schema supports that policy; it does not yet run automatic pruning. S3 raw retention remains 90 days; longer-lived event evidence uses the separate `evidence/` prefix.

The existing fenced S3 registry remains the ingestion authority. These control tables are a downstream projection, not a second collection lock or scheduler.

## Logins

| Role | Access |
| --- | --- |
| `urbanlens_migrator` | Owns application schemas; applies migrations and grants |
| `urbanlens_loader` | Reads control state; writes source/control/review and Silver rows; cannot publish releases or edit migration history |
| `urbanlens_dbt` | Reads Silver/control; builds Gold and manages release metadata; cannot change Silver observations |
| `urbanlens_api` | Reads seven published views and executes the radius function; no private-schema access, writes, DDL or temporary tables |

The administrator URL stays separate. Scoped role URLs are stored only in ignored `.env`; Neon API uses the pooled endpoint, while loader/dbt/migrations use direct connections. Existing AWS ingestion credentials and S3 deployment are unchanged.

API access uses an explicit allowlist, including for future objects. Default privileges depend on the role creating an object, so grants cover both migrator-created and dbt-created models. [PostgreSQL documentation](https://www.postgresql.org/docs/17/sql-alterdefaultprivileges.html).

Locations use WGS84/SRID 4326. Native geography columns have GiST indexes. The trusted radius function keeps the indexed predicate inside the publication boundary, supports 250/500/1000/2000 metres, includes the boundary and excludes postcode-centroid fallbacks.

## Commands

Run from the repository root with Docker running for local checks:

```powershell
python scripts/dev.py database setup --target local
python scripts/dev.py database migrate --target local
python scripts/dev.py database check --target local
python scripts/dev.py database clean-check --target local
python scripts/dev.py dbt-debug --target local
```

Use `--target neon` for the configured Neon development project. Initial Neon setup requires its owner URL in `URBANLENS_DATABASE_ADMIN_URL` (or the original pipeline URL before setup). `setup` provisions roles and saves their URLs; `migrate` applies new SQL; `check` verifies connections/permissions; `clean-check` creates a new temporary database, tests it and removes it. CI uses only disposable local PostGIS and needs no cloud secrets.

Private machine-readable results go to `data/database/`. Detailed database failures go to ignored `.cache/database-error.txt`. [Verified results](../../docs/evidence/database-foundation.md).
