# Gold models

Steps 29–32 transform accepted Silver data into analytical tables with **dbt/SQL**. Both local PostGIS and Neon use the same definitions. Provider collection remains monthly.

```powershell
python scripts/dev.py gold build --target local
python scripts/dev.py gold build --target neon
python scripts/dev.py gold check --target neon
python scripts/dev.py gold fixtures
```

`build` creates/replaces the private `ul_candidate_gold` workspace, runs blocking dbt checks, installs shared query functions, generates dbt documentation and saves evidence. `check` reconciles its current results. `fixtures` runs local calculation/health examples without cloud credentials; GitHub Actions now runs it after database setup.

The build needs the existing scoped database logins and AWS profile for private S3 evidence. Schema provisioning uses the existing administrator connection; model creation uses `urbanlens_dbt`, which cannot change Silver data. A session lock keeps Silver inputs stable across dbt threads. No Lambda deployment, source refresh or API publication occurs. Atomic publication/rollback follows in step 33.

## Files

| Location | Purpose |
| --- | --- |
| `scripts/gold.py` | Build/check commands, input/version manifest, private schema permissions and verification |
| `dbt/models/staging/` | Typed current Silver observations and canonical candidates |
| `dbt/models/intermediate/` | One row per venue, with accepted source evidence, hygiene, provenance and independent exclusion reasons |
| `dbt/models/marts/` | Stored query-ready Gold tables below |
| `dbt/models/schema.yml` | Table descriptions, grains and key checks |
| `dbt/macros/` | Shared rate, freshness, selection and radius/area query definitions |
| `dbt/tests/` | Population reconciliations and hand-computable rate/health examples |

Staging/intermediate models are SQL CTEs inside dependent models, rather than extra stored copies. Generated catalog/manifest/SQL files are ignored under `.cache/gold-dbt-<target>-ul_candidate_gold/`; `index.html` is the dbt documentation entry point. Detailed build/check results are in `data/gold/`; immutable proofs and matching code archives also go to private S3.

## Tables in `ul_candidate_gold`

| Tables | Grain / use |
| --- | --- |
| `mart_establishment` | One primary spatial venue per release/UUID: **1,178** |
| `mart_venue_context` | One other candidate per release/UUID: **2,693**; uncertainty stays visible outside competitor totals |
| `mart_category`, `mart_coverage` | Category totals/shares; explicit coverage denominators and overlapping exclusion reasons |
| `dim_area`, `mart_area`, `mart_city` | 165 pinned LSOAs; area/category measures and weighted city benchmarks |
| `mart_company`, `mart_company_link` | 1,378 legal companies; reviewed operator relationships (currently zero) |
| `mart_activity`, `mart_activity_status` | Retained source/venue/company changes and separate baseline/empty-history states |
| `mart_source_health`, `mart_release` | Collection/check times, monthly due dates, reference editions and exact candidate inputs/versions |

Every table carries a candidate release ID. Published time stays null. These private tables are not yet returned by the public API. Rebuilding replaces this development candidate; step 33 supplies immutable published release handling.

## Calculation and filter rules

`N` counts primary spatial venues once by primary category, including `food_service`. `N/A`, `N/P×1000` and `P/A` use official land km² and compatible resident population. City rates divide summed numerators by summed denominators. Missing/invalid denominators produce null plus a reason; a valid zero venue count produces zero. Demographic counts/shares retain their reference date and describe residents.

Category `all` in area/city tables is a total: never add it to category rows. Source mix describes accepted source links, not guaranteed independent confirmations. Exclusion reasons overlap. Names can be unavailable; approximate/unresolved locations never become precise competitors.

Using a scoped database connection, `selected_venues(categories, area_codes, lon, lat, radius)` supplies the same full selection for later map/list/summary consumers. Null filters mean all; empty arrays mean none. `area_summary(area_codes, categories)` applies those filters with compatible area denominators. `radius_summary(lon,lat,radius,categories)` returns counts/mix and partial-coverage status, **without resident rates or allocated demographics**. Supported radii: 250/500/1,000/2,000 metres. Invalid categories/areas/centres are rejected.

Monthly warnings begin **more than 48 hours after the next due time**; the age of a statistical reference year does not make a source stale. Reference/bulk/postcode monthly checks remain manual until step 49; existing FSA/OSM/profile schedules are unchanged.

[Measured results and validation](../../docs/evidence/gold-models.md). dbt reference: [data tests](https://docs.getdbt.com/docs/build/data-tests).

[Publication and read API](../api/README.md): steps 33?36 freeze verified candidates into retained releases; the development workspace stays separate.
