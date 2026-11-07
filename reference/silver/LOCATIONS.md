# Location checks — step 22

Checks FSA/OSM source observations before establishment matching. A usable point
does not prove that a business is eligible, unique, currently observed or ready
for publication.

| File | Use |
| --- | --- |
| `src/urbanlens/silver/locations.py` | Coordinate validation, selection and review types |
| `location_processing.py` | Postcode fallback, boundary assignment, S3 evidence and database load |
| `scripts/locations.py` | Run the assessment |
| `src/urbanlens/database/sql/0007_location_assessment.sql` | Private results and search-coverage functions |
| `data/silver/locations-local.json`, `locations-neon.json` | Latest run summaries and S3 evidence references |

```powershell
python scripts/dev.py locations --target local
python scripts/dev.py locations --target neon
```

Requires existing step 20/21 data, migration 0007 and the configured AWS profile.
Default cache assessment date is today in UTC. `--as-of YYYY-MM-DD` reproduces a
dated assessment; it does not fetch historical source data. Repeat after source,
classification or review changes. Source acquisition dates remain unchanged.

Rules:

- Accept finite WGS84 coordinates within global bounds; reject zero coordinates
  and wrong SRIDs. Check supported coverage separately. Never swap axes or move
  a point automatically. Source points are not independently verified storefronts.
- Prefer a usable source point over a postcode centroid. Use only resolved,
  unexpired cached postcodes acquired by the assessment date. Retain cache dates,
  quality/vintage limitations and evidence. No new provider requests are made.
- Approximate points receive **no primary LSOA assignment** and cannot enter
  primary area, city or radius totals. Unknown spatial exclusions are city-wide;
  their membership in a selected area/circle is unknown.
- Use full-resolution LSOA-2021 polygons, inclusive `ST_Covers`, and the lowest
  covering code for a shared boundary. Retain all covering codes to expose ties.
- A postcode authority contradiction requires review and blocks spatial use.
  A different postcode-centroid LSOA is flagged but does not disprove a precise
  premises point. Never reconstruct withheld addresses or locations.
- Conflicting precise alternatives require explicit review. Optional
  `--reviews path.json` accepts a JSON list with `provider`, `scope`, `source_key`,
  `expected_record_hash`, `longitude`, `latitude`, `reviewed_on`, `reason` and
  `evidence`. Stale, undated or invalid overrides fail. No reviews are fabricated.
- Cross-provider identity/address comparisons follow accepted matching in step
  23. This step does not assume that nearby FSA/OSM records are the same venue.
- Search centres must themselves be covered. Circles reaching beyond the nearest
  city boundary, coastline or hole receive partial coverage. Distances use PostGIS
  geography metres; accepted radii are 250/500/1,000/2,000 m. Existing precise
  radius membership uses `ST_DWithin` with the shared 0.000001 m numeric tolerance.
  No out-of-scope competitors or radius demographics are estimated.

`ul_silver.observation_location` stores one assessment per retained venue
observation. Full evidence remains immutable in S3. `spatial_usable` concerns
location only; `eligible_spatial_candidate` also checks step 21 eligibility.
Neither is a published competitor count: duplicate/status/release gates remain.
Unresolved eligibility records retain their spatial evidence for later resolution.
