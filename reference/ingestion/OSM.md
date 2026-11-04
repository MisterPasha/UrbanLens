# OSM collection

Refresh policy: monthly. Existing November observations count for November; their original metadata remains unchanged. The next routine collection is December 2026.

Collects venue-tagged features and every `fhrs:id` feature in a padded city envelope. Keeps nodes, ways and relations separately for later matching; it does not create canonical venues.

```powershell
python scripts/dev.py osm-fetch
python scripts/dev.py osm-check
```

- `osm-fetch`: submits one bounded Overpass query, saves original JSON and its query/boundary definition, validates and normalizes, then selects the accepted snapshot. Repeating collection within an accepted UTC calendar month reuses it.
- `osm-check`: rebuilds from retained inputs without network access, compares normalized hashes and writes `data/ingestion/osm-check.json`. Current selection and source freshness stay unchanged.

| File | Purpose |
| --- | --- |
| [osm-source.json](../../config/osm-source.json) | Version, endpoint, pinned boundary/template hashes and count limits |
| [osm-query.overpassql](osm-query.overpassql) | Versioned venue/FHRS selectors, full geometry, edit metadata and terminal count |
| [osm.py](../../src/urbanlens/ingestion/osm.py) | Collector, complete-response checks, source fields and geometry handling |
| [osm_source.py](../../scripts/osm_source.py) | Fetch/replay commands |
| [fixtures](../../tests/fixtures/osm/complete.json) | Invented nodes, polygons, courtyard and unresolved way |

Artifacts and run/current state share ignored `data/ingestion/` with FSA, under separate provider paths. Raw JSON and the exact query/boundary definition are immutable. Stored and request-body SHA-256 values support replay. Source base timestamp, element version/time where supplied, tags, original geometry and ambiguous FHRS tokens remain separate evidence.

HTTP 200 is insufficient: errors/remarks, malformed JSON, unknown element types, missing or inconsistent final counts, invalid IDs/tags/metadata, duplicates and implausible counts block acceptance. Defaults: 1,000–20,000 records and at most a 20% drop from current. Missing or unusable geometry remains explicitly unresolved, without inventing a location.

Geometry uses source node coordinates or a point on a valid polygon's surface. Multipolygon members are assembled by outer/inner roles; courtyard holes are excluded. Open ways, broken rings and unsupported relations stay unresolved. Bounding-box centres are never used as venue positions. Original geometry remains unchanged.

The query uses the pinned ONS city's envelope plus **0.03 degrees on each side** (roughly 2 km east/west and 3 km north/south here); output geometry is not cropped. This captures ordinary nearby boundary-crossing premises even when vertices are outside the city. Fixtures verify that case. It cannot prove completeness for enormous polygons whose vertices/members all lie outside the padded window; such coverage would need a regional extract. This is a bounded source observation, not a census of all businesses.

`in_city` uses inclusive `city.covers(representative_point)`; footprint intersection/crossing are separate flags. A crossing footprint does not automatically make a venue eligible. Features outside the city remain source evidence.

The collector is ready for a later monthly scheduler; this step does not deploy one. Interactive API/UI code never calls Overpass. Scheduled failures preserve accepted cached data. Two HTTP attempts maximum, 32 MiB total, 240 seconds overall; source query timeout is 90 seconds.

OSM reuse remains subject to [ODbL and attribution](https://www.openstreetmap.org/copyright), including the project's later combined-data release requirements. Provider guidance: [query geometry/bounds](https://dev.overpass-api.de/overpass-doc/en/full_data/bbox.html), [metadata/output](https://dev.overpass-api.de/overpass-doc/en/targets/formats.html), [shared-service use](https://dev.overpass-api.de/overpass-doc/en/preface/commons.html).
