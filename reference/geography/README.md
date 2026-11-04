# Pinned Brighton & Hove reference geography

Validated locally on 1 November 2026 for plan step 05. These are reference inputs and validation artifacts; they are not an application data release.

- `manifest.json`: exact acquisition URLs/query parameters, product IDs/editions, retrieval timestamps, byte counts, and SHA-256 checksums for 16 source responses (about 25 MB).
- `target-areas.csv`: the 165 supported 2021 LSOA codes/names, authority code, official land denominators, and joined population/age values for pipeline use.
- `inspection.json`: measured counts, schema mapping, workbook notes, reconciliation totals, transformation, and generated geometry checksums.
- `spatial-fixtures.json`: 165 interior-point assignments, an inclusive outer-boundary point, and an offshore exclusion point.

Original responses and full analytical geometry live in ignored `data/reference/geography/`. The area list and fixtures are small reviewable reference artifacts. Unrounded population cells support calculation/reconciliation; apply the workbook's nearest-100 presentation request when quoting population in prose/UI. Do not describe these estimates as exact headcounts.

From the repository root:

```powershell
python scripts/dev.py geography-fetch
python scripts/dev.py geography-check
python scripts/dev.py db-up
python scripts/dev.py db-check
python scripts/dev.py db-down
```

`geography-fetch` downloads public products, records provenance, and verifies existing cached artifacts on later runs. `geography-check` is offline and requires all pinned source files. It rebuilds the area list, coverage geometry, and fixtures after validation. Neither command loads permanent database tables or creates cloud resources. Database checks use temporary tables and roll back.

Acquisition uses the full-resolution BFC V10 service without generalisation parameters, selecting exact code batches rather than downloading the entire national boundary file. Count and code-set checks reject truncated results. Existing pins never silently advance: if an upstream response changes, acquisition fails and requires an explicit version review. Keep the original snapshot for replay; the live provider may no longer serve identical historical metadata bytes. Do not edit a checksum merely to make a failure pass.

The OSTN15 coordinate grid is pinned alongside source data; validation refuses a lower-accuracy fallback. Its reported transformation accuracy is one metre, which is not a claim that source venue coordinates or every boundary point are independently accurate to one metre. Native EPSG:27700 geometry is retained; WGS84 analytical outputs use longitude/latitude order. Display simplification is future work and must produce separate artifacts.

Use inclusive `covers` for area/city boundaries, assigning the smallest official LSOA code when more than one polygon covers a point. A circle crossing the pinned coastline/city union has partial coverage, including its offshore portion. Seaward features outside the clipped geometry are outside this supported spatial footprint and require visible exclusions/review; do not extend the geometry silently. Never allocate LSOA populations proportionally to a search circle.

Radius queries use WGS84 **geography**, with metres and the default spheroid. The numerical policy is `ST_DWithin(point, centre, radius + 0.000001)`; the one-micrometre addition handles arithmetic disagreement at an exact constructed boundary. It is not a positional-quality allowance. Approximate/postcode-centroid exclusions remain a separate requirement. This shared constant is in `src/urbanlens/spatial.py`; future query implementation must use the same policy.

Source and reuse details: [ONS source register](../../docs/sources.md), [inspection evidence](../../docs/evidence/geography.md).

Attribution for these derived reference artifacts:

Source: Office for National Statistics licensed under the Open Government Licence v.3.0.

Contains OS data © Crown copyright and database right 2026.

Area measurements have been created by the Office for National Statistics. Crown Copyright. All rights reserved 2022.

ONS products are reused under [OGL v3](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/). The separate OSTN15 grid is supplied under BSD-2-Clause; retain its notices if distributing it (the grid itself is not included in this repository). See the pinned supplier README in the raw snapshot.
