# Frontend

Steps 40–48 provide the integrated research interface and local M4 verification. Public hosting, operating controls and final release checks remain in M5.

## Run

Start Docker Desktop, then run:

```powershell
python scripts/dev.py db-up
python scripts/dev.py api
```

In a second terminal, run `python scripts/dev.py web` and open **http://127.0.0.1:5173**. Vite forwards API requests to port 8000. The local database must already contain a published release to show counts.

## Files

- `web/src/App.tsx`: navigation, published release, expiry recovery and source notices.
- `web/src/state/session.ts`: shared categories, search centre/radius/area, comparison areas, selected venue and pagination. Selections survive navigation in memory.
- `web/src/api/query.ts`: bounded cache keyed by release and filters; aborts abandoned requests and ignores obsolete responses. Refresh preserves filters and clears cursors/cache.
- `web/src/components/Exploration.tsx`: filtered totals, paginated list and selected venue.
- `web/src/components/VenueEvidence.tsx`: selected venue summary, hygiene information and public source records.
- `web/src/components/Companies.tsx`: company scopes, name search, 25-row pages, legal details and verified venue links.
- `web/src/components/Activity.tsx`: record/change filters, starting baselines, 25-event pages and public before/after comparisons.
- `web/src/components/Methodology.tsx`: source health/dates, measured coverage, interpretation and attribution.
- `web/public/data-reuse/index.html`: working reuse/status page; downloadable bundles remain pending public-release validation.
- `web/src/components/VenueMap.tsx`: Leaflet map, category colours, linked selection and tile-failure recovery.
- `web/src/components/RadiusSearch.tsx`: postcode lookup, centre validation and clear failure messages.
- `web/src/api/exploration.ts`: shared query parameters for centre validation, map, list and totals.
- `web/src/components/Comparison.tsx`: area preview, two-area metrics, city benchmark and data dates.
- `web/src/components/AreaMap.tsx`, `AreaSelect.tsx`: clickable LSOA boundaries and searchable name/code selectors.
- `web/src/components/CategoryMix.tsx`: shared category bars, counts and percentage table for Explore/Compare.
- `web/src/components/Feedback.tsx`, `Workspace.tsx`: shared feedback and the other section summaries.
- `web/src/style.css`: desktop/mobile layout, keyboard focus and reduced motion.
- `web/tests/shell.spec.ts`: two browser cases for navigation/races and recovery/mobile layout. Existing smoke check remains.
- `web/tests/exploration.spec.ts`: two browser cases for selection/pagination/filters and tile-failure/keyboard/mobile behaviour. Tests stub tiles rather than contacting the public provider.
- `web/tests/radius.spec.ts`: two browser cases for circles/filters and failed or obsolete centre lookups.
- `web/tests/comparison.spec.ts`: two browser cases for map/keyboard selection, reconciliation, missing values and recovery.
- `web/tests/venue-evidence.spec.ts`: two browser cases for evidence/hygiene and failure recovery, withheld fields and obsolete responses.
- `web/tests/companies.spec.ts`: two browser cases for paging, company/venue links, search races, withheld offices and empty scopes.
- `web/tests/activity.spec.ts`: two browser cases for event meaning, paging, privacy, sparse history, recovery and obsolete responses.
- `web/tests/integrated.spec.ts`, `fixtures/journey.ts`: two integrated checks for all views, axe accessibility and bounded failure recovery. Synthetic data stays in tests.

## Explore

**Your competition at a glance** appears above the map with the matching venue total and filtered category counts/shares. The former separate total/category-selection cards are removed.

The list has **25 venues per page**; Previous/Next preserve the full analytical total. The map shows up to **500 overview points**, plus one highlighted venue if selected outside that overview. All matches remain accessible through the list. Clicking a marker or list entry selects the same venue; off-page selections are labelled explicitly.

Category filters affect map, list, total and category summary together. Moving/zooming the map changes presentation only; **Show overview points** resets its extent. Coverage tables live in **Data & methodology** as separate release-wide context.

Tile failure leaves points, list and summaries usable, with a retry control. Default tiles use visible OpenStreetMap attribution and normal browser caching. Optional public `VITE_MAP_TILE_URL` / `VITE_MAP_TILE_ATTRIBUTION` settings belong in `web/.env.local`. No offline tile download or prefetch is provided. [Tile policy](https://operations.osmfoundation.org/policies/tiles/) · [Leaflet 1.9.4](https://leafletjs.com/reference-1.9.4.html).

Choose **Around a point**, then enter a full UK postcode or select **Choose centre on map** and click a point. **Use map view as centre** also works with keyboard map panning. Moving the map alone never changes the search. Advanced coordinates remain under **Enter coordinates**.

The default radius is **500 m**, with **250 m, 1 km and 2 km** alternatives. The displayed centre and circle match the query. New centres take effect only after validation; failed/unsupported lookups and obsolete responses preserve the current centre. Counts/category mix use precise venues, excluding approximate venue locations. A postcode is an approximate search centre. Boundary warnings explain that outside-city venues are not covered; no circle population/age estimates are invented. Leaflet draws the circle in metres; server spatial distance determines membership.

## Venue evidence

In **Explore**, click a marker or select a list entry. **Selected venue** shows its name, address, category, last observation, FSA hygiene information and public source records. Hygiene assessment and collection dates stay separate; missing hygiene is not a zero rating. Withheld addresses remain unavailable. **Clear selection** closes the panel.

## Companies

Browse **Registered offices in Brighton & Hove**, search legal names and select a company. Lists show 25 companies per page. Details include company number, legal status/type, incorporation/cessation, SIC codes, registered office and the official Companies House link. Verified venue links open the existing concise venue summary where available.

Registered-office companies are selected by office geography and reported hospitality SIC codes; they are not venue competitors. Legal dates are not venue opening/closure dates. Missing or withheld offices stay unavailable. Search, pages and selected company survive section navigation; changing the search resets paging and selection. Venue category filters do not affect company results.

## Activity

Filter by **Record type** or **Change type**. Events show observation time, provider, source event date where supplied and meaningful public before/after values. Venue, provider-record, legal-company and processing events stay distinct. Pages contain 25 events; filters/pages survive section navigation, and changing filters resets paging. Venue category filters do not affect activity.

**History starting points** shows baseline availability independently of filters. Missing baselines mean insufficient history; zero recorded events after a baseline differs from no matching filtered events. Baselines alone cannot establish trends, and the current API does not certify a count of comparable monthly follow-ups. Retention dates are not monitoring start dates. Missing/reappearing source records do not prove closure/reopening; incorporation/dissolution describes legal companies. Historical addresses, coordinates and private evidence downloads remain absent. Synthetic events exist only in browser tests.

## Data & methodology

Source dates/editions and health are separate from release publication and population reference dates. Expand **Measured coverage** groups to see source contribution, location quality, eligibility, category detail, unresolved records and company-link coverage with denominators. These figures describe the whole release, independently of search filters; populations/dimensions must not be added together. Missing denominators remain unavailable.

Interpretation, source limitations and linked licence notices are on the same page. Map credit stays visible if tiles fail. **Data reuse and downloadable-data status** opens the static preview page; no public bundle is claimed available. Required downloadable bundles and rights checks remain public-release gates.

Verification uses `@axe-core/playwright` 4.14.0 for sampled WCAG 2/2.1 A/AA checks, plus keyboard, status, overflow and recovery assertions. It is not a complete accessibility certification. Bounded slow-start and cutoff responses are simulated; deployed database latency and the real operating cutoff remain M5 work.

## Compare

Open **Compare** and click an area boundary to preview its data. The next click chooses the second area. Radio controls select which slot to replace; green/purple identify the first/second area. Name/code search and selectors provide keyboard access. **Show selected areas** zooms to the selection.

The 165 outlines are simplified **2021 LSOAs**, not familiar neighbourhood borders or hexagons. Map streets/landmarks help locate them; official names/codes remain visible. Tile failure preserves boundaries, selectors and data. Geometry is for display; published area metrics use the authoritative assignments and official land denominators.

Both areas and the city benchmark share categories, release, geography and population edition. Cards show counts, category shares, land/resident rates, population and broad age mix. Collection/publication and population dates are explicit. Missing denominators remain unavailable with reasons; zero matches have no defined category shares. These figures describe concentration and resident context, not footfall or demand. No hand-made neighbourhood aliases were added. [Leaflet GeoJSON reference](https://leafletjs.com/reference-1.9.4.html#geojson).

The URL fragment selects a section; there are no saved projects or account storage. HTTP 410 pauses research until **Continue with current data** is selected. Failures do not replace results with invented zeros.

Verify with `python scripts/dev.py browser-test`. [Step 40 evidence](../../docs/evidence/app-shell.md) · [Step 41 evidence](../../docs/evidence/exploration.md) · [Step 42 evidence](../../docs/evidence/radius-search.md) · [Step 43 evidence](../../docs/evidence/area-comparison.md) · [Step 44 evidence](../../docs/evidence/venue-evidence.md) · [Step 45 evidence](../../docs/evidence/company-workflow.md) · [Step 46 evidence](../../docs/evidence/observed-activity.md) / [Step 47 evidence](../../docs/evidence/methodology.md) / [M4 journey evidence](../../docs/evidence/m4-journeys.md).
