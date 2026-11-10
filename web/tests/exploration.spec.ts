import { expect, test } from '@playwright/test';
import type { Page } from '@playwright/test';
import type { Venue, VenueDetail } from '../src/api/contracts';

const release = '11111111-1111-4111-8111-111111111111';
const id = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const all = ['cafe', 'restaurant', 'takeaway', 'bakery', 'pub_bar', 'food_service'];
const venues: Venue[] = Array.from({ length: 501 }, (_, index) => {
  const n = index + 1;
  return { establishment_id: id(n), display_name: `Venue ${n}`, name_unavailable: false,
    primary_category: n % 2 ? 'cafe' : 'restaurant', secondary_categories: [],
    address: n <= 2 ? '42 Shared Street' : `${n} Harbour Road`, postcode: 'BN1 1AA',
    location_quality: 'source_point', area_code: 'E01000001', observation_state: 'observed',
    last_observed_at: '2026-11-04T12:00:00Z', source_mix: 'fsa+osm',
    longitude: n <= 2 ? -.15 : -.15 + (n % 10) * .001,
    latitude: n <= 2 ? 50.82 : 50.82 + Math.floor(n / 10) * .00015 };
});
const coverage = [
  { universe: 'current canonical roster', dimension: 'coordinate_quality', bucket: 'postcode_fallback', n: 6 },
  ...[['source_point', 501], ['postcode_fallback', 5], ['withheld', 2]].map(([bucket, n]) => ({
    universe: 'eligible resolved supported venues', dimension: 'coordinate_quality', bucket, n,
  })),
  { universe: 'current canonical roster', dimension: 'eligibility', bucket: 'unresolved', n: 9 },
  { universe: 'current canonical roster', dimension: 'duplicates', bucket: 'unresolved', n: 5 },
];

function envelope(data: unknown) {
  return { release_id: release, metadata: { published_at: '2026-11-07T12:00:00Z',
    geography_version: 'LSOA-2021', population_edition: 'fixture', population_reference_date: '2024-06-30',
    taxonomy_version: 'fixture', sources: [], attribution: ['OpenStreetMap contributors: ODbL'] },
    applied_filters: {}, coverage, null_reasons: {}, data };
}

async function api(page: Page, requests: URL[]) {
  await page.route('**/api/v1/**', async route => {
    const url = new URL(route.request().url());
    requests.push(url);
    const endpoint = url.pathname.split('/api/v1/')[1];
    let data: unknown;
    if (endpoint === 'meta') data = { city: 'Brighton & Hove', industry: 'Hospitality', categories: all, radii: [250, 500, 1000, 2000], default_radius: 500 };
    else if (endpoint === 'areas') data = [];
    else if (endpoint === 'establishments') {
      const cats = url.searchParams.getAll('categories');
      const selected = url.searchParams.get('empty_categories') === 'true' ? [] : venues.filter(venue => cats.includes(venue.primary_category));
      const offset = Number(url.searchParams.get('cursor') ?? 0);
      const limit = Number(url.searchParams.get('limit'));
      const mapLimit = Number(url.searchParams.get('map_limit'));
      data = { total: selected.length, items: selected.slice(offset, offset + limit),
        has_more: selected.length > offset + limit,
        next_cursor: selected.length > offset + limit ? String(offset + limit) : null,
        map_points: selected.slice(0, mapLimit).map(venue => ({ establishment_id: venue.establishment_id,
          primary_category: venue.primary_category, longitude: venue.longitude, latitude: venue.latitude })),
        map_truncated: selected.length > mapLimit, category_mix: {
          cafe: selected.filter(venue => venue.primary_category === 'cafe').length,
          restaurant: selected.filter(venue => venue.primary_category === 'restaurant').length,
        }, partial_coverage: false, unknown_location_scope: 'city-wide; membership in this search is unknown' };
    } else if (endpoint.startsWith('establishments/')) {
      const venue = venues.find(venue => venue.establishment_id === endpoint.split('/')[1]);
      if (!venue) throw new Error('Unexpected fixture venue');
      data = { ...venue, eligibility: 'eligible', duplicate_status: 'resolved', primary_spatial_eligible: true,
        first_observed_at: venue.last_observed_at, exclusion_reasons: [], hygiene: [], null_reasons: {},
        sources: [], provenance: {}, verified_companies: [] } satisfies VenueDetail;
    } else throw new Error(`Unexpected endpoint ${endpoint}`);
    await route.fulfill({ json: envelope(data) }).catch(() => {});
  });
}

const background = { contentType: 'image/svg+xml', body: '<svg xmlns="http://www.w3.org/2000/svg" width="256" height="256"><rect width="256" height="256" fill="#e5eee7"/></svg>' };

test('map/list selection, pagination and filtering share the pinned total without duplicating methodology coverage', async ({ page }) => {
  const requests: URL[] = [];
  const errors: string[] = [];
  page.on('pageerror', error => errors.push(error.message));
  await api(page, requests);
  await page.route('https://tile.openstreetmap.org/**', route => route.fulfill(background));
  await page.goto('/');
  await expect(page.locator('.competition-total strong')).toHaveText('501');
  await expect(page.getByText('Categories selected', { exact: true })).toHaveCount(0);
  expect(await page.locator('.competition-summary').evaluate(node =>
    Boolean(node.compareDocumentPosition(document.querySelector('.venue-map-panel')!) & Node.DOCUMENT_POSITION_FOLLOWING))).toBeTruthy();
  await expect(page.locator('.venue-row')).toHaveCount(25);
  await expect(page.locator('.venue-map [data-venue-id]')).toHaveCount(500);
  await expect(page.getByText('Showing 1–25 of 501', { exact: true })).toBeVisible();
  await expect(page.getByText(/500 overview points shown of 501/)).toBeVisible();
  await expect(page.locator('.leaflet-control-attribution').getByRole('link', { name: 'OpenStreetMap' })).toBeVisible();
  await expect(page.getByText('Coverage and venues outside these counts', { exact: true })).toHaveCount(0);
  await page.locator(`.venue-map [data-venue-id="${id(500)}"]`).click();
  await expect(page.locator('.selected-venue h4')).toHaveText('Venue 500');
  await expect(page.locator('.selected-venue')).toContainText('outside the current list page');
  await expect(page.locator(`.venue-map [data-venue-id="${id(500)}"]`)).toHaveClass(/selected-marker/);
  const first = page.getByRole('button', { name: /^Venue 1\b.*42 Shared Street/ });
  await first.focus();
  await page.keyboard.press('Enter');
  await expect(first).toHaveAttribute('aria-pressed', 'true');
  await expect(page.locator('.selected-venue h4')).toHaveText('Venue 1');
  await expect(page.getByRole('button', { name: /^Venue 2\b.*42 Shared Street/ })).toBeVisible();
  await expect(page.locator(`.venue-map [data-venue-id="${id(1)}"]`)).toHaveClass(/selected-marker/);
  await page.getByRole('button', { name: 'Next venues', exact: true }).click();
  await expect(page.getByText('Showing 26–50 of 501', { exact: true })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Venue list', exact: true })).toBeFocused();
  await expect(page.locator('.competition-total strong')).toHaveText('501');
  await page.getByRole('button', { name: 'Previous venues', exact: true }).click();
  await expect(page.getByText('Showing 1–25 of 501', { exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Next venues', exact: true }).click();
  await expect(page.getByText('Page 2', { exact: true })).toBeVisible();
  await page.getByRole('checkbox', { name: 'Cafés', exact: true }).uncheck();
  await expect(page.locator('.competition-total strong')).toHaveText('250');
  await expect(page.locator('.venue-map [data-venue-id]')).toHaveCount(250);
  await expect(page.getByText('Page 1', { exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Previous venues' })).toBeDisabled();
  await expect(page.locator('.selected-venue')).toHaveCount(0);
  await expect(page.locator('.venue-row').first()).toContainText('Venue 2');
  expect(requests.filter(url => !url.pathname.endsWith('/meta')).every(url => url.searchParams.get('release_id') === release)).toBeTruthy();
  expect(requests.filter(url => url.pathname.endsWith('/establishments')).every(url => url.searchParams.get('limit') === '25' && url.searchParams.get('map_limit') === '500')).toBeTruthy();
  expect(errors).toEqual([]);
});

test('failed tiles preserve keyboard research and the last list page can select a venue beyond the overview cap', async ({ page }) => {
  test.setTimeout(60_000);
  const requests: URL[] = [];
  const errors: string[] = [];
  page.on('pageerror', error => errors.push(error.message));
  await api(page, requests);
  let tilesAvailable = false;
  await page.route('https://tile.openstreetmap.org/**', route => tilesAvailable ? route.fulfill(background) : route.abort());
  await page.goto('/');
  await expect(page.getByText(/Map background unavailable/)).toBeVisible();
  await expect(page.locator('.competition-total strong')).toHaveText('501');
  const first = page.getByRole('button', { name: /^Venue 1\b.*42 Shared Street/ });
  await first.focus(); await page.keyboard.press('Enter');
  await expect(page.locator('.selected-venue h4')).toHaveText('Venue 1');
  tilesAvailable = true;
  await page.getByRole('button', { name: 'Retry map background' }).click();
  await expect(page.getByText(/Map background unavailable/)).toHaveCount(0);
  for (let number = 2; number <= 21; number++) {
    await page.getByRole('button', { name: 'Next venues', exact: true }).click();
    await expect(page.getByText(`Page ${number}`, { exact: true })).toBeVisible();
  }
  await expect(page.getByText('Showing 501–501 of 501', { exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Next venues' })).toBeDisabled();
  const last = page.getByRole('button', { name: /^Venue 501\b.*501 Harbour Road/ });
  await last.focus(); await page.keyboard.press('Enter');
  await expect(page.locator('.selected-venue h4')).toHaveText('Venue 501');
  await expect(page.locator(`.venue-map [data-venue-id="${id(501)}"]`)).toHaveClass(/selected-marker/);
  await expect(page.locator('.venue-map [data-venue-id]')).toHaveCount(501);
  await expect(page.getByText(/additional highlighted point/)).toBeVisible();
  await page.setViewportSize({ width: 375, height: 812 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBeTruthy();
  expect(errors).toEqual([]);
  await page.screenshot({ path: 'test-results/exploration-mobile.png', fullPage: true });
});
