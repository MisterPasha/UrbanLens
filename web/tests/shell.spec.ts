import { expect, test } from '@playwright/test';
import type { AreaMetrics, Envelope_Meta_ } from '../src/api/contracts';

const first = '11111111-1111-4111-8111-111111111111';
const second = '22222222-2222-4222-8222-222222222222';
const all = ['cafe', 'restaurant', 'takeaway', 'bakery', 'pub_bar', 'food_service'];
test.beforeEach(async ({ page }) => {
  await page.route('https://tile.openstreetmap.org/**', route => route.fulfill({
    contentType: 'image/svg+xml', body: '<svg xmlns="http://www.w3.org/2000/svg" width="256" height="256"><rect width="256" height="256" fill="#e5eee7"/></svg>',
  }));
});
const metadata: Envelope_Meta_['metadata'] = {
  published_at: '2026-11-07T12:00:00Z', geography_version: 'LSOA-2021',
  population_edition: 'fixture', population_reference_date: '2024-06-30', taxonomy_version: 'fixture',
  sources: [{ provider: 'fsa', status: 'healthy' }], attribution: ['OpenStreetMap contributors: ODbL'],
};
const area = (code: string): AreaMetrics => ({
  area_code: code, area_name: code === 'E01000001' ? 'Test area one' : 'Test area two',
  geography_version: 'LSOA-2021', population_edition: 'fixture', population_reference_date: '2024-06-30',
  land_area_basis: 'fixture', land_area_edition: {}, establishment_count: 7, official_land_km2: 1,
  population: 100, establishments_per_land_km2: 7, land_rate_unavailable_reason: null,
  establishments_per_1000_residents: 70, resident_rate_unavailable_reason: null,
  population_per_land_km2: 100, population_density_unavailable_reason: null,
  age_0_15: 20, age_16_64: 60, age_65_plus: 20, age_0_15_percent: 20,
  age_16_64_percent: 60, age_65_plus_percent: 20, age_mix_unavailable_reason: null, category_mix: { cafe: 7 },
});

function envelope(data: unknown, release = first, stale = false) {
  return { api_version: 'v1', release_id: release, applied_filters: {},
    metadata: { ...metadata, sources: stale ? [{ provider: 'fsa', status: 'stale', retained_older_snapshot: true }] : metadata.sources },
    coverage: [], null_reasons: {}, data };
}

function content(path: string, search: URLSearchParams) {
  if (path === 'meta') return { categories: all, radii: [250, 500, 1000, 2000],
    default_radius: 500, city: 'Brighton & Hove', industry: 'Hospitality' };
  if (path === 'areas') return ['E01000001', 'E01000002'].map(code => ({
    area_code: code, area_name: area(code).area_name, geography_version: 'LSOA-2021', display_geometry: {},
  }));
  if (path.startsWith('areas/') && path.endsWith('/summary')) return { area: area(path.split('/')[1]), city_benchmark: area('city') };
  if (path === 'establishments') {
    const selected = search.getAll('categories');
    const n = search.get('empty_categories') === 'true' ? 0 : selected.length === 6 ? 100
      : selected.length === 5 ? 42 : 7;
    return { total: n, items: [], has_more: false, next_cursor: null, map_points: [], map_truncated: false,
      category_mix: Object.fromEntries(selected.map((category, index) => [category, index === 0 ? n : 0])),
      partial_coverage: false, unknown_location_scope: 'city-wide' };
  }
  if (path === 'compare') return { areas: [area('E01000001'), area('E01000002')], city_benchmark: area('city') };
  if (path === 'companies') return { total: 0, items: [], has_more: false, interpretation: 'Legal entities' };
  if (path === 'activity') return { total: 0, items: [], has_more: false, baselines: [],
    interpretation: 'Not verified openings or closures', context_note: 'Synthetic browser fixture' };
  throw new Error(`Unexpected API route: ${path}`);
}

test('navigation preserves filters and radius/area selections; late results cannot replace newer results', async ({ page }) => {
  const errors: string[] = [];
  const requested: URL[] = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.route('**/api/v1/**', async route => {
    const url = new URL(route.request().url());
    requested.push(url);
    const path = url.pathname.split('/api/v1/')[1];
    if (path === 'establishments' && url.searchParams.getAll('categories').length === 5) {
      await new Promise(resolve => setTimeout(resolve, 450));
    }
    await route.fulfill({ json: envelope(content(path, url.searchParams)) }).catch(() => {});
  });
  await page.goto('/');
  const total = page.locator('.competition-total strong');
  await expect(total).toHaveText('100');
  const olderRequest = page.waitForRequest(request => request.url().includes('/establishments?')
    && new URL(request.url()).searchParams.getAll('categories').length === 5);
  await page.getByLabel('Cafés', { exact: true }).uncheck();
  await olderRequest;
  await page.getByLabel('Restaurants', { exact: true }).uncheck();
  await expect(total).toHaveText('7');
  await page.waitForTimeout(550); // Give the deliberately obsolete response time to arrive.
  await expect(total).toHaveText('7');
  await page.getByRole('link', { name: 'Compare', exact: true }).click();
  await page.getByRole('combobox', { name: 'First neighbourhood', exact: true }).selectOption('E01000001');
  await page.getByRole('combobox', { name: 'Second neighbourhood', exact: true }).selectOption('E01000002');
  await expect(page.getByRole('heading', { name: 'Test area one', exact: true })).toBeVisible();
  await page.getByRole('link', { name: 'Companies', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'No companies in this scope' })).toBeVisible();
  await page.getByRole('link', { name: 'Compare', exact: true }).click();
  await expect(page.getByRole('combobox', { name: 'First neighbourhood', exact: true })).toHaveValue('E01000001');
  await expect(page.getByLabel('Cafés', { exact: true })).not.toBeChecked();
  await page.getByRole('link', { name: 'Explore', exact: true }).click();
  await page.getByRole('combobox', { name: 'Search scope', exact: true }).selectOption('radius');
  await page.getByRole('combobox', { name: 'Search radius', exact: true }).selectOption('1000');
  await page.getByText('Enter coordinates', { exact: true }).click();
  await page.getByLabel('Longitude', { exact: true }).fill('-0.14');
  await page.getByLabel('Latitude', { exact: true }).fill('50.82');
  await page.getByRole('button', { name: 'Apply centre' }).click();
  await expect(total).toHaveText('7');
  await page.getByRole('link', { name: 'Activity', exact: true }).click();
  await expect(page.getByText('Observed changes', { exact: true })).toBeVisible();
  await page.getByRole('link', { name: 'Data & methodology' }).click();
  await expect(page.getByRole('heading', { name: 'Source attribution' })).toBeVisible();
  await page.getByRole('link', { name: 'Explore', exact: true }).click();
  await expect(page.getByRole('combobox', { name: 'Search scope', exact: true })).toHaveValue('radius');
  await expect(page.getByRole('combobox', { name: 'Search radius', exact: true })).toHaveValue('1000');
  await page.getByText('Enter coordinates', { exact: true }).click();
  await expect(page.getByLabel('Longitude', { exact: true })).toHaveValue('-0.14');
  await page.getByRole('link', { name: 'Skip to research' }).focus();
  await page.keyboard.press('Enter');
  await expect(page.locator('main')).toBeFocused();
  expect(requested.filter(url => !url.pathname.endsWith('/meta'))
    .every(url => url.searchParams.get('release_id') === first)).toBeTruthy();
  expect(errors).toEqual([]);
  await page.screenshot({ path: 'test-results/shell-desktop.png', fullPage: true });
});

test('unavailable, throttled, expired, stale and empty states recover without losing selections on mobile', async ({ page }) => {
  await page.setViewportSize({ width: 375, height: 812 });
  let bootstrapUnavailable = true;
  let busy = true;
  let pin = first;
  const requested: URL[] = [];
  await page.route('**/api/v1/**', async route => {
    const url = new URL(route.request().url());
    requested.push(url);
    const path = url.pathname.split('/api/v1/')[1];
    if (path === 'meta' && bootstrapUnavailable) {
      await route.fulfill({ status: 503, contentType: 'text/html', body: '<h1>Unavailable</h1>' });
      return;
    }
    if (path === 'establishments' && busy) {
      await route.fulfill({ status: 429, json: { error: { code: 'rate_limited', message: 'Please wait', request_id: 'fixture' } } });
      return;
    }
    if (path === 'compare' && url.searchParams.get('release_id') === first) {
      await route.fulfill({ status: 410, json: { error: { code: 'release_expired', message: 'Expired', request_id: 'fixture', refresh_required: true } } });
      return;
    }
    await route.fulfill({ json: envelope(content(path, url.searchParams), pin, pin === second) });
  });
  await page.goto('/');
  await expect(page.getByRole('heading', { name: 'Data is temporarily unavailable' })).toBeVisible();
  await page.getByLabel('Cafés', { exact: true }).uncheck();
  bootstrapUnavailable = false;
  await page.getByRole('button', { name: 'Try again' }).click();
  await expect(page.getByRole('heading', { name: 'Service is busy' })).toBeVisible();
  busy = false;
  await page.getByRole('button', { name: 'Try again' }).click();
  await expect(page.locator('.competition-total strong')).toHaveText('42');
  await page.getByRole('link', { name: 'Compare', exact: true }).click();
  await page.getByRole('combobox', { name: 'First neighbourhood', exact: true }).selectOption('E01000001');
  await page.getByRole('combobox', { name: 'Second neighbourhood', exact: true }).selectOption('E01000002');
  await expect(page.getByRole('heading', { name: 'This data version has expired' })).toBeVisible();
  await expect(page.locator('.comparison-cards')).toHaveCount(0);
  pin = second;
  await page.getByRole('button', { name: 'Continue with current data' }).click();
  await expect(page.getByRole('heading', { name: 'Test area one', exact: true })).toBeVisible();
  await expect(page.getByRole('combobox', { name: 'First neighbourhood', exact: true })).toHaveValue('E01000001');
  await expect(page.getByLabel('Cafés', { exact: true })).not.toBeChecked();
  await expect(page.getByText('Some source data is older than expected', { exact: true })).toBeVisible();
  await expect(page.getByText('Some sources have limited availability', { exact: true })).toBeVisible();
  await page.getByRole('link', { name: 'Explore', exact: true }).click();
  await page.getByRole('button', { name: 'Clear', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'No matching venues' })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBeTruthy();
  expect([...new Set(requested.filter(url => url.pathname.endsWith('/compare'))
    .map(url => url.searchParams.get('release_id')))])
    .toEqual([first, second]);
  await page.screenshot({ path: 'test-results/shell-mobile.png', fullPage: true });
});
