import { expect, test } from '@playwright/test';
import type { Page } from '@playwright/test';
import type { AreaMetrics } from '../src/api/contracts';

const release = '11111111-1111-4111-8111-111111111111';
const cats = ['cafe', 'restaurant', 'takeaway', 'bakery', 'pub_bar', 'food_service'];
const codes = ['E01000001', 'E01000002', 'E01000003'];
const metric = (code: string | null, filtered = false, empty = false): AreaMetrics => ({
  area_code: code, area_name: code ? `Fixture ${code.slice(-1)}` : 'Brighton & Hove',
  geography_version: 'LSOA-2021', population_edition: '2024', population_reference_date: '2024-06-30',
  land_area_basis: 'Official land', land_area_edition: { edition: '2021' },
  establishment_count: empty ? 0 : filtered ? 3 : 10, category_mix: empty ? {} : filtered ? { restaurant: 3 } : { cafe: 7, restaurant: 3 },
  official_land_km2: code === codes[1] ? null : 2, population: 1000,
  establishments_per_land_km2: code === codes[1] ? null : empty ? 0 : filtered ? 1.5 : 5,
  land_rate_unavailable_reason: code === codes[1] ? 'official_land_missing' : null,
  establishments_per_1000_residents: empty ? 0 : filtered ? 3 : 10, resident_rate_unavailable_reason: null,
  population_per_land_km2: code === codes[1] ? null : 500,
  population_density_unavailable_reason: code === codes[1] ? 'official_land_missing' : null,
  age_0_15: 200, age_16_64: 600, age_65_plus: 200,
  age_0_15_percent: 20, age_16_64_percent: 60, age_65_plus_percent: 20, age_mix_unavailable_reason: null,
});
async function fixture(page: Page, requests: URL[], control: { busy: boolean; mismatched: boolean }) {
  await page.route('https://tile.openstreetmap.org/**', route => route.abort());
  await page.route('**/api/v1/**', async route => {
    const url = new URL(route.request().url()); requests.push(url);
    const path = url.pathname.split('/api/v1/')[1];
    const filtered = !url.searchParams.getAll('categories').includes('cafe');
    const empty = url.searchParams.get('empty_categories') === 'true';
    let data: unknown;
    if (path === 'meta') data = { categories: cats, radii: [250, 500, 1000, 2000], default_radius: 500, city: 'Brighton & Hove', industry: 'Hospitality' };
    else if (path === 'areas') data = codes.map((code, index) => ({ area_code: code, area_name: `Fixture ${index + 1}`, geography_version: 'LSOA-2021',
      display_geometry: { type: 'Polygon', coordinates: [[[-.18 + index * .02, 50.82], [-.16 + index * .02, 50.82],
        [-.16 + index * .02, 50.84], [-.18 + index * .02, 50.84], [-.18 + index * .02, 50.82]]] } }));
    else if (path.endsWith('/summary')) data = { area: metric(path.split('/')[1], filtered, empty), city_benchmark: metric(null, filtered, empty) };
    else if (path === 'compare') {
      if (control.busy) { await route.fulfill({ status: 503, json: { error: { code: 'unavailable', message: 'Temporarily unavailable' } } }); return; }
      data = { areas: url.searchParams.getAll('area_codes').reverse().map(code => ({ ...metric(code, filtered, empty),
        population_edition: control.mismatched ? 'other' : '2024' })), city_benchmark: metric(null, filtered, empty) };
    } else if (path === 'establishments') data = { total: 10, items: [], map_points: [], has_more: false,
      map_truncated: false, category_mix: { cafe: 7, restaurant: 3 }, partial_coverage: false, unknown_location_scope: 'city' };
    else throw new Error(path);
    await route.fulfill({ json: { release_id: release, applied_filters: {}, coverage: [], null_reasons: {},
      metadata: { published_at: '2026-11-07T12:00:00Z', geography_version: 'LSOA-2021', population_edition: '2024',
        population_reference_date: '2024-06-30', taxonomy_version: 'fixture', sources: [], attribution: [] }, data } });
  });
  await page.goto('/#compare');
}

test('boundary clicks preview an area and compare distinct areas with reconciled counts, shares and resident context', async ({ page }) => {
  const requests: URL[] = [];
  await fixture(page, requests, { busy: false, mismatched: false });
  await expect(page.locator('.area-map [data-area-code]')).toHaveCount(3);
  const boundary = page.locator(`[data-area-code="${codes[0]}"]`);
  const initialBoundary = await boundary.getAttribute('d');
  await page.locator('.area-map').hover();
  await page.mouse.wheel(0, -150);
  await expect.poll(() => boundary.getAttribute('d')).not.toBe(initialBoundary);
  const zoomedBoundary = await boundary.getAttribute('d');
  await page.mouse.wheel(0, 150);
  await expect.poll(() => boundary.getAttribute('d')).not.toBe(zoomedBoundary);
  await page.locator(`[data-area-code="${codes[0]}"]`).click();
  await expect(page.getByRole('combobox', { name: 'First neighbourhood', exact: true })).toHaveValue(codes[0]);
  await expect(page.locator('.single-preview')).toContainText('Fixture 1');
  await expect(page.locator('.single-preview')).toContainText('70.0%');
  await page.locator(`[data-area-code="${codes[0]}"]`).click();
  await expect(page.getByRole('combobox', { name: 'Second neighbourhood', exact: true })).toHaveValue('');
  await page.locator(`[data-area-code="${codes[1]}"]`).click();
  await expect(page.locator('.comparison-cards .area-card')).toHaveCount(2);
  await expect(page.locator('.comparison-cards .area-card h3')).toHaveText(['Fixture 1', 'Fixture 2']);
  await expect(page.locator('.comparison-cards .area-card').nth(1)).toContainText('Unavailable — official land missing');
  await expect(page.locator('.comparison-cards .area-card').first()).toContainText('30/06/2024');
  await expect(page.locator('.comparison-cards .area-card').first()).toContainText('07/11/2026');
  await expect(page.locator('.city-comparison')).toContainText('Brighton & Hove');
  await expect(page.locator(`[data-area-code="${codes[1]}"]`)).toHaveAttribute('data-selected-slot', '2');
  await page.getByLabel('Cafés', { exact: true }).uncheck();
  await expect(page.locator('.comparison-cards .area-card').first().locator('.area-metrics dd').first()).toHaveText('3');
  await expect(page.locator('.comparison-cards .area-card').first().getByRole('table')).toContainText('100.0%');
  await expect(page.locator('.comparison-cards .area-card').first()).toContainText('1,000');
  const last = requests.filter(url => url.pathname.endsWith('/compare')).at(-1)!;
  expect(last.searchParams.getAll('area_codes')).toEqual(codes.slice(0, 2));
  expect(last.searchParams.getAll('categories')).not.toContain('cafe');
  expect(last.searchParams.get('release_id')).toBe(release);
  await expect(page.getByRole('combobox', { name: 'First neighbourhood', exact: true }).locator('option')).toHaveCount(4);
  await page.getByRole('link', { name: 'Explore', exact: true }).click();
  await page.getByRole('link', { name: 'Compare', exact: true }).click();
  await expect(page.getByRole('combobox', { name: 'Second neighbourhood', exact: true })).toHaveValue(codes[1]);
});

test('keyboard selection, missing values, failures, edition mismatch and zero shares stay honest on mobile', async ({ page }) => {
  await page.setViewportSize({ width: 375, height: 812 });
  const control = { busy: true, mismatched: false };
  await fixture(page, [], control);
  await page.getByRole('combobox', { name: 'First neighbourhood', exact: true }).selectOption(codes[0]);
  await page.getByRole('combobox', { name: 'Second neighbourhood', exact: true }).selectOption(codes[1]);
  await expect(page.getByRole('heading', { name: 'Data is temporarily unavailable' })).toBeVisible();
  await expect(page.locator('.comparison-cards')).toHaveCount(0);
  control.busy = false;
  await page.getByRole('button', { name: 'Try again', exact: true }).click();
  await expect(page.locator('.comparison-cards .area-card')).toHaveCount(2);
  await expect(page.getByText(/Map background unavailable/)).toBeVisible();
  control.mismatched = true;
  await page.getByLabel('Cafés', { exact: true }).uncheck();
  await expect(page.getByRole('alert')).toContainText('data editions do not match');
  await expect(page.locator('.comparison-cards')).toHaveCount(0);
  control.mismatched = false;
  await page.getByRole('button', { name: 'Clear', exact: true }).click();
  await expect(page.locator('.comparison-cards .area-card')).toHaveCount(2);
  await expect(page.locator('.comparison-cards .area-card').first().locator('.area-metrics dd').first()).toHaveText('0');
  await expect(page.locator('.comparison-cards .area-card').first()).toContainText('Shares are unavailable');
  await expect(page.locator('.comparison-cards .area-card').nth(1)).toContainText('Unavailable — official land missing');
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBeTruthy();
});
