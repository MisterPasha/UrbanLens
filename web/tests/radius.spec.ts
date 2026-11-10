import { expect, test } from '@playwright/test';
import type { Page } from '@playwright/test';

const release = '11111111-1111-4111-8111-111111111111';
const categories = ['cafe', 'restaurant', 'takeaway', 'bakery', 'pub_bar', 'food_service'];
function envelope(data: unknown) {
  return { release_id: release, metadata: { published_at: '2026-11-07T12:00:00Z',
    geography_version: 'LSOA-2021', population_edition: 'fixture', population_reference_date: '2024-06-30',
    taxonomy_version: 'fixture', sources: [], attribution: [] },
    applied_filters: {}, coverage: [], null_reasons: {}, data };
}
async function fixture(page: Page, requests: URL[]) {
  await page.route('https://tile.openstreetmap.org/**', route => route.abort());
  await page.route('**/api/v1/**', async route => {
    const url = new URL(route.request().url()); requests.push(url);
    const endpoint = url.pathname.split('/api/v1/')[1];
    let data: unknown;
    if (endpoint === 'meta') data = { categories, radii: [250, 500, 1000, 2000], default_radius: 500, city: 'Brighton & Hove', industry: 'Hospitality' };
    else if (endpoint === 'areas') data = [];
    else if (endpoint.startsWith('postcodes/')) {
      const postcode = endpoint.split('/')[1];
      const state = ({ BN11AB: 'unmatched', BN11AJ: 'outside', BN11AD: 'incompatible_geography',
        BN11AE: 'provider_failure', BN11AF: 'throttled' } as Record<string, string>)[postcode] ?? 'resolved';
      if (postcode === 'BN11AG') {
        await route.fulfill({ status: 429, json: { error: { code: 'rate_limited', message: 'Wait' } } }); return;
      }
      if (postcode === 'BN11AH') await new Promise(resolve => setTimeout(resolve, 800));
      data = { postcode, state, can_define_search: state === 'resolved', longitude: -.14, latitude: 50.82,
        approximate: true, area_code: 'E01000001' };
    } else if (endpoint === 'establishments') {
      if (url.searchParams.get('longitude') === '0') {
        await route.fulfill({ status: 422, json: { error: { code: 'unsupported_scope', message: 'Outside' } } }); return;
      }
      const selected = url.searchParams.getAll('categories');
      const n = url.searchParams.get('empty_categories') === 'true' ? 0 : selected.includes('cafe') ? 8 : 3;
      data = { total: n, items: [], map_points: [], has_more: false, next_cursor: null, map_truncated: false,
        category_mix: { cafe: selected.includes('cafe') ? 5 : 0, restaurant: n ? 3 : 0 },
        partial_coverage: url.searchParams.get('mode') === 'radius', unknown_location_scope: 'city-wide' };
    } else throw new Error(`Unexpected endpoint ${endpoint}`);
    await route.fulfill({ json: envelope(data) }).catch(() => {});
  });
  await page.goto('/');
  await expect(page.locator('.competition-total strong')).toHaveText('8');
  await page.getByRole('combobox', { name: 'Search scope', exact: true }).selectOption('radius');
}
async function postcode(page: Page, value: string) {
  await page.getByLabel('Full UK postcode', { exact: true }).fill(value);
  await page.getByRole('button', { name: 'Use postcode', exact: true }).click();
}

test('map centre controls remain clickable while choosing a point', async ({ page }) => {
  for (const width of [1280, 375]) {
    await page.setViewportSize({ width, height: 800 });
    await fixture(page, []);
    await page.getByRole('button', { name: 'Choose centre on map', exact: true }).click();
    const controls = page.locator('.map-centre-controls');
    const map = page.locator('.venue-map');
    await expect(map).toHaveClass(/\bleaflet-container\b/);
    const controlBox = await controls.boundingBox();
    const mapBox = await map.boundingBox();
    expect(controlBox && mapBox && mapBox.y >= controlBox.y + controlBox.height).toBeTruthy();
    await page.getByRole('button', { name: 'Cancel map selection', exact: true }).click();
    await page.getByRole('button', { name: 'Choose centre on map', exact: true }).click();
    await page.getByRole('button', { name: 'Use map view as centre', exact: true }).click();
    await expect(page.getByRole('region', { name: 'Applied radius search' })).toContainText('Map point');
  }
});

test('postcode circle, presets, filters, zero results and explicit map selection use the same pinned query', async ({ page }) => {
  const requests: URL[] = [];
  await fixture(page, requests);
  await expect(page.locator('.search-circle')).toHaveCount(0);
  await postcode(page, 'bn1 1aa');
  const circle = page.locator('.search-circle');
  await expect(circle).toHaveAttribute('data-radius', '500');
  await expect(circle).toHaveAttribute('data-longitude', '-0.14');
  await expect(circle).toHaveAttribute('data-latitude', '50.82');
  const initialCircle = await circle.getAttribute('d');
  await page.locator('.venue-map').hover();
  await page.mouse.wheel(0, -150);
  await expect.poll(() => circle.getAttribute('d')).not.toBe(initialCircle);
  const zoomedCircle = await circle.getAttribute('d');
  await page.mouse.wheel(0, 150);
  await expect.poll(() => circle.getAttribute('d')).not.toBe(zoomedCircle);
  await expect(page.getByRole('region', { name: 'Applied radius search' })).toContainText('BN1 1AA');
  await expect(page.getByText(/Part of this circle is outside/)).toBeVisible();
  await expect(page.getByText(/Population and age estimates are not calculated/)).toBeVisible();
  for (const radius of ['250', '1000', '2000', '500']) {
    await page.getByRole('combobox', { name: 'Search radius', exact: true }).selectOption(radius);
    await expect(circle).toHaveAttribute('data-radius', radius);
    await expect(page.locator('.competition-total strong')).toHaveText('8');
    expect(requests.some(url => url.searchParams.get('radius') === radius
      && url.searchParams.get('longitude') === '-0.14' && url.searchParams.get('latitude') === '50.82')).toBeTruthy();
  }
  await page.getByLabel('Cafés', { exact: true }).uncheck();
  await expect(page.locator('.competition-total strong')).toHaveText('3');
  await expect(circle).toHaveAttribute('data-radius', '500');
  await page.getByRole('button', { name: 'Clear', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'No matching venues' })).toBeVisible();
  await expect(circle).toBeVisible();
  await page.getByRole('button', { name: 'Choose centre on map', exact: true }).click();
  await page.locator('.venue-map').click({ position: { x: 180, y: 150 } });
  await expect(page.getByRole('region', { name: 'Applied radius search' })).toContainText('Map point');
  const longitude = await circle.getAttribute('data-longitude');
  await page.locator('.venue-map').click({ position: { x: 200, y: 180 } });
  await expect(circle).toHaveAttribute('data-longitude', longitude!);
  await page.getByRole('link', { name: 'Data & methodology' }).click();
  await page.getByRole('link', { name: 'Explore', exact: true }).click();
  await expect(circle).toHaveAttribute('data-longitude', longitude!);
  await page.setViewportSize({ width: 375, height: 812 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBeTruthy();
  expect(requests.filter(url => !url.pathname.endsWith('/meta')).every(url => url.searchParams.get('release_id') === release)).toBeTruthy();
});

test('invalid, unsupported and failed lookups preserve the centre; a late postcode cannot override a map choice', async ({ page }) => {
  const requests: URL[] = [];
  await fixture(page, requests);
  await postcode(page, 'BN1 1AA');
  const circle = page.locator('.search-circle');
  await expect(circle).toHaveAttribute('data-longitude', '-0.14');
  for (const [value, message] of [['invalid', /valid full UK postcode/], ['BN1 1AB', /was not found/],
    ['BN1 1AJ', /outside the supported/], ['BN1 1AD', /cannot be matched/], ['BN1 1AE', /temporarily unavailable/],
    ['BN1 1AF', /temporarily limited/], ['BN1 1AG', /temporarily limited/]] as const) {
    await postcode(page, value);
    await expect(page.getByRole('alert')).toContainText(message);
    await expect(circle).toHaveAttribute('data-longitude', '-0.14');
    await expect(page.locator('.competition-total strong')).toHaveText('8');
  }
  expect(requests.some(url => url.pathname.includes('/postcodes/INVALID'))).toBeFalsy();
  await page.getByText('Enter coordinates', { exact: true }).click();
  await page.getByLabel('Longitude', { exact: true }).fill('0');
  await page.getByLabel('Latitude', { exact: true }).fill('51');
  await page.getByRole('button', { name: 'Apply centre' }).click();
  await expect(page.getByRole('alert')).toContainText('This point is outside');
  await expect(circle).toHaveAttribute('data-longitude', '-0.14');
  await postcode(page, 'BN1 1AH');
  await expect(page.getByRole('status').filter({ hasText: 'Looking up postcode' })).toBeVisible();
  await page.getByRole('button', { name: 'Use map view as centre', exact: true }).click();
  await expect(page.getByRole('region', { name: 'Applied radius search' })).toContainText('Map point');
  await page.waitForTimeout(900); // Deliberately late provider response must remain obsolete.
  await expect(page.getByRole('region', { name: 'Applied radius search' })).toContainText('Map point');
});
