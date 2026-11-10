import { expect, test } from '@playwright/test';
import type { Page } from '@playwright/test';
import type { VenueDetail } from '../src/api/contracts';

const release = '11111111-1111-4111-8111-111111111111';
const id = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const cats = ['cafe', 'restaurant', 'takeaway', 'bakery', 'pub_bar', 'food_service'];
function detail(n: number): VenueDetail {
  const withheld = n === 2;
  return { establishment_id: id(n), display_name: `Evidence venue ${n}`, name_unavailable: false,
    primary_category: 'cafe', secondary_categories: ['food_service'], address: withheld ? null : '42 Evidence Street',
    postcode: withheld ? null : 'BN1 1AA', area_code: withheld ? null : 'E01000001',
    longitude: withheld ? null : -.14, latitude: withheld ? null : 50.82, location_quality: withheld ? 'withheld' : 'source_point',
    eligibility: 'eligible', duplicate_status: 'resolved', observation_state: 'observed', primary_spatial_eligible: !withheld,
    first_observed_at: '2026-11-01T12:00:00Z', last_observed_at: '2026-11-04T12:00:00Z', source_mix: 'fsa+osm',
    exclusion_reasons: withheld ? ['location_withheld'] : [], null_reasons: withheld ? { address: 'withheld', location: 'withheld' } : {},
    hygiene: withheld ? [] : [
      { source_key: '1234', observed_at: '2026-11-04T12:00:00Z', rating: { state: 'rated', score: 0,
        assessed_on: { state: 'published', value: '2023-06-15' } } },
      { source_key: '1235', rating: { state: 'awaiting_inspection', score: null, assessed_on: { state: 'missing', value: null } } },
      { source_key: '1236', rating: { state: 'exempt', score: null, assessed_on: { state: 'missing', value: null } } },
    ],
    sources: [{ provider: 'fsa', source_key: '1234', observed_at: '2026-11-04T12:00:00Z', state: 'observed',
      url: withheld ? 'javascript:alert(1)' : 'https://ratings.food.gov.uk/business/1234' },
    { provider: 'osm', source_key: 'node/456', observed_at: '2026-11-04T12:00:00Z', state: 'observed',
      url: withheld ? 'https://www.openstreetmap.org.evil.example/node/456' : 'https://www.openstreetmap.org/node/456' }],
    provenance: { display_name: { reason: 'Current OSM name preference', reviewed: true, source_time: '2026-11-04T12:00:00Z' },
      location: { reason: withheld ? 'PRIVATE ADDRESS MUST NOT RENDER' : 'Accepted source point', reviewed: true },
      address: { reason: withheld ? 'PRIVATE ADDRESS MUST NOT RENDER' : 'Accepted FSA address', reviewed: true } },
    verified_companies: withheld ? [] : [{ company_number: '00000001', relationship: 'operator', reviewed_at: null,
      reviewed_on: '2026-11-02', url: 'https://find-and-update.company-information.service.gov.uk/company/00000001' }],
  };
}
async function fixture(page: Page, control: { busy: boolean; delay: boolean }) {
  await page.route('https://tile.openstreetmap.org/**', route => route.abort());
  await page.route('**/api/v1/**', async route => {
    const url = new URL(route.request().url()), path = url.pathname.split('/api/v1/')[1];
    let data: unknown;
    if (path === 'meta') data = { categories: cats, radii: [250, 500, 1000, 2000], default_radius: 500, city: 'Brighton & Hove', industry: 'Hospitality' };
    else if (path === 'areas') data = [];
    else if (path === 'establishments') data = { total: 2, items: [1, 2].map(n => ({ ...detail(n),
      longitude: -.14 + n * .001, latitude: 50.82, location_quality: 'source_point' })),
      map_points: [], map_truncated: false, has_more: false, category_mix: { cafe: 2 }, partial_coverage: false, unknown_location_scope: 'city' };
    else if (path.startsWith('establishments/')) {
      if (control.busy) { await route.fulfill({ status: 503, json: { error: { code: 'unavailable', message: 'Unavailable' } } }); return; }
      const n = path.endsWith(id(1)) ? 1 : 2;
      if (n === 1 && control.delay) await new Promise(resolve => setTimeout(resolve, 700));
      data = detail(n);
    } else throw new Error(path);
    await route.fulfill({ json: { release_id: release, applied_filters: {}, coverage: [], null_reasons: {},
      metadata: { published_at: '2026-11-07T12:00:00Z', geography_version: 'LSOA-2021', population_edition: 'fixture',
        population_reference_date: '2024-06-30', taxonomy_version: 'fixture', sources: [{ provider: 'fsa', status: 'stale',
          last_success_at: '2026-11-04T12:00:00Z', retained_older_snapshot: true }], attribution: [] }, data } }).catch(() => {});
  });
  await page.goto('/');
  await expect(page.locator('.venue-row')).toHaveCount(2);
}

test('selected venue shows a concise summary, separate hygiene states and public source records', async ({ page }) => {
  await fixture(page, { busy: false, delay: false });
  await page.locator('.venue-row').first().focus(); await page.keyboard.press('Enter');
  const panel = page.locator('.selected-venue');
  await expect(panel.locator('h4')).toHaveText('Evidence venue 1');
  for (const name of ['Identity and inclusion', 'Location evidence', 'Field decisions', 'Verified company context']) {
    await expect(panel.getByRole('region', { name, exact: true })).toHaveCount(0);
  }
  const hygiene = panel.getByRole('region', { name: 'Food hygiene evidence' });
  await expect(hygiene).toContainText('Food hygiene rating: 0 / 5');
  await expect(hygiene).toContainText('Awaiting inspection');
  await expect(hygiene).toContainText('Exempt');
  await expect(hygiene).toContainText('15/06/2023');
  await expect(hygiene).toContainText('04/11/2026');
  await expect(hygiene).toContainText('older accepted snapshot');
  await expect(hygiene).toContainText('not a competition score');
  await expect(panel.getByRole('link', { name: 'View public source record' })).toHaveCount(2);
  await page.getByRole('button', { name: 'Clear selection', exact: true }).click();
  await expect(panel).toHaveCount(0);
});

test('detail failures recover; obsolete responses, withheld fields and unverified links stay unavailable on mobile', async ({ page }) => {
  await page.setViewportSize({ width: 375, height: 812 });
  const control = { busy: true, delay: false };
  await fixture(page, control);
  await page.locator('.venue-row').first().click();
  await expect(page.locator('.selected-venue').getByRole('heading', { name: 'Data is temporarily unavailable' })).toBeVisible();
  control.busy = false; control.delay = true;
  await page.getByRole('button', { name: 'Try again', exact: true }).click();
  await page.locator('.venue-row').nth(1).click();
  const panel = page.locator('.selected-venue');
  await expect(panel.locator('h4')).toHaveText('Evidence venue 2');
  await page.waitForTimeout(800); // The deliberately obsolete first detail must not replace the selection.
  await expect(panel.locator('h4')).toHaveText('Evidence venue 2');
  await expect(panel).toContainText('Address withheld by source');
  await expect(panel).not.toContainText('PRIVATE ADDRESS MUST NOT RENDER');
  await expect(panel).not.toContainText('BN1 1AA');
  await expect(panel).not.toContainText('-0.140000');
  await expect(panel).toContainText('This does not mean a zero rating');
  await expect(panel.getByRole('link')).toHaveCount(0);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBeTruthy();
});
