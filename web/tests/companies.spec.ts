import { expect, test } from '@playwright/test';
import type { Page } from '@playwright/test';
import type { CompanyDetail } from '../src/api/contracts';

const release = '11111111-1111-4111-8111-111111111111';
const number = (n: number) => String(n).padStart(8, '0');
const venueId = '00000000-0000-4000-8000-000000000001';
function company(n: number): CompanyDetail {
  return { company_number: number(n), legal_name: `Company ${n} Limited`, legal_status: n === 2 ? 'dissolved' : 'active',
    company_type: 'ltd', sic_codes: ['56101'], incorporation_date: '2010-01-02', cessation_date: n === 2 ? '2025-06-03' : null,
    registered_office: n === 2 ? { address_state: 'withheld', address: 'DO NOT DISPLAY WITHHELD ADDRESS', postcode: 'SECRET' }
      : { address_state: 'published', address: '42 Contact Street', postcode: 'BN1 1AA' },
    registered_office_postcode: n === 2 ? 'SECRET' : 'BN1 1AA', cohorts: ['registered_office'],
    office_caveat: 'Registered office is not trading premises', first_observed_at: '2026-11-02T12:00:00Z',
    last_observed_at: '2026-11-04T12:00:00Z', profile_observed_at: null,
    url: n === 2 ? 'javascript:alert(1)' : `https://find-and-update.company-information.service.gov.uk/company/${number(n)}`,
    verified_premises: n === 1 ? [{ establishment_id: venueId, display_name: 'Linked cafe', relationship: 'operator',
      reviewed_at: null, reviewed_on: '2026-11-01', evidence_url: null, primary_spatial_eligible: true }] : [] };
}
async function fixture(page: Page, control: { busy: boolean; delay: boolean }, requests: URL[]) {
  await page.route('https://tile.openstreetmap.org/**', route => route.abort());
  await page.route('**/api/v1/**', async route => {
    const url = new URL(route.request().url()); requests.push(url);
    const path = url.pathname.split('/api/v1/')[1]; let data: unknown;
    if (path === 'meta') data = { city: 'Brighton & Hove', industry: 'Hospitality', categories: ['cafe'], radii: [250, 500, 1000, 2000], default_radius: 500 };
    else if (path === 'areas') data = [];
    else if (path === 'companies') {
      if (control.busy) { await route.fulfill({ status: 503, json: { error: { code: 'unavailable', message: 'Unavailable' } } }); return; }
      let items = Array.from({ length: 26 }, (_, i) => company(i + 1));
      const name = url.searchParams.get('name');
      if (name) items = items.filter(item => item.legal_name.toLowerCase().includes(name.toLowerCase()));
      if (name === 'Company 1' && control.delay) await new Promise(resolve => setTimeout(resolve, 700));
      const offset = Number(url.searchParams.get('cursor') ?? 0);
      data = { total: items.length, items: items.slice(offset, offset + 25), has_more: offset + 25 < items.length,
        next_cursor: offset + 25 < items.length ? String(offset + 25) : null, interpretation: 'Legal entities' };
    } else if (path.startsWith('companies/')) data = company(Number(path.split('/')[1]));
    else if (path.startsWith('establishments/')) data = { establishment_id: venueId, display_name: 'Linked cafe', name_unavailable: false,
      primary_category: 'cafe', secondary_categories: [], address: '8 Trading Street', postcode: 'BN1 1AA', location_quality: 'source_point',
      longitude: -.14, latitude: 50.82, area_code: 'E01000001', eligibility: 'eligible', duplicate_status: 'resolved', observation_state: 'observed',
      primary_spatial_eligible: true, first_observed_at: '2026-11-01T12:00:00Z', last_observed_at: '2026-11-04T12:00:00Z',
      source_mix: 'osm', exclusion_reasons: [], hygiene: [], sources: [], provenance: {}, null_reasons: {}, verified_companies: [] };
    else if (path === 'establishments') data = { total: 0, items: [], map_points: [], category_mix: {}, has_more: false, map_truncated: false, partial_coverage: false };
    else throw new Error(path);
    await route.fulfill({ json: { release_id: release, applied_filters: {}, coverage: [], null_reasons: {},
      metadata: { published_at: '2026-11-07T12:00:00Z', geography_version: 'LSOA-2021', population_edition: 'fixture',
        population_reference_date: '2024-06-30', taxonomy_version: 'fixture', sources: [], attribution: [] }, data } }).catch(() => {});
  });
  await page.goto('/#companies');
}

test('company pages preserve legal numbers and navigation, and verified links open accepted venue evidence', async ({ page }) => {
  const requests: URL[] = [];
  await fixture(page, { busy: false, delay: false }, requests);
  await expect(page.locator('.company-row')).toHaveCount(25);
  await page.locator('.company-row').first().focus(); await page.keyboard.press('Enter');
  const panel = page.getByRole('region', { name: 'Selected company', exact: true });
  await expect(panel).toContainText('00000001');
  await expect(panel).toContainText('02/01/2010');
  await expect(panel).toContainText('56101');
  await expect(panel).toContainText('42 Contact Street');
  await expect(panel.getByRole('link', { name: 'View Companies House record' })).toHaveAttribute('href',
    'https://find-and-update.company-information.service.gov.uk/company/00000001');
  await panel.getByRole('button', { name: 'Linked cafe', exact: true }).click();
  await expect(panel.locator('.linked-company-venue')).toContainText('8 Trading Street');
  await page.getByRole('button', { name: 'Next companies', exact: true }).click();
  await expect(page.locator('.company-row')).toHaveCount(1);
  await expect(page.locator('.company-row')).toContainText('Company 26 Limited');
  await expect(page.getByRole('heading', { name: 'Company list', exact: true })).toBeFocused();
  await page.getByRole('link', { name: 'Data & methodology' }).click();
  await page.getByRole('link', { name: 'Companies', exact: true }).click();
  await expect(page.locator('.company-row')).toContainText('Company 26 Limited');
  await expect(panel).toContainText('00000001');
  await page.getByRole('button', { name: 'Previous companies', exact: true }).click();
  await expect(page.locator('.company-row')).toHaveCount(25);
  await expect(page.getByText('Registered offices in Brighton & Hove', { exact: true })).toBeVisible();
  expect(requests.filter(url => !url.pathname.endsWith('/meta')).every(url => url.searchParams.get('release_id') === release)).toBeTruthy();
  expect(requests.filter(url => url.pathname.endsWith('/companies')).every(url => !url.searchParams.has('categories'))).toBeTruthy();
  expect(requests.filter(url => url.pathname.endsWith('/companies')).every(url => url.searchParams.get('cohort') === 'registered_office')).toBeTruthy();
});

test('company failures, outdated searches and withheld offices stay usable on mobile', async ({ page }) => {
  await page.setViewportSize({ width: 375, height: 812 });
  const control = { busy: true, delay: false };
  await fixture(page, control, []);
  await expect(page.getByRole('heading', { name: 'Data is temporarily unavailable' })).toBeVisible();
  control.busy = false;
  await page.getByRole('button', { name: 'Try again', exact: true }).click();
  await page.locator('.company-row').nth(1).click();
  const panel = page.getByRole('region', { name: 'Selected company', exact: true });
  await expect(panel).toContainText('dissolved');
  await expect(panel).toContainText('03/06/2025');
  await expect(panel).toContainText('Address withheld');
  await expect(panel).not.toContainText('DO NOT DISPLAY');
  await expect(panel).not.toContainText('SECRET');
  await expect(panel.getByRole('link')).toHaveCount(0);
  await expect(panel).toContainText('No verified venue links');
  control.delay = true;
  await page.getByLabel('Company name', { exact: true }).fill('Company 1');
  await page.getByRole('button', { name: 'Search companies', exact: true }).click();
  await page.getByLabel('Company name', { exact: true }).fill('Company 26');
  await page.getByRole('button', { name: 'Search companies', exact: true }).click();
  await expect(page.locator('.company-row')).toHaveCount(1);
  await expect(page.locator('.company-row')).toContainText('Company 26 Limited');
  await page.waitForTimeout(800); // The deliberately older name-search result must not replace the newer one.
  await expect(page.locator('.company-row')).toHaveCount(1);
  await page.getByRole('button', { name: 'Clear company search', exact: true }).click();
  await expect(page.locator('.company-row')).toHaveCount(25);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBeTruthy();
});
