import { expect, test } from '@playwright/test';
import type { Page } from '@playwright/test';
import type { ActivityEvent } from '../src/api/contracts';

const release = '11111111-1111-4111-8111-111111111111';
const kinds: ActivityEvent['event_type'][] = ['no_longer_observed', 'newly_observed', 'reappeared', 'source_changed',
  'company_incorporated', 'company_dissolved', 'processing_correction'];
const events: ActivityEvent[] = Array.from({ length: 27 }, (_, i) => ({
  event_id: `00000000-0000-4000-8000-${String(i + 1).padStart(12, '0')}`,
  entity_type: i === 1 ? 'source' : i === 4 || i === 5 ? 'company' : 'establishment',
  establishment_id: null, company_number: i === 4 || i === 5 ? '00000001' : null,
  event_type: kinds[i] ?? 'source_changed', observed_at: '2026-11-08T12:00:00Z',
  source_event_date: i === 4 ? '2020-02-03' : null,
  before_value: { value: { display_name: `Old name ${i + 1}`, observation_state: 'observed', address: 'PRIVATE HISTORICAL ADDRESS',
    location: { latitude: 1 } } },
  after_value: { value: { display_name: `Event ${i + 1}`, observation_state: i === 0 ? 'no_longer_observed' : 'observed',
    postcode: 'SECRET POSTCODE' } },
  evidence: { provider: i === 4 || i === 5 ? 'companies-house' : 'fsa', manifest_uri: 's3://private/history' },
  rule_version: 'fixture', baseline_established_at: '2026-11-01T12:00:00Z', interpretation: 'Observed changes, not confirmed openings/closures',
}));
type Control = { mode: 'events' | 'baseline' | 'missing'; busy: boolean; delay: boolean };
async function fixture(page: Page, control: Control, requests: URL[]) {
  await page.route('https://tile.openstreetmap.org/**', route => route.abort());
  await page.route('**/api/v1/**', async route => {
    const url = new URL(route.request().url()); requests.push(url);
    const path = url.pathname.split('/api/v1/')[1]; let data: unknown;
    if (path === 'meta') data = { city: 'Brighton & Hove', industry: 'Hospitality', categories: ['cafe'], radii: [250, 500, 1000, 2000], default_radius: 500 };
    else if (path === 'areas') data = [];
    else if (path === 'activity') {
      if (control.busy) { await route.fulfill({ status: 503, json: { error: { code: 'unavailable', message: 'Unavailable' } } }); return; }
      const entity = url.searchParams.get('entity_type'), event = url.searchParams.get('event_type');
      const selected = control.mode === 'events' ? events.filter(item => (!entity || item.entity_type === entity) && (!event || item.event_type === event)) : [];
      const offset = Number(url.searchParams.get('cursor') ?? 0);
      data = { total: selected.length, items: selected.slice(offset, offset + 25), has_more: selected.length > offset + 25,
        next_cursor: selected.length > offset + 25 ? String(offset + 25) : null,
        baselines: ['establishment', 'company'].map(entity_type => ({ entity_type,
          established_at: control.mode === 'missing' ? null : '2026-11-01T12:00:00Z',
          retained_event_count: control.mode === 'missing' ? null : control.mode === 'events' ? 27 : 0,
          state: control.mode === 'missing' ? 'baseline_unavailable' : control.mode === 'baseline' ? 'no_observed_changes' : 'observed_changes',
          retention_boundary: '2025-11-08T12:00:00Z', baseline_source_scopes: control.mode === 'missing' ? 0 : 2 })),
        interpretation: 'Not confirmed openings/closures', context_note: 'Test fixture; no live events created' };
      if (entity === 'source' && control.delay) await new Promise(resolve => setTimeout(resolve, 700));
    } else throw new Error(path);
    await route.fulfill({ json: { release_id: release, applied_filters: {}, coverage: [], null_reasons: {},
      metadata: { published_at: '2026-11-08T12:00:00Z', geography_version: 'LSOA-2021', population_edition: 'fixture',
        population_reference_date: '2024-06-30', taxonomy_version: 'fixture', sources: [], attribution: [] }, data } }).catch(() => {});
  });
  await page.goto('/#activity');
}

test('observed events distinguish venue, provider, legal and processing changes with public differences and pinned paging', async ({ page }) => {
  const requests: URL[] = [];
  await fixture(page, { mode: 'events', busy: false, delay: false }, requests);
  await expect(page.locator('.activity-event')).toHaveCount(25);
  const missing = page.getByRole('article', { name: 'No longer observed — Venue record', exact: true });
  await expect(missing).toContainText('not a verified business closure');
  await expect(missing.getByRole('table')).toContainText('Old name 1');
  await expect(missing.getByRole('table')).toContainText('Event 1');
  await expect(page.getByRole('article', { name: 'Newly observed — Provider record', exact: true })).toContainText('not a verified opening');
  await expect(page.getByRole('article', { name: 'Reappeared in source data — Venue record', exact: true })).toContainText('does not prove the business reopened');
  await expect(page.getByRole('article', { name: 'Company incorporated — Legal company', exact: true })).toContainText('03/02/2020');
  await expect(page.getByRole('article', { name: 'Company dissolved — Legal company', exact: true })).toContainText('Company 00000001');
  await expect(page.getByRole('article', { name: 'Processing correction — Venue record', exact: true })).toContainText('data-processing or review correction');
  await expect(page.locator('main')).not.toContainText('PRIVATE HISTORICAL ADDRESS');
  await expect(page.locator('main')).not.toContainText('SECRET POSTCODE');
  await expect(page.locator('main')).not.toContainText('s3://');
  await page.getByRole('button', { name: 'Next events', exact: true }).click();
  await expect(page.locator('.activity-event')).toHaveCount(2);
  await expect(page.getByRole('heading', { name: 'Recorded events', exact: true })).toBeFocused();
  await page.getByRole('link', { name: 'Data & methodology' }).click();
  await page.getByRole('link', { name: 'Activity', exact: true }).click();
  await expect(page.locator('.activity-event')).toHaveCount(2);
  await page.getByRole('button', { name: 'Previous events', exact: true }).click();
  await expect(page.locator('.activity-event')).toHaveCount(25);
  await page.getByRole('combobox', { name: 'Change type', exact: true }).selectOption('processing_correction');
  await expect(page.locator('.activity-event')).toHaveCount(1);
  const last = requests.filter(url => url.pathname.endsWith('/activity')).at(-1)!;
  expect(last.searchParams.get('event_type')).toBe('processing_correction');
  expect(last.searchParams.get('cursor')).toBeNull();
  expect(requests.filter(url => !url.pathname.endsWith('/meta')).every(url => url.searchParams.get('release_id') === release)).toBeTruthy();
});

test('baseline-only, insufficient-history and filtered emptiness remain distinct; failures and late results recover on mobile', async ({ page }) => {
  await page.setViewportSize({ width: 375, height: 812 });
  const control: Control = { mode: 'baseline', busy: true, delay: false };
  await fixture(page, control, []);
  await expect(page.getByRole('heading', { name: 'Data is temporarily unavailable' })).toBeVisible();
  control.busy = false;
  await page.getByRole('button', { name: 'Try again', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'No observed changes recorded', exact: true })).toBeVisible();
  await expect(page.getByText(/baseline-only history cannot establish a trend/)).toBeVisible();
  control.mode = 'missing';
  await page.reload();
  await expect(page.getByRole('heading', { name: 'History is not ready for a full comparison' })).toBeVisible();
  await expect(page.getByText(/Some starting baselines are missing/)).toBeVisible();
  control.mode = 'events'; control.delay = true;
  await page.getByRole('combobox', { name: 'Record type', exact: true }).selectOption('source');
  await page.getByRole('combobox', { name: 'Record type', exact: true }).selectOption('company');
  await expect(page.locator('.activity-event')).toHaveCount(2);
  await page.waitForTimeout(800); // The deliberately older provider response cannot replace legal-company events.
  await expect(page.locator('.activity-event')).toHaveCount(2);
  await page.getByRole('combobox', { name: 'Change type', exact: true }).selectOption('reappeared');
  await expect(page.getByRole('heading', { name: 'No matching events' })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBeTruthy();
});
