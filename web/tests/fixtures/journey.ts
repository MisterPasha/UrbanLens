import type { Page } from '@playwright/test';
import type { AreaMetrics, CompanyDetail, VenueDetail } from '../../src/api/contracts';

export const firstRelease = '11111111-1111-4111-8111-111111111111';
export const secondRelease = '22222222-2222-4222-8222-222222222222';
export const areaCodes = ['E01000001', 'E01000002'];
export const allCategories = ['cafe', 'restaurant', 'takeaway', 'bakery', 'pub_bar', 'food_service'];
export interface JourneyControl {
  release: string;
  mode: 'ready' | 'slow' | 'outage' | 'throttle' | 'limit' | 'timeout' | 'expired';
}
export const venue: VenueDetail = { establishment_id: '00000000-0000-4000-8000-000000000001',
  display_name: 'Journey Cafe', name_unavailable: false, address: '42 Journey Street', postcode: 'BN1 1AA',
  primary_category: 'cafe', secondary_categories: [], eligibility: 'eligible', duplicate_status: 'resolved', observation_state: 'observed',
  primary_spatial_eligible: true, location_quality: 'source_point', longitude: -.14, latitude: 50.82, area_code: areaCodes[0],
  first_observed_at: '2026-11-01T12:00:00Z', last_observed_at: '2026-11-04T12:00:00Z', source_mix: 'fsa+osm',
  exclusion_reasons: [], hygiene: [], null_reasons: {}, provenance: {}, verified_companies: [], sources: [{
    provider: 'osm', source_key: 'node/1234', state: 'observed', observed_at: '2026-11-04T12:00:00Z', url: 'https://www.openstreetmap.org/node/1234' }],
};
const company: CompanyDetail = { company_number: '00000001', legal_name: 'Journey Hospitality Limited', legal_status: 'active',
  company_type: 'ltd', sic_codes: ['56101'], incorporation_date: '2020-01-01', cessation_date: null,
  registered_office: { address_state: 'published', address: '1 Contact Street', postcode: 'BN1 1AA' }, registered_office_postcode: 'BN1 1AA',
  cohorts: ['registered_office'], office_caveat: 'Registered office is not trading premises', first_observed_at: venue.first_observed_at,
  last_observed_at: venue.last_observed_at, profile_observed_at: null, url: 'https://find-and-update.company-information.service.gov.uk/company/00000001', verified_premises: [],
};
const metrics = (code: string | null, n: number): AreaMetrics => ({ area_code: code,
  area_name: code ? `Journey area ${code === areaCodes[0] ? 'one' : 'two'}` : 'Brighton & Hove', geography_version: 'LSOA-2021',
  population_edition: '2024', population_reference_date: '2024-06-30', land_area_basis: 'Official land', land_area_edition: { edition: '2021' },
  establishment_count: n, category_mix: n ? { cafe: n } : {}, official_land_km2: code === areaCodes[1] ? null : 1, population: 1000,
  establishments_per_land_km2: code === areaCodes[1] ? null : n, land_rate_unavailable_reason: code === areaCodes[1] ? 'official_land_missing' : null,
  establishments_per_1000_residents: n, resident_rate_unavailable_reason: null,
  population_per_land_km2: code === areaCodes[1] ? null : 1000, population_density_unavailable_reason: code === areaCodes[1] ? 'official_land_missing' : null,
  age_0_15: 200, age_16_64: 600, age_65_plus: 200, age_0_15_percent: 20, age_16_64_percent: 60, age_65_plus_percent: 20, age_mix_unavailable_reason: null });

export async function journeyApi(page: Page, control: JourneyControl, requests: URL[]) {
  await page.route('https://tile.openstreetmap.org/**', route => route.abort());
  await page.route('**/api/v1/**', async route => {
    const url = new URL(route.request().url()); requests.push(url);
    const path = url.pathname.split('/api/v1/')[1];
    if (path === 'establishments' && control.mode !== 'ready') {
      if (control.mode === 'slow') await new Promise(resolve => setTimeout(resolve, 600));
      else if (control.mode === 'timeout') return;
      else {
        const [status, code] = control.mode === 'expired' ? [410, 'release_expired'] : control.mode === 'throttle' ? [429, 'rate_limited']
          : control.mode === 'limit' ? [503, 'cost_limit'] : [503, 'service_unavailable'];
        await route.fulfill({ status: Number(status), json: { error: { code, message: 'Temporarily unavailable', request_id: 'fixture' } } }); return;
      }
    }
    const categories = url.searchParams.getAll('categories');
    const n = url.searchParams.get('empty_categories') === 'true' || (categories.length > 0 && !categories.includes('cafe')) ? 0 : 1;
    let data: unknown;
    if (path === 'meta') data = { city: 'Brighton & Hove', industry: 'Hospitality', categories: allCategories, radii: [250, 500, 1000, 2000], default_radius: 500 };
    else if (path === 'areas') data = areaCodes.map((code, i) => ({ area_code: code, area_name: metrics(code, 1).area_name, geography_version: 'LSOA-2021',
      display_geometry: { type: 'Polygon', coordinates: [[[-.15 + i * .02, 50.81], [-.13 + i * .02, 50.81], [-.13 + i * .02, 50.83], [-.15 + i * .02, 50.83], [-.15 + i * .02, 50.81]]] } }));
    else if (path === 'establishments') data = { total: n, items: n ? [venue] : [], map_points: n ? [venue] : [], has_more: false, map_truncated: false,
      category_mix: n ? { cafe: 1 } : {}, partial_coverage: url.searchParams.get('mode') === 'radius', unknown_location_scope: 'city-wide' };
    else if (path.startsWith('establishments/')) data = venue;
    else if (path.startsWith('postcodes/')) data = { postcode: 'BN11AA', state: 'resolved', longitude: -.14, latitude: 50.82, can_define_search: true, approximate: true };
    else if (path.startsWith('areas/')) data = { area: metrics(path.split('/')[1], n), city_benchmark: metrics(null, n) };
    else if (path === 'compare') data = { areas: url.searchParams.getAll('area_codes').map(code => metrics(code, n)), city_benchmark: metrics(null, n) };
    else if (path === 'companies') data = { total: 1, items: [company], has_more: false, interpretation: 'Legal entities' };
    else if (path.startsWith('companies/')) data = company;
    else if (path === 'activity') data = { total: 0, items: [], has_more: false, interpretation: 'Not verified openings/closures', context_note: 'Test fixture',
      baselines: ['establishment', 'company'].map(entity_type => ({ entity_type, established_at: '2026-11-01T12:00:00Z', retained_event_count: 0,
        state: 'no_observed_changes', retention_boundary: '2025-11-08T12:00:00Z', baseline_source_scopes: 2 })) };
    else throw new Error(path);
    await route.fulfill({ json: { release_id: control.release, applied_filters: {}, null_reasons: {},
      metadata: { published_at: '2026-11-08T12:00:00Z', geography_version: 'LSOA-2021', population_edition: '2024', population_reference_date: '2024-06-30',
        taxonomy_version: 'fixture', attribution: ['OpenStreetMap contributors: ODbL', 'ONS: Open Government Licence'], sources: [
          { provider: 'fsa', status: 'stale', retained_older_snapshot: true, last_success_at: '2026-10-01T12:00:00Z', source_extract_date: '2026-09-30', next_due_at: '2026-11-01T08:00:00Z' },
          { provider: 'osm', status: 'healthy', last_success_at: '2026-11-04T12:00:00Z', source_edition: 'Test edition' },
        ] }, coverage: [
          { universe: 'current canonical roster', dimension: 'coordinate_quality', bucket: 'source_point', n: 1, denominator: 2, percent: 50, reasons_overlap: false },
          { universe: 'current canonical roster', dimension: 'coordinate_quality', bucket: 'postcode_fallback', n: 1, denominator: 2, percent: 50, reasons_overlap: false },
          { universe: 'current canonical roster', dimension: 'duplicates', bucket: 'unresolved', n: 1, denominator: 2, percent: 50 },
          { universe: 'current canonical roster', dimension: 'accepted_source_mix', bucket: 'fsa+osm', n: 2, denominator: 2, percent: 100 },
          { universe: 'current canonical roster', dimension: 'category_detail', bucket: 'broad', n: 1 },
          { universe: 'primary spatial venues', dimension: 'category_detail', bucket: 'specific', n: 1, denominator: 1, percent: 100 },
          { universe: 'non-excluded venue records', dimension: 'duplicates', bucket: 'unresolved', n: 1, denominator: 2, percent: 50 },
          { universe: 'eligible resolved supported venues', dimension: 'coordinate_quality', bucket: 'source_point', n: 1, denominator: 2, percent: 50 },
          { universe: 'eligible resolved supported venues', dimension: 'category_detail', bucket: 'broad', n: 1 },
          { universe: 'eligible resolved supported venues', dimension: 'verified_company_link', bucket: 'unlinked', n: 1, denominator: 1, percent: 100 },
        ], data } }).catch(() => {});
  });
}
