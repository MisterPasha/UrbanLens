import type { ActivityEvent, Envelope_Meta_, Meta } from '../api/contracts';

export type Section = 'explore' | 'compare' | 'activity' | 'companies' | 'data';
export type Category = Meta['categories'][number];
export type Radius = Meta['radii'][number];
export interface MapView { latitude: number; longitude: number; zoom: number }
export interface CentreCandidate {
  longitude: number; latitude: number; label: string;
  source: 'postcode' | 'map' | 'coordinates';
}
export const categoryColours: Record<Category, string> = {
  cafe: '#a45d16', restaurant: '#126f61', takeaway: '#475aa4', bakery: '#9a4373',
  pub_bar: '#665598', food_service: '#566a3a',
};
export const categories: { value: Category; label: string }[] = [
  { value: 'cafe', label: 'Cafés' }, { value: 'restaurant', label: 'Restaurants' },
  { value: 'takeaway', label: 'Takeaways' }, { value: 'bakery', label: 'Bakeries' },
  { value: 'pub_bar', label: 'Pubs & bars' }, { value: 'food_service', label: 'Other food service' },
];
export const sections: { value: Section; label: string }[] = [
  { value: 'explore', label: 'Explore' }, { value: 'compare', label: 'Compare' },
  { value: 'activity', label: 'Activity' }, { value: 'companies', label: 'Companies' },
  { value: 'data', label: 'Data & methodology' },
];

export function sectionFromHash(): Section {
  const value = window.location.hash.slice(1);
  return sections.find(section => section.value === value)?.value ?? 'explore';
}

export interface Session {
  section: Section;
  release: Envelope_Meta_ | null;
  bootError: string | null;
  refresh: number;
  expired: boolean;
  categories: Category[];
  search: {
    mode: 'city' | 'area' | 'radius';
    areaCode: string | null;
    centre: { longitude: number; latitude: number } | null;
    centreLabel: string | null;
    centreSource: CentreCandidate['source'] | null;
    radius: Radius;
  };
  comparison: [string | null, string | null];
  selectedVenue: string | null;
  mapView: MapView | null;
  radiusCandidate: CentreCandidate | null;
  centreIntent: string | null;
  pickingCentre: boolean;
  exploreHistory: (string | null)[];
  companyName: string;
  selectedCompany: string | null;
  companyHistory: (string | null)[];
  activityFilters: { entity: ActivityEvent['entity_type'] | ''; event: ActivityEvent['event_type'] | '' };
  activityHistory: (string | null)[];
  pagination: Record<'explore' | 'activity' | 'companies', string | null>;
}

export function initialSession(): Session {
  return {
    section: sectionFromHash(), release: null, bootError: null, refresh: 0, expired: false,
    categories: categories.map(category => category.value),
    search: { mode: 'city', areaCode: null, centre: null, centreLabel: null, centreSource: null, radius: 500 },
    comparison: [null, null], selectedVenue: null, mapView: null, exploreHistory: [],
    pagination: { explore: null, activity: null, companies: null },
    radiusCandidate: null, centreIntent: null, pickingCentre: false,
    companyName: '', selectedCompany: null, companyHistory: [],
    activityFilters: { entity: '', event: '' }, activityHistory: [],
  };
}

export type Action =
  | { type: 'navigate'; section: Section }
  | { type: 'pin'; release: Envelope_Meta_ }
  | { type: 'bootError'; message: string }
  | { type: 'refresh' }
  | { type: 'expired'; releaseId: string }
  | { type: 'category'; category: Category }
  | { type: 'categories'; values: Category[] }
  | { type: 'search'; value: Partial<Session['search']> }
  | { type: 'compare'; slot: 0 | 1; area: string | null }
  | { type: 'venue'; id: string | null }
  | { type: 'mapView'; value: MapView }
  | { type: 'lookupStart'; token: string }
  | { type: 'cancelCentre' }
  | { type: 'pickCentre'; active: boolean }
  | { type: 'proposeCentre'; value: CentreCandidate; token?: string }
  | { type: 'acceptCentre'; value: CentreCandidate }
  | { type: 'exploreNext'; cursor: string }
  | { type: 'exploreBack' }
  | { type: 'companyName'; value: string }
  | { type: 'company'; number: string | null }
  | { type: 'companyNext'; cursor: string }
  | { type: 'companyBack' }
  | { type: 'activityFilters'; value: Partial<Session['activityFilters']> }
  | { type: 'activityNext'; cursor: string }
  | { type: 'activityBack' }
  | { type: 'page'; section: keyof Session['pagination']; cursor: string | null };

export function sessionReducer(state: Session, action: Action): Session {
  switch (action.type) {
    case 'navigate': return { ...state, section: action.section,
      radiusCandidate: null, centreIntent: null, pickingCentre: false };
    case 'pin': return { ...state, release: action.release, bootError: null, expired: false,
      activityHistory: [],
      selectedCompany: null, companyHistory: [],
      pagination: { explore: null, companies: null, activity: null }, selectedVenue: null,
      mapView: null, exploreHistory: [], radiusCandidate: null, centreIntent: null, pickingCentre: false };
    case 'bootError': return { ...state, bootError: action.message };
    case 'refresh': return { ...state, release: null, bootError: null, expired: false,
      activityHistory: [],
      selectedCompany: null, companyHistory: [],
      refresh: state.refresh + 1, pagination: { explore: null, companies: null, activity: null },
      selectedVenue: null, mapView: null, exploreHistory: [], radiusCandidate: null, centreIntent: null, pickingCentre: false };
    case 'expired': return state.release?.release_id === action.releaseId
      ? { ...state, expired: true } : state;
    case 'category': return sessionReducer(state, { type: 'categories', values:
      state.categories.includes(action.category)
        ? state.categories.filter(value => value !== action.category)
        : [...state.categories, action.category].sort() });
    case 'categories': return { ...state, categories: [...action.values].sort(), selectedVenue: null, exploreHistory: [],
      pagination: { ...state.pagination, explore: null } };
    case 'search': return { ...state, search: { ...state.search, ...action.value }, selectedVenue: null,
      mapView: null, exploreHistory: [],
      radiusCandidate: null, centreIntent: null, pickingCentre: false,
      pagination: { ...state.pagination, explore: null } };
    case 'compare': {
      if (action.area && state.comparison[action.slot === 0 ? 1 : 0] === action.area) return state;
      const comparison: Session['comparison'] = [...state.comparison];
      comparison[action.slot] = action.area;
      return { ...state, comparison };
    }
    case 'venue': return { ...state, selectedVenue: action.id };
    case 'mapView': return { ...state, mapView: action.value };
    case 'lookupStart': return { ...state, centreIntent: action.token, radiusCandidate: null, pickingCentre: false };
    case 'cancelCentre': return { ...state, centreIntent: null, radiusCandidate: null, pickingCentre: false };
    case 'pickCentre': return { ...state, centreIntent: null, radiusCandidate: null, pickingCentre: action.active };
    case 'proposeCentre': return action.token && action.token !== state.centreIntent ? state : {
      ...state, radiusCandidate: action.value, centreIntent: null, pickingCentre: false };
    case 'acceptCentre': return state.radiusCandidate !== action.value ? state : {
      ...state, search: { ...state.search, mode: 'radius',
        centre: { longitude: action.value.longitude, latitude: action.value.latitude },
        centreLabel: action.value.label, centreSource: action.value.source },
      radiusCandidate: null, centreIntent: null, pickingCentre: false, selectedVenue: null,
      mapView: null, exploreHistory: [], pagination: { ...state.pagination, explore: null } };
    case 'exploreNext': return { ...state,
      exploreHistory: [...state.exploreHistory, state.pagination.explore],
      pagination: { ...state.pagination, explore: action.cursor } };
    case 'exploreBack': return state.exploreHistory.length ? { ...state,
      pagination: { ...state.pagination, explore: state.exploreHistory.at(-1) ?? null },
      exploreHistory: state.exploreHistory.slice(0, -1) } : state;
    case 'company': return { ...state, selectedCompany: action.number };
    case 'activityFilters': return { ...state, activityFilters: { ...state.activityFilters, ...action.value },
      activityHistory: [], pagination: { ...state.pagination, activity: null } };
    case 'activityNext': return { ...state, activityHistory: [...state.activityHistory, state.pagination.activity],
      pagination: { ...state.pagination, activity: action.cursor } };
    case 'activityBack': return state.activityHistory.length ? { ...state,
      pagination: { ...state.pagination, activity: state.activityHistory.at(-1) ?? null },
      activityHistory: state.activityHistory.slice(0, -1) } : state;
    case 'companyName': return { ...state, companyName: action.value, selectedCompany: null, companyHistory: [],
      pagination: { ...state.pagination, companies: null } };
    case 'companyNext': return { ...state, companyHistory: [...state.companyHistory, state.pagination.companies],
      pagination: { ...state.pagination, companies: action.cursor } };
    case 'companyBack': return state.companyHistory.length ? { ...state,
      pagination: { ...state.pagination, companies: state.companyHistory.at(-1) ?? null },
      companyHistory: state.companyHistory.slice(0, -1) } : state;
    case 'page': return { ...state,
      pagination: { ...state.pagination, [action.section]: action.cursor } };
  }
}
