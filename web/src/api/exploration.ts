import type { ApiRoutes } from './contracts';
import type { CentreCandidate, Session } from '../state/session';

export const PAGE_SIZE = 25;
export const MAP_LIMIT = 500;
type Params = ApiRoutes['GET /api/v1/establishments']['params'] & { release_id: string };

// Candidate validation uses the exact eventual query, so accepting it reuses the
// same release/filter-bound response instead of issuing a second radius query.
export function explorationParams(state: Session, release: string, candidate?: CentreCandidate): Params {
  const search = state.search;
  const mode = candidate ? 'radius' : search.mode;
  const centre = candidate ?? search.centre;
  return { release_id: release, categories: state.categories, mode,
    ...(mode === 'area' ? { area_code: search.areaCode } : {}),
    ...(mode === 'radius' && centre ? { longitude: centre.longitude, latitude: centre.latitude, radius: search.radius } : {}),
    cursor: candidate ? null : state.pagination.explore, limit: PAGE_SIZE, map_limit: MAP_LIMIT };
}
