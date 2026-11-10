import { useCallback, useEffect, useMemo, useRef } from 'react';
import type { Dispatch } from 'react';
import type { Envelope_Meta_, MapPoint, Venue } from '../api/contracts';
import { ApiError } from '../api/client';
import { explorationParams, PAGE_SIZE } from '../api/exploration';
import type { QueryClient } from '../api/query';
import { useApiQuery } from '../api/query';
import { categories, categoryColours } from '../state/session';
import type { Action, MapView, Session } from '../state/session';
import { Empty, QueryContent, Unavailable } from './Feedback';
import { VenueMap } from './VenueMap';
import { CategoryMix } from './CategoryMix';
import { VenueEvidence } from './VenueEvidence';
import type { SearchCircle } from './VenueMap';

const noPoints: MapPoint[] = [];
const noItems: Venue[] = [];
const categoryLabel = (value: string | null) => categories.find(category => category.value === value)?.label ?? 'Category unavailable';
export function Explore({ state, client, release, dispatch }: {
  state: Session; client: QueryClient; release: Envelope_Meta_; dispatch: Dispatch<Action>;
}) {
  const { search } = state;
  const ready = search.mode === 'city' || (search.mode === 'area' && search.areaCode !== null)
    || (search.mode === 'radius' && search.centre !== null);
  const query = useApiQuery(client, 'GET /api/v1/establishments', ready ? explorationParams(state, release.release_id) : null);
  const selected = useApiQuery(client, 'GET /api/v1/establishments/{establishment_id}', state.selectedVenue ? {
    release_id: release.release_id, establishment_id: state.selectedVenue,
  } : null);
  const onSelect = useCallback((id: string) => dispatch({ type: 'venue', id }), [dispatch]);
  const onViewChange = useCallback((value: MapView) => dispatch({ type: 'mapView', value }), [dispatch]);
  const onPickCentre = useCallback((point: { longitude: number; latitude: number }) =>
    dispatch({ type: 'proposeCentre', value: { ...point, label: 'Map point', source: 'map' } }), [dispatch]);
  const onPickingChange = useCallback((active: boolean) => dispatch({ type: 'pickCentre', active }), [dispatch]);
  const circle = useMemo<SearchCircle | null>(() => search.mode === 'radius' && search.centre ? {
    ...search.centre, radius: search.radius, label: search.centreLabel ?? 'Chosen point',
  } : null, [search.mode, search.centre, search.radius, search.centreLabel]);
  const listHeading = useRef<HTMLHeadingElement>(null);
  const pageFocus = useRef<string | null | undefined>(undefined);
  useEffect(() => {
    if (query.status !== 'success' || pageFocus.current === undefined) return;
    if (pageFocus.current === state.pagination.explore) listHeading.current?.focus({ preventScroll: true });
    pageFocus.current = undefined;
  }, [query.status, state.pagination.explore]);
  const selectedPoint = useMemo<MapPoint | null>(() => {
    const item = query.data?.data.items.find(item => item.establishment_id === state.selectedVenue);
    if (item) return item;
    const detail = selected.data?.data;
    return detail?.primary_spatial_eligible && detail.primary_category && detail.longitude !== null && detail.latitude !== null
      ? { establishment_id: detail.establishment_id, primary_category: detail.primary_category,
        longitude: detail.longitude, latitude: detail.latitude } : null;
  }, [query.data, selected.data, state.selectedVenue]);

  const mapOptions = { circle, allowCentreSelection: search.mode === 'radius',
    pickingCentre: state.pickingCentre, onPickCentre, onPickingChange, initialView: state.mapView,
    onSelect, onViewChange };
  const circleSummary = circle && <section className="radius-summary" aria-label="Applied radius search">
    <h3>Search circle</h3><dl><div><dt>Centre</dt><dd>{circle.label}</dd></div>
      <div><dt>Longitude</dt><dd>{circle.longitude.toFixed(6)}</dd></div>
      <div><dt>Latitude</dt><dd>{circle.latitude.toFixed(6)}</dd></div>
      <div><dt>Radius</dt><dd>{circle.radius < 1000 ? `${circle.radius} m` : `${circle.radius / 1000} km`}</dd></div></dl>
    <p>{search.centreSource === 'postcode' ? 'This is an approximate postcode centre. ' : ''}
      Venue counts use precise published venue locations. Population and age estimates are not calculated for this circle.</p>
  </section>;
  if (!ready) return <><Empty title={search.mode === 'area' ? 'Choose a neighbourhood' : 'Choose a search centre'}>
    {search.mode === 'radius' ? 'Use a full postcode or explicitly choose a point on the map.' : 'Set your search scope using the research filters.'}</Empty>
    {search.mode === 'radius' && <VenueMap {...mapOptions} points={noPoints} items={noItems}
      selectedId={null} selectedPoint={null} awaitingCentre />}</>;
  if (search.mode === 'radius' && query.status === 'error') return <>{circleSummary}
    {query.error instanceof ApiError && query.error.status === 422
      ? <div className="feedback unavailable" role="alert"><h3>Search centre outside the supported area</h3>
        <p>The chosen point has not moved. Choose a supported point in Brighton &amp; Hove to load radius results.</p></div>
      : <Unavailable error={query.error} retry={query.retry} />}
    <VenueMap {...mapOptions} points={noPoints} items={noItems} selectedId={null} selectedPoint={null} /></>;
  return <>{circleSummary}<QueryContent query={query}>{({ data }) => <>
    <section className="card competition-summary" aria-labelledby="mix-heading">
      <div className="card-heading"><h3 id="mix-heading">Your competition at a glance</h3>
        <span className="tag">{search.mode === 'city' ? 'Whole city' : search.mode === 'area' ? 'Selected neighbourhood' : `${search.radius} m radius`}</span></div>
      <p className="competition-total" role="status" aria-live="polite" aria-atomic="true"><span>Matching venues</span>
        <strong>{data.total.toLocaleString('en-GB')}</strong></p>
      <CategoryMix mix={data.category_mix} total={data.total} selected={state.categories} showCaption={false} />
    </section>
    {data.partial_coverage && <p className="coverage-note" role="status">Part of this circle is outside Brighton &amp; Hove. Counts cover the supported city area only.</p>}
    {(data.total > 0 || search.mode === 'radius') && <VenueMap {...mapOptions} points={data.map_points} items={data.items}
      selectedId={state.selectedVenue} selectedPoint={selectedPoint} />}
    {data.total === 0 ? <Empty title="No matching venues">{state.categories.length
      ? 'Try another area or a wider set of categories.' : 'Select at least one venue category to explore.'}</Empty> : <>
      <p className="marker-summary" role="status">{data.map_points.length.toLocaleString('en-GB')} overview points shown
        {data.map_truncated ? ` of ${data.total.toLocaleString('en-GB')} matching venues. The marker limit does not change the total; all matches are accessible through the list.` : '. All matching overview points are shown.'}
        {selectedPoint && !data.map_points.some(point => point.establishment_id === selectedPoint.establishment_id)
          && ' The selected venue is shown as an additional highlighted point.'}</p>
      <ul className="map-legend" aria-label="Map category legend">{categories.filter(category => state.categories.includes(category.value)).map(category =>
        <li key={category.value}><span style={{ backgroundColor: categoryColours[category.value] }} aria-hidden="true" />{category.label}</li>)}</ul>
      {state.selectedVenue && <section className="selected-venue card" aria-labelledby="selected-venue-heading">
        <div className="card-heading"><h3 id="selected-venue-heading">Selected venue</h3>
          <button className="text-button" onClick={() => dispatch({ type: 'venue', id: null })}>Clear selection</button></div>
        <QueryContent query={selected}>{({ data: venue, metadata }) => <VenueEvidence venue={venue} metadata={metadata}
          offPage={!data.items.some(item => item.establishment_id === venue.establishment_id)} />}</QueryContent>
      </section>}
      <section className="venue-list card" id="venue-list" aria-labelledby="venue-list-heading">
        <div className="card-heading"><h3 id="venue-list-heading" ref={listHeading} tabIndex={-1}>Venue list</h3>
          <span className="list-range">Showing {data.items.length ? state.exploreHistory.length * PAGE_SIZE + 1 : 0}–{state.exploreHistory.length * PAGE_SIZE + data.items.length} of {data.total.toLocaleString('en-GB')}</span></div>
        <ol start={state.exploreHistory.length * PAGE_SIZE + 1}>{data.items.map(venue => <li key={venue.establishment_id}>
          <button className={`venue-row${state.selectedVenue === venue.establishment_id ? ' is-selected' : ''}`}
            aria-pressed={state.selectedVenue === venue.establishment_id}
            onClick={() => onSelect(venue.establishment_id)}>
            <span className="venue-row-heading"><strong>{venue.display_name ?? 'Name unavailable'}</strong>
              <span className="tag">{categoryLabel(venue.primary_category)}</span></span>
            <span>{venue.address ?? 'Address unavailable'}{venue.postcode ? ` · ${venue.postcode}` : ''}</span>
            <small>{venue.area_code} · Observed {new Date(venue.last_observed_at).toLocaleDateString('en-GB')}
              {state.selectedVenue === venue.establishment_id && ' · Selected'}</small>
          </button>
        </li>)}</ol>
        <nav className="list-pagination" aria-label="Venue list pages">
          <button className="button secondary" disabled={!state.exploreHistory.length} onClick={() => {
            pageFocus.current = state.exploreHistory.at(-1) ?? null; dispatch({ type: 'exploreBack' });
          }}>Previous venues</button>
          <span>Page {state.exploreHistory.length + 1}</span>
          <button className="button secondary" disabled={!data.has_more || !data.next_cursor} onClick={() => {
            if (data.next_cursor) { pageFocus.current = data.next_cursor; dispatch({ type: 'exploreNext', cursor: data.next_cursor }); }
          }}>Next venues</button>
        </nav>
      </section>
    </>}
  </>}</QueryContent></>;
}
